// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.service;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateStatistics;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.Types;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventPropertyKey;
import org.apache.impala.catalog.iceberg.IcebergPuffinStatsWriter;
import org.apache.impala.catalog.iceberg.IcebergStatsMerger;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.thrift.TAlterTableUpdateStatsParams;
import org.apache.impala.thrift.TColumnStats;
import org.apache.impala.thrift.TIcebergPuffinStatsParams;
import org.apache.impala.thrift.TIcebergStatsMode;
import org.apache.impala.util.EventSequence;
import org.apache.impala.util.IcebergSchemaConverter;
import org.apache.impala.util.IcebergUtil;
import org.apache.impala.util.IcebergUtil.ComputeStatsSnapshotPropertyConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * The only writer of Impala statistics of Iceberg tables in catalogd: the Puffin
 * file registered for the snapshot the statistics describe, the snapshot ids property
 * and the HMS column statistics. Every write goes "property, then HMS" so that HMS
 * metrics never describe a snapshot other than the one recorded for their column.
 * Runs on the DDL thread under the write lock of the table; takes no locks itself.
 */
public class IcebergStatsUpdater {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergStatsUpdater.class);

  private static final String IDS = IcebergTable.COMPUTE_STATS_SNAPSHOT_IDS;

  private static final IcebergPuffinStatsWriter PUFFIN_WRITER =
      new IcebergPuffinStatsWriter();

  /** Loads a fresh Iceberg table instance; the catalog cache must stay untouched. */
  @FunctionalInterface
  public interface FreshTableLoader {
    org.apache.iceberg.Table load(IcebergTable t) throws ImpalaException;
  }

  /** HMS side of the statistics write. */
  public interface HmsStatsWriter {
    void updateColumnStats(ColumnStatistics stats) throws ImpalaException;

    /**
     * Writes the parameters of 'msTbl'. With HiveCatalog the metadata locations are
     * taken from HMS first, so the write cannot roll back an Iceberg commit.
     */
    void alterTableProperties(org.apache.hadoop.hive.metastore.api.Table msTbl)
        throws ImpalaException;

    /** Parameters of the table 'msTbl' as stored in HMS now. */
    Map<String, String> readTableParameters(
        org.apache.hadoop.hive.metastore.api.Table msTbl) throws ImpalaException;

    /** Columns without statistics have no key in the result. */
    Map<String, ColumnStatisticsObj> readColumnStats(
        org.apache.hadoop.hive.metastore.api.Table msTbl, List<String> colNames)
        throws ImpalaException;
  }

  enum Reconcile { WRITE, ALREADY_APPLIED, CONFLICT }

  static final class ColumnResult {
    final String colName;
    final TColumnStats stats;
    /** Null for columns without a theta sketch. */
    @Nullable final byte[] sketch;
    /** NDV stored in the blob; meaningful only for columns with a sketch. */
    final long ndv;
    /** Null if HMS keeps no statistics for the column type. */
    @Nullable final ColumnStatisticsObj hmsStats;

    ColumnResult(String colName, TColumnStats stats, @Nullable byte[] sketch, long ndv,
        @Nullable ColumnStatisticsObj hmsStats) {
      this.colName = colName;
      this.stats = stats;
      this.sketch = sketch;
      this.ndv = ndv;
      this.hmsStats = hmsStats;
    }
  }

  /** A commit of Iceberg metadata that may or may not have been applied. */
  private static final class UnknownOutcomeException extends Exception {
    final RuntimeException failure;

    UnknownOutcomeException(RuntimeException failure) {
      super(failure.getMessage(), failure);
      this.failure = failure;
    }
  }

  private final FreshTableLoader loader_;

  public IcebergStatsUpdater() { this(IcebergUtil::loadTable); }

  public IcebergStatsUpdater(FreshTableLoader loader) {
    loader_ = Preconditions.checkNotNull(loader);
  }

  /**
   * Writes the statistics computed by COMPUTE STATS for the snapshot of the analysis.
   * 'msTbl' is a copy of the HMS table that already carries the table-level
   * properties set by the caller; the snapshot ids property is updated in it, starting
   * from the stored one.
   * Returns the number of columns written to HMS.
   */
  public long apply(IcebergTable table, org.apache.hadoop.hive.metastore.api.Table msTbl,
      TAlterTableUpdateStatsParams params, HmsStatsWriter hms, EventSequence timeline)
      throws ImpalaException {
    Preconditions.checkState(table.isWriteLockedByCurrentThread());
    checkInput(table, params);
    TIcebergPuffinStatsParams p = params.getIceberg_puffin_stats();
    long s1 = p.getCalc_snapshot_id();

    org.apache.iceberg.Table fresh = load(table);
    checkAgainstAnalysis(table, fresh, p);
    takeStoredIds(fresh, msTbl, integrated(msTbl), hms);
    Set<Integer> fieldIds = new TreeSet<>(p.getColumn_field_ids().values());
    switch (reconcile(table, fresh, msTbl, p, fieldIds, hms)) {
      case ALREADY_APPLIED: return fieldIds.size();
      case CONFLICT:
        throw new ImpalaRuntimeException(String.format("Statistics of table %s were "
            + "changed concurrently; run COMPUTE INCREMENTAL STATS again.",
            table.getFullName()));
      default: break;
    }
    Map<Integer, ColumnResult> results = finalStats(table, fresh, params, hms);

    boolean integrated = integrated(msTbl);
    TreeMap<Integer, Long> newIds = null;
    if (!fieldIds.isEmpty()) newIds = newIds(msTbl, fieldIds, s1, p.isSampled());
    if (newIds != null && newIds.isEmpty() && integrated) {
      try {
        commit(() -> setIdsKnownToHms(fresh, msTbl));
      } catch (UnknownOutcomeException e) {
        LOG.error("Commit state of statistics of table {} snapshot {} is unknown: run "
            + "COMPUTE STATS", table.getFullName(), s1, e.failure);
        throw unknownState(table, "COMPUTE STATS", e.failure);
      } catch (RuntimeException e) {
        throw registerFailed(table, fresh, s1, null, e);
      }
    }

    StatisticsFile file = null;
    if (!fieldIds.isEmpty()) file = writePuffinFile(table, fresh, s1, results, timeline);

    Transaction txn;
    Map<String, String> staged;
    try {
      RegistrationGuard guard = new RegistrationGuard(ops(fresh), table.getFullName(),
          Collections.singletonMap(s1, registeredPath(fresh, s1)),
          s1 == -1 ? Collections.emptySet() : Collections.singleton(s1));
      txn = Transactions.newTransaction(fresh.name(), guard);
      if (file != null) txn.updateStatistics().setStatistics(file).commit();
      staged = stageProperties(txn, msTbl, newIds, integrated);
    } catch (RuntimeException e) {
      throw registerFailed(table, fresh, s1, file, e);
    }
    if (file != null || !staged.isEmpty()) {
      try {
        commit(txn::commitTransaction);
      } catch (UnknownOutcomeException e) {
        if (!committed(table, fresh, msTbl, newIds, fieldIds, s1, file, staged, hms, e)) {
          throw registerFailed(table, fresh, s1, file, e.failure);
        }
      } catch (RuntimeException e) {
        throw registerFailed(table, fresh, s1, file, e);
      }
      timeline.markEvent("Committed Iceberg statistics transaction");
    }
    try {
      if (!integrated) {
        hms.alterTableProperties(msTbl);
        timeline.markEvent("Updated table properties");
      }
      if (fieldIds.isEmpty()) return 0;
      ColumnStatistics colStats = hmsColumnStats(table, results);
      if (colStats.getStatsObjSize() > 0) {
        hms.updateColumnStats(colStats);
        timeline.markEvent("Updated column stats");
      }
      return colStats.getStatsObjSize();
    } catch (ImpalaException | RuntimeException e) {
      compensate(table, fresh, msTbl, newIds, fieldIds, s1, integrated, hms);
      throw e;
    }
  }

  /**
   * Looks for the trace of the commit of unknown outcome in a fresh load: the
   * registered file of 's1' or, without a file, the staged snapshot ids property; the
   * other staged properties only if the ids are not staged. If the table cannot be
   * loaded, the outcome stays unknown: the file is kept, since it may be registered,
   * and the columns of the command are removed from the ids.
   */
  private boolean committed(IcebergTable table, org.apache.iceberg.Table fresh,
      org.apache.hadoop.hive.metastore.api.Table msTbl,
      @Nullable Map<Integer, Long> newIds, Set<Integer> fieldIds, long s1,
      @Nullable StatisticsFile file, Map<String, String> staged, HmsStatsWriter hms,
      UnknownOutcomeException e) throws ImpalaRuntimeException {
    org.apache.iceberg.Table reloaded;
    try {
      reloaded = reload(table);
    } catch (ImpalaException | RuntimeException loadError) {
      e.failure.addSuppressed(loadError);
      LOG.error("Commit state of statistics of table {} snapshot {} is unknown: run "
          + "COMPUTE STATS", table.getFullName(), s1, e.failure);
      compensate(table, fresh, msTbl, newIds, fieldIds, s1, integrated(msTbl), hms);
      throw unknownState(table, "COMPUTE STATS", e.failure);
    }
    boolean applied;
    if (file != null) {
      applied = file.path().equals(registeredPath(reloaded, s1));
    } else {
      Map<String, String> props = reloaded.properties();
      // Another engine may change the other properties before the reload.
      Map<String, String> traces = staged.containsKey(IDS)
          ? Collections.singletonMap(IDS, staged.get(IDS)) : staged;
      applied = traces.entrySet().stream().allMatch(
          p -> Objects.equals(p.getValue(), props.get(p.getKey())));
    }
    if (applied) {
      LOG.warn("Statistics of table {} snapshot {} were committed despite the failure",
          table.getFullName(), s1, e.failure);
    }
    return applied;
  }

  /**
   * Best effort: writes the snapshot ids without the columns of the command; a failure
   * is only logged, the caller reports its own error.
   */
  private static void compensate(IcebergTable table, org.apache.iceberg.Table fresh,
      org.apache.hadoop.hive.metastore.api.Table msTbl,
      @Nullable Map<Integer, Long> newIds, Set<Integer> fieldIds, long s1,
      boolean integrated, HmsStatsWriter hms) {
    if (newIds == null) return;
    Map<Integer, Long> ids = new TreeMap<>(newIds);
    ids.keySet().removeAll(fieldIds);
    if (ids.equals(newIds)) return;
    runUninterrupted(() -> {
      try {
        writeIds(table, fresh, msTbl, ids, integrated, hms);
      } catch (ImpalaException | UnknownOutcomeException | RuntimeException e) {
        LOG.error("Failed to remove the failed columns from the snapshot ids of table {} "
            + "snapshot {}: run COMPUTE STATS", table.getFullName(), s1, e);
      }
    });
  }

  /**
   * Removes the columns 'colNames' from the snapshot ids before HMS statistics that
   * come without Iceberg statistics parameters are written for them. Records the ids
   * in 'msTbl', which the caller writes afterwards; writes no column statistics.
   */
  public void detachColumns(IcebergTable table,
      org.apache.hadoop.hive.metastore.api.Table msTbl, Collection<String> colNames,
      HmsStatsWriter hms) throws ImpalaException {
    Preconditions.checkState(table.isWriteLockedByCurrentThread());
    org.apache.iceberg.Table fresh = load(table);
    takeStoredIds(fresh, msTbl, integrated(msTbl), hms);
    Map<Integer, Long> ids =
        ComputeStatsSnapshotPropertyConverter.stringToMap(msTbl.getParameters().get(IDS));
    Map<Integer, Long> detached = new TreeMap<>(ids);
    for (String colName : colNames) {
      Types.NestedField field = fresh.schema().caseInsensitiveFindField(colName);
      if (field != null) detached.remove(field.fieldId());
    }
    boolean integrated = integrated(msTbl);
    // Without HMS integration a copy left in the Iceberg properties is stale.
    if (detached.equals(ids) && (integrated || !fresh.properties().containsKey(IDS))) {
      takeHmsMetadataLocation(msTbl, integrated, hms);
      return;
    }
    try {
      writeIds(table, fresh, msTbl, detached, integrated, hms);
    } catch (UnknownOutcomeException e) {
      LOG.error("Commit state of statistics of table {} is unknown: run COMPUTE STATS",
          table.getFullName(), e.failure);
      throw unknownState(table, "COMPUTE STATS", e.failure);
    } catch (RuntimeException e) {
      LOG.warn("Failed to remove columns {} from the snapshot ids of table {}", colNames,
          table.getFullName(), e);
      throw new ImpalaRuntimeException(e.getMessage(), e);
    }
  }

  /**
   * Removes the blobs written by Impala from the statistics files registered for any
   * snapshot and the snapshot ids property, in one Iceberg commit that is refused if a
   * touched registration changed meanwhile. A file that also has blobs of other engines
   * is replaced by a copy without the Impala blobs. The property is removed from 'msTbl'
   * only after the commit; without HMS integration 'msTbl' is written to HMS before
   * returning.
   */
  public void dropImpalaStatistics(IcebergTable table,
      org.apache.hadoop.hive.metastore.api.Table msTbl, HmsStatsWriter hms)
      throws ImpalaException {
    Preconditions.checkState(table.isWriteLockedByCurrentThread());
    org.apache.iceberg.Table fresh = load(table);
    Map<StatisticsFile, Set<BlobMetadata>> impalaBlobs = impalaBlobs(fresh);
    boolean integrated = integrated(msTbl);
    takeStoredIds(fresh, msTbl, integrated, hms);
    if (impalaBlobs.isEmpty() && !msTbl.getParameters().containsKey(IDS)
        && !fresh.properties().containsKey(IDS)) {
      takeHmsMetadataLocation(msTbl, integrated, hms);
      return;
    }
    List<StatisticsFile> copies = new ArrayList<>();
    if (integrated) {
      try {
        commit(() -> setIdsKnownToHms(fresh, msTbl));
      } catch (UnknownOutcomeException e) {
        throw dropUnknown(table, e.failure);
      } catch (RuntimeException e) {
        throw dropFailed(table, fresh, copies, e);
      }
    }
    try {
      Transaction txn = stageDrop(table, fresh, msTbl, impalaBlobs, integrated, copies);
      if (txn != null) commit(txn::commitTransaction);
    } catch (UnknownOutcomeException e) {
      if (!dropCommitted(table, copies, e)) {
        throw dropFailed(table, fresh, copies, e.failure);
      }
    } catch (ImpalaException | RuntimeException e) {
      throw dropFailed(table, fresh, copies, e);
    }
    msTbl.getParameters().remove(IDS);
    if (!integrated) hms.alterTableProperties(msTbl);
  }

  /**
   * Stages in a transaction on 'fresh' the removal of 'impalaBlobs' and of the snapshot
   * ids property. Copies of the files with blobs of other engines are written before
   * and added to 'copies'. Returns null if there is nothing to commit.
   */
  @Nullable
  private static Transaction stageDrop(IcebergTable table,
      org.apache.iceberg.Table fresh, org.apache.hadoop.hive.metastore.api.Table msTbl,
      Map<StatisticsFile, Set<BlobMetadata>> impalaBlobs, boolean integrated,
      List<StatisticsFile> copies) throws ImpalaRuntimeException {
    Map<Long, String> guarded = new HashMap<>();
    Map<Long, Optional<StatisticsFile>> replacements = new LinkedHashMap<>();
    for (Map.Entry<StatisticsFile, Set<BlobMetadata>> e : impalaBlobs.entrySet()) {
      StatisticsFile file = e.getKey();
      Optional<StatisticsFile> copy =
          PUFFIN_WRITER.rewriteWithout(fresh, table.getFullName(), file, e.getValue());
      copy.ifPresent(copies::add);
      replacements.put(file.snapshotId(), copy);
      guarded.put(file.snapshotId(), file.path());
    }
    Transaction txn = Transactions.newTransaction(fresh.name(),
        new RegistrationGuard(ops(fresh), table.getFullName(), guarded,
            Collections.emptySet()));
    boolean dropIds = txn.table().properties().containsKey(IDS);
    if (replacements.isEmpty() && !dropIds) return null;
    if (!replacements.isEmpty()) {
      UpdateStatistics update = txn.updateStatistics();
      replacements.forEach((snapshotId, copy) -> {
        if (copy.isPresent()) {
          update.setStatistics(copy.get());
        } else {
          update.removeStatistics(snapshotId);
        }
      });
      update.commit();
    }
    Map<String, String> catalogIds =
        integrated ? catalogIds(msTbl) : Collections.emptyMap();
    if (!catalogIds.isEmpty()) IcebergCatalogOpExecutor.setTblProperties(txn, catalogIds);
    if (dropIds) {
      IcebergCatalogOpExecutor.unsetTblProperties(txn, Collections.singletonList(IDS));
    }
    return txn;
  }

  /**
   * Looks for the trace of the drop commit of unknown outcome in a fresh load: a
   * registered copy, or neither Impala blobs nor the snapshot ids property left. If the
   * table cannot be loaded, the outcome stays unknown and the copies are kept, since
   * they may be registered.
   */
  private boolean dropCommitted(IcebergTable table, List<StatisticsFile> copies,
      UnknownOutcomeException e) throws ImpalaRuntimeException {
    org.apache.iceberg.Table reloaded;
    try {
      reloaded = reload(table);
    } catch (ImpalaException | RuntimeException loadError) {
      e.failure.addSuppressed(loadError);
      throw dropUnknown(table, e.failure);
    }
    Set<String> registered = reloaded.statisticsFiles().stream()
        .map(StatisticsFile::path).collect(Collectors.toSet());
    boolean applied = copies.stream().anyMatch(c -> registered.contains(c.path()))
        || (copies.isEmpty() && impalaBlobs(reloaded).isEmpty()
            && !reloaded.properties().containsKey(IDS));
    if (applied) {
      LOG.warn("Puffin statistics of table {} were dropped despite the failure",
          table.getFullName(), e.failure);
    }
    return applied;
  }

  /** Impala blobs of each registered statistics file that has any. */
  private static Map<StatisticsFile, Set<BlobMetadata>> impalaBlobs(
      org.apache.iceberg.Table t) {
    Map<StatisticsFile, Set<BlobMetadata>> res = new LinkedHashMap<>();
    for (StatisticsFile file : t.statisticsFiles()) {
      Set<BlobMetadata> blobs = file.blobMetadata().stream()
          .filter(b -> b.properties().containsKey(
              IcebergPuffinStatsWriter.IMPALA_CREATED_BY))
          .collect(Collectors.toSet());
      if (!blobs.isEmpty()) res.put(file, blobs);
    }
    return res;
  }

  /** Deletes the copies of the command, which are not registered. */
  private static ImpalaRuntimeException dropFailed(IcebergTable table,
      org.apache.iceberg.Table fresh, List<StatisticsFile> copies, Exception cause) {
    LOG.warn("Failed to drop Puffin statistics of table {}", table.getFullName(), cause);
    runUninterrupted(() -> {
      for (StatisticsFile copy : copies) PUFFIN_WRITER.deleteQuietly(fresh, copy.path());
    });
    return new ImpalaRuntimeException(String.format(
        "Failed to drop Puffin statistics of table %s: %s", table.getFullName(),
        cause.getMessage()), cause);
  }

  private static ImpalaRuntimeException dropUnknown(IcebergTable table,
      RuntimeException cause) {
    LOG.error("Commit state of statistics of table {} is unknown: run DROP STATS again",
        table.getFullName(), cause);
    return unknownState(table, "DROP STATS again", cause);
  }

  /**
   * Writes 'ids' as the snapshot ids property: with HMS integration in an Iceberg
   * transaction that HMS follows, otherwise to HMS after removing the Iceberg copy.
   */
  private static void writeIds(IcebergTable table, org.apache.iceberg.Table fresh,
      org.apache.hadoop.hive.metastore.api.Table msTbl, Map<Integer, Long> ids,
      boolean integrated, HmsStatsWriter hms)
      throws ImpalaException, UnknownOutcomeException {
    if (integrated && ids.isEmpty()) commit(() -> setIdsKnownToHms(fresh, msTbl));
    Transaction txn = Transactions.newTransaction(fresh.name(),
        new RegistrationGuard(ops(fresh), table.getFullName(), Collections.emptyMap(),
            Collections.emptySet()));
    if (!stageProperties(txn, msTbl, ids, integrated).isEmpty()) {
      commit(txn::commitTransaction);
    }
    if (!integrated) hms.alterTableProperties(msTbl);
  }

  /**
   * Stages in 'txn' the Iceberg properties of a write of 'ids' and records 'ids' in
   * 'msTbl'; null 'ids' keeps the property. Returns the staged properties, a removed
   * one with a null value.
   */
  private static Map<String, String> stageProperties(Transaction txn,
      org.apache.hadoop.hive.metastore.api.Table msTbl, @Nullable Map<Integer, Long> ids,
      boolean integrated) {
    Map<String, String> staged =
        integrated ? commitProperties(msTbl) : new LinkedHashMap<>();
    String value = null;
    if (ids != null && !ids.isEmpty()) {
      value = ComputeStatsSnapshotPropertyConverter.mapToString(new TreeMap<>(ids));
      msTbl.putToParameters(IDS, value);
      if (integrated) staged.put(IDS, value);
    } else if (ids != null) {
      msTbl.getParameters().remove(IDS);
    }
    if (!staged.isEmpty()) IcebergCatalogOpExecutor.setTblProperties(txn, staged);
    if (ids != null && (!integrated || value == null)
        && txn.table().properties().containsKey(IDS)) {
      IcebergCatalogOpExecutor.unsetTblProperties(txn, Collections.singletonList(IDS));
      staged.put(IDS, null);
    }
    return staged;
  }

  /**
   * Runs a commit of Iceberg metadata. A rejected commit propagates as is; after any
   * other failure the commit may have been applied.
   */
  private static void commit(Runnable commit) throws UnknownOutcomeException {
    try {
      commit.run();
    } catch (CommitFailedException | ValidationException e) {
      throw e;
    } catch (RuntimeException e) {
      if (Throwables.getCausalChain(e).stream()
          .anyMatch(c -> c instanceof InterruptedException)) {
        Thread.currentThread().interrupt();
      }
      throw new UnknownOutcomeException(e);
    }
  }

  /** Loads the table after a commit that may have been interrupted. */
  private org.apache.iceberg.Table reload(IcebergTable table) throws ImpalaException {
    return uninterrupted(() -> loader_.load(table));
  }

  @FunctionalInterface
  private interface Task<T, E extends Exception> {
    T run() throws E;
  }

  /**
   * Runs 'task' with the interrupt flag cleared, then raises it again if it was set. An
   * interrupted commit leaves the flag raised, which would fail the IO of the cleanup
   * and of the load that follow it, or be lost in them.
   */
  private static <T, E extends Exception> T uninterrupted(Task<T, E> task) throws E {
    boolean interrupted = Thread.interrupted();
    try {
      return task.run();
    } finally {
      if (interrupted) Thread.currentThread().interrupt();
    }
  }

  private static void runUninterrupted(Runnable task) {
    uninterrupted(() -> {
      task.run();
      return null;
    });
  }

  private org.apache.iceberg.Table load(IcebergTable table)
      throws ImpalaRuntimeException {
    try {
      return loader_.load(table);
    } catch (ImpalaException | RuntimeException e) {
      LOG.warn("Failed to load Iceberg table {}", table.getFullName(), e);
      throw new ImpalaRuntimeException(String.format(
          "Failed to load Iceberg table %s: %s", table.getFullName(), e.getMessage()), e);
    }
  }

  /** Deletes the file of the command, which is not registered. */
  private static ImpalaRuntimeException registerFailed(IcebergTable table,
      org.apache.iceberg.Table fresh, long s1, @Nullable StatisticsFile file,
      RuntimeException cause) {
    LOG.warn("Failed to register Puffin statistics of table {} snapshot {}",
        table.getFullName(), s1, cause);
    if (file != null) {
      runUninterrupted(() -> PUFFIN_WRITER.deleteQuietly(fresh, file.path()));
    }
    return new ImpalaRuntimeException(String.format(
        "Failed to register Puffin statistics of table %s: %s", table.getFullName(),
        cause.getMessage()), cause);
  }

  /** 'advice' is the statement to run, e.g. "COMPUTE STATS". */
  private static ImpalaRuntimeException unknownState(IcebergTable table, String advice,
      RuntimeException cause) {
    return new ImpalaRuntimeException(String.format(
        "Commit state of statistics of table %s is unknown; run %s.",
        table.getFullName(), advice), cause);
  }

  /**
   * Called when a command commits nothing. Without HMS integration the caller writes
   * 'msTbl' afterwards; a HiveCatalog table must get the metadata location from HMS
   * first, since the cache may miss an Iceberg commit whose HMS write failed.
   */
  private static void takeHmsMetadataLocation(
      org.apache.hadoop.hive.metastore.api.Table msTbl, boolean integrated,
      HmsStatsWriter hms) throws ImpalaException {
    if (!integrated && IcebergUtil.isHiveCatalog(msTbl)) hms.alterTableProperties(msTbl);
  }

  /**
   * Replaces the snapshot ids of the cached 'msTbl', which may be stale, with the
   * stored ones: with HMS integration the Iceberg properties, else the HMS parameters.
   */
  private static void takeStoredIds(org.apache.iceberg.Table fresh,
      org.apache.hadoop.hive.metastore.api.Table msTbl, boolean integrated,
      HmsStatsWriter hms) throws ImpalaException {
    String value = integrated ? fresh.properties().get(IDS) : null;
    if (value == null) value = hms.readTableParameters(msTbl).get(IDS);
    putIds(msTbl, value);
  }

  /**
   * Best effort: replaces the snapshot ids of the cached HMS table of 'table' with
   * those in HMS, so that a later DDL writing the cached table does not restore old
   * ones. If HMS cannot be read, the ids are removed from the cache: such a DDL then
   * drops them, which loses the bases of the columns but never pairs their statistics
   * with another snapshot. A failure is only logged.
   */
  public static void syncCachedIds(IcebergTable table, HmsStatsWriter hms) {
    Preconditions.checkState(table.isWriteLockedByCurrentThread());
    org.apache.hadoop.hive.metastore.api.Table msTbl = table.getMetaStoreTable();
    try {
      putIds(msTbl, hms.readTableParameters(msTbl).get(IDS));
    } catch (ImpalaException | RuntimeException e) {
      putIds(msTbl, null);
      LOG.warn("Failed to read the snapshot ids of table {} from HMS; they are removed "
          + "from the cached table until REFRESH", table.getFullName(), e);
    }
  }

  private static void putIds(org.apache.hadoop.hive.metastore.api.Table msTbl,
      @Nullable String value) {
    if (value != null) {
      msTbl.putToParameters(IDS, value);
    } else {
      msTbl.getParameters().remove(IDS);
    }
  }

  private static boolean integrated(org.apache.hadoop.hive.metastore.api.Table msTbl)
      throws ImpalaRuntimeException {
    return CatalogOpExecutor.isIcebergHmsIntegrationEnabled(msTbl);
  }

  /**
   * LEGACY and FULL overwrite the statistics of their snapshot. An incremental request
   * whose base does not match HMS removes the columns of the command from the stored
   * snapshot ids, so that the next analysis does not pick the same base, and fails.
   */
  Reconcile reconcile(IcebergTable table, org.apache.iceberg.Table fresh,
      org.apache.hadoop.hive.metastore.api.Table msTbl, TIcebergPuffinStatsParams p,
      Set<Integer> fieldIds, HmsStatsWriter hms) throws ImpalaException {
    if (p.getMode() != TIcebergStatsMode.INCREMENTAL) return Reconcile.WRITE;
    IcebergIncrementalReconciler.Outcome outcome =
        IcebergIncrementalReconciler.reconcile(table, fresh, msTbl, p, fieldIds, hms);
    switch (outcome.verdict) {
      case WRITE: return Reconcile.WRITE;
      case ALREADY_APPLIED: return Reconcile.ALREADY_APPLIED;
      case CONFLICT: return Reconcile.CONFLICT;
      default: break;
    }
    Preconditions.checkState(
        outcome.verdict == IcebergIncrementalReconciler.Verdict.BASE_MISMATCH);
    Map<Integer, Long> storedIds = ComputeStatsSnapshotPropertyConverter.stringToMap(
        msTbl.getParameters().get(IDS));
    boolean integrated = integrated(msTbl);
    compensate(table, fresh, idsOnlyTable(table, msTbl, integrated), storedIds,
        fieldIds, p.getCalc_snapshot_id(), integrated, hms);
    throw new IcebergStatsBaseMismatchException(
        table.getFullName(), outcome.mismatchedColumns);
  }

  /**
   * HMS table for a write of the snapshot ids alone: the cached table with the ids and
   * catalog ids of 'msTbl', but without the table statistics of the failed request.
   * Without HMS integration all its parameters are written, so the cached ones keep
   * the stored statistics; with it the statistics time must not reach Iceberg.
   */
  private static org.apache.hadoop.hive.metastore.api.Table idsOnlyTable(
      IcebergTable table, org.apache.hadoop.hive.metastore.api.Table msTbl,
      boolean integrated) {
    org.apache.hadoop.hive.metastore.api.Table res =
        table.getMetaStoreTable().deepCopy();
    putIds(res, msTbl.getParameters().get(IDS));
    catalogIds(msTbl).forEach(res::putToParameters);
    if (integrated) {
      res.getParameters().remove(HdfsTable.TBL_PROP_LAST_COMPUTE_STATS_TIME);
    }
    return res;
  }

  /**
   * Builds the HMS statistics object of every column once; with a sketch its NDV is
   * the sketch estimate, so the blob and HMS store the same number.
   */
  Map<Integer, ColumnResult> finalStats(IcebergTable table,
      org.apache.iceberg.Table fresh, TAlterTableUpdateStatsParams params,
      HmsStatsWriter hms) throws ImpalaException {
    TIcebergPuffinStatsParams p = params.getIceberg_puffin_stats();
    if (p.getMode() == TIcebergStatsMode.INCREMENTAL) {
      return IcebergIncrementalReconciler.finalStats(table, fresh, params);
    }
    boolean full = p.getMode() == TIcebergStatsMode.FULL;
    long numRows = params.isSetTable_stats() ? params.getTable_stats().getNum_rows() : -1;
    Map<Integer, ColumnResult> res = new TreeMap<>();
    for (Map.Entry<String, Integer> e : p.getColumn_field_ids().entrySet()) {
      String colName = e.getKey();
      int fieldId = e.getValue();
      Type type = IcebergSchemaConverter.toImpalaType(fresh.schema().findType(fieldId));
      TColumnStats stats = params.getColumn_stats().get(colName);
      byte[] sketch = null;
      if (full) {
        stats = stats.deepCopy();
        ByteBuffer buf = p.isSetTheta_sketches() ? p.getTheta_sketches().get(fieldId)
            : null;
        if (buf != null) {
          sketch = bytes(buf);
          // A NULL sketch arrives as no bytes. The aggregate returns the empty sketch for
          // a column without values, so this only keeps an invalid blob out of the file.
          if (sketch.length == 0) sketch = IcebergStatsMerger.emptySketch().toByteArray();
          stats.setNum_distinct_values(
              estimate(colName, sketch, p.getCalc_snapshot_id()));
        } else {
          stats.setNum_distinct_values(-1);
        }
      }
      res.put(fieldId, columnResult(colName, type, stats, sketch, numRows));
    }
    return res;
  }

  /**
   * Builds the result of one column. With a sketch, 'stats' carries the sketch estimate
   * as NDV; a negative 'numRows' means the row count is unknown.
   */
  static ColumnResult columnResult(String colName, Type type, TColumnStats stats,
      @Nullable byte[] sketch, long numRows) {
    ColumnStatisticsData data =
        ColumnStats.createHiveColStatsData(numRows, stats, type);
    ColumnStatisticsObj obj = data == null ? null
        : new ColumnStatisticsObj(colName, type.toString().toLowerCase(), data);
    long ndv;
    if (sketch == null) {
      ndv = stats.getNum_distinct_values();
    } else if (type.getPrimitiveType() == PrimitiveType.BOOLEAN || data == null) {
      long estimate = stats.getNum_distinct_values();
      ndv = numRows < 0 ? estimate : Math.min(estimate, numRows);
    } else {
      ndv = ndvOf(data);
    }
    return new ColumnResult(colName, stats, sketch, ndv, obj);
  }

  private static void checkInput(IcebergTable table, TAlterTableUpdateStatsParams params)
      throws ImpalaRuntimeException {
    TIcebergPuffinStatsParams p = params.getIceberg_puffin_stats();
    Preconditions.checkNotNull(p);
    Map<String, Integer> columnFieldIds = p.isSetColumn_field_ids()
        ? p.getColumn_field_ids() : Collections.emptyMap();
    Map<String, TColumnStats> columnStats = params.isSetColumn_stats()
        ? params.getColumn_stats() : Collections.emptyMap();
    if (!columnFieldIds.keySet().equals(columnStats.keySet())) {
      throw invalidInput(table, String.format(
          "keys of column_field_ids %s differ from keys of column_stats %s",
          new TreeSet<>(columnFieldIds.keySet()), new TreeSet<>(columnStats.keySet())));
    }
    List<Integer> thetaFieldIds = p.isSetTheta_field_ids() ? p.getTheta_field_ids()
        : Collections.emptyList();
    Map<Integer, ByteBuffer> sketches = p.isSetTheta_sketches() ? p.getTheta_sketches()
        : Collections.emptyMap();
    boolean legacy = p.getMode() == TIcebergStatsMode.LEGACY;
    if (legacy && (p.isSetTheta_field_ids() || p.isSetTheta_sketches())) {
      throw invalidInput(table, "LEGACY mode with theta sketches");
    }
    for (Integer fieldId : new TreeSet<>(sketches.keySet())) {
      if (!thetaFieldIds.contains(fieldId)) {
        throw invalidInput(table,
            "theta sketch of field id " + fieldId + " not in theta_field_ids");
      }
    }
    for (Integer fieldId : thetaFieldIds) {
      if (!sketches.containsKey(fieldId)) {
        throw invalidInput(table, "no theta sketch for field id " + fieldId);
      }
    }
    Set<Integer> fieldIds = new HashSet<>(columnFieldIds.values());
    for (Integer fieldId : thetaFieldIds) {
      if (!fieldIds.contains(fieldId)) {
        throw invalidInput(table,
            "theta field id " + fieldId + " not in column_field_ids");
      }
    }
    if (!legacy && p.isSampled()) {
      throw invalidInput(table, "sampled statistics in " + p.getMode() + " mode");
    }
    if (!thetaFieldIds.isEmpty() && p.getCalc_snapshot_id() == -1) {
      throw invalidInput(table, "theta sketches without a snapshot");
    }
  }

  private static ImpalaRuntimeException invalidInput(IcebergTable table, String what) {
    return new ImpalaRuntimeException(String.format(
        "Invalid Iceberg statistics parameters for table %s: %s", table.getFullName(),
        what));
  }

  /**
   * The snapshot and the columns of the analysis must still exist with the same names:
   * the statistics describe them, not the current state of the table.
   */
  private static void checkAgainstAnalysis(IcebergTable table,
      org.apache.iceberg.Table fresh, TIcebergPuffinStatsParams p)
      throws ImpalaRuntimeException {
    long s1 = p.getCalc_snapshot_id();
    if (s1 != -1 && fresh.snapshot(s1) == null) {
      throw new ImpalaRuntimeException(snapshotGone(s1, table.getFullName()));
    }
    for (Map.Entry<String, Integer> e : p.getColumn_field_ids().entrySet()) {
      Types.NestedField field = fresh.schema().findField(e.getValue());
      if (field == null || !field.name().equalsIgnoreCase(e.getKey())) {
        throw new ImpalaRuntimeException(String.format(
            "Column %s of table %s was changed concurrently; run COMPUTE STATS again.",
            e.getKey(), table.getFullName()));
      }
    }
  }

  private static String snapshotGone(long snapshotId, String tableName) {
    return String.format("Snapshot %d of table %s no longer exists; run COMPUTE STATS "
        + "again.", snapshotId, tableName);
  }

  /**
   * The previous ids with the columns of this command set to 's1', or removed if the
   * statistics come from a sample and so cannot be a base.
   */
  private static TreeMap<Integer, Long> newIds(
      org.apache.hadoop.hive.metastore.api.Table msTbl, Set<Integer> fieldIds, long s1,
      boolean sampled) {
    TreeMap<Integer, Long> ids =
        ComputeStatsSnapshotPropertyConverter.stringToMap(msTbl.getParameters().get(IDS));
    for (int fieldId : fieldIds) {
      if (sampled) {
        ids.remove(fieldId);
      } else {
        ids.put(fieldId, s1);
      }
    }
    if (ids.size() > ComputeStatsSnapshotPropertyConverter.MAX_FIELD_IDS) {
      // The reader would reject the merged value, like any unreadable previous one.
      ids = new TreeMap<>();
      if (!sampled) {
        for (int fieldId : fieldIds) ids.put(fieldId, s1);
      }
    }
    return ids;
  }

  /**
   * With HMS integration the parameters of the HMS table follow the Iceberg properties
   * on commit, but a key missing from the Iceberg properties is never removed from HMS.
   * Setting it first lets the following transaction remove it from both. The commit
   * carries the catalog ids so that event processing treats it as its own change.
   */
  private static void setIdsKnownToHms(org.apache.iceberg.Table fresh,
      org.apache.hadoop.hive.metastore.api.Table msTbl) {
    String value = msTbl.getParameters().get(IDS);
    if (value == null || fresh.properties().containsKey(IDS)) return;
    UpdateProperties update = fresh.updateProperties();
    commitProperties(msTbl).forEach(update::set);
    update.set(IDS, value).commit();
  }

  @Nullable
  private static StatisticsFile writePuffinFile(IcebergTable table,
      org.apache.iceberg.Table fresh, long s1, Map<Integer, ColumnResult> results,
      EventSequence timeline) throws ImpalaRuntimeException {
    Map<Integer, byte[]> sketches = new TreeMap<>();
    Map<Integer, Long> ndvs = new TreeMap<>();
    for (Map.Entry<Integer, ColumnResult> e : results.entrySet()) {
      if (e.getValue().sketch == null) continue;
      sketches.put(e.getKey(), e.getValue().sketch);
      ndvs.put(e.getKey(), e.getValue().ndv);
    }
    if (sketches.isEmpty()) return null;
    StatisticsFile file =
        PUFFIN_WRITER.write(fresh, table.getFullName(), s1, sketches, ndvs);
    if (file != null) timeline.markEvent("Wrote Puffin statistics file");
    return file;
  }

  private static Map<String, String> commitProperties(
      org.apache.hadoop.hive.metastore.api.Table msTbl) {
    Map<String, String> props = new LinkedHashMap<>();
    String time = msTbl.getParameters().get(HdfsTable.TBL_PROP_LAST_COMPUTE_STATS_TIME);
    if (time != null) props.put(HdfsTable.TBL_PROP_LAST_COMPUTE_STATS_TIME, time);
    props.putAll(catalogIds(msTbl));
    return props;
  }

  /**
   * The catalog ids of 'msTbl', which let event processing treat the commit as a change
   * of its own. Without active HMS event processing there are none; that is fine.
   */
  private static Map<String, String> catalogIds(
      org.apache.hadoop.hive.metastore.api.Table msTbl) {
    Map<String, String> ids = new LinkedHashMap<>();
    for (String key : new String[] {MetastoreEventPropertyKey.CATALOG_SERVICE_ID.getKey(),
        MetastoreEventPropertyKey.CATALOG_VERSION.getKey()}) {
      String value = msTbl.getParameters().get(key);
      if (value != null) ids.put(key, value);
    }
    return ids;
  }

  private static ColumnStatistics hmsColumnStats(IcebergTable table,
      Map<Integer, ColumnResult> results) {
    ColumnStatistics colStats = MetastoreShim.createNewHiveColStats();
    colStats.setStatsDesc(
        new ColumnStatisticsDesc(true, table.getDb().getName(), table.getName()));
    for (ColumnResult r : results.values()) {
      if (r.hmsStats != null) colStats.addToStatsObj(r.hmsStats);
    }
    return colStats;
  }

  private static long estimate(String colName, byte[] sketch, long s1)
      throws ImpalaRuntimeException {
    try {
      return Math.round(CompactSketch.wrap(Memory.wrap(sketch)).getEstimate());
    } catch (RuntimeException e) {
      // Nothing is written yet, so any parse failure is a plain error of the command.
      throw new ImpalaRuntimeException(String.format(
          "Invalid theta sketch for column %s (snapshot %d)", colName, s1), e);
    }
  }

  private static long ndvOf(ColumnStatisticsData data) {
    if (data.isSetLongStats()) return data.getLongStats().getNumDVs();
    if (data.isSetDoubleStats()) return data.getDoubleStats().getNumDVs();
    if (data.isSetStringStats()) return data.getStringStats().getNumDVs();
    if (data.isSetDecimalStats()) return data.getDecimalStats().getNumDVs();
    if (data.isSetDateStats()) return data.getDateStats().getNumDVs();
    throw new IllegalStateException("No NDV in HMS statistics " + data);
  }

  static byte[] bytes(ByteBuffer buf) {
    ByteBuffer dup = buf.duplicate();
    byte[] res = new byte[dup.remaining()];
    dup.get(res);
    return res;
  }

  private static TableOperations ops(org.apache.iceberg.Table fresh) {
    return ((HasTableOperations) fresh).operations();
  }

  @Nullable
  private static String registeredPath(org.apache.iceberg.Table fresh, long snapshotId) {
    return registeredPath(fresh.statisticsFiles(), snapshotId);
  }

  @Nullable
  private static String registeredPath(List<StatisticsFile> files, long snapshotId) {
    for (StatisticsFile f : files) {
      if (f.snapshotId() == snapshotId) return f.path();
    }
    return null;
  }

  /**
   * Table operations that refuse a commit if the statistics file registered for a
   * guarded snapshot is no longer the one read before the command wrote anything:
   * another engine replaced it, and committing would silently drop its blobs. It also
   * refuses a commit if a required snapshot was expired meanwhile: statistics
   * registered for a missing snapshot are never removed by expiry. The checks run on
   * every commit attempt, including the ones Iceberg retries on newer metadata.
   */
  static final class RegistrationGuard implements TableOperations {
    private final TableOperations delegate_;
    private final String tableName_;
    /** Snapshot id -> path of its registered statistics file, null if none. */
    private final Map<Long, String> guarded_;
    /** Snapshots that must still exist. */
    private final Set<Long> required_;

    RegistrationGuard(TableOperations delegate, String tableName,
        Map<Long, String> guarded, Set<Long> required) {
      delegate_ = delegate;
      tableName_ = tableName;
      guarded_ = new HashMap<>(guarded);
      required_ = new HashSet<>(required);
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      List<StatisticsFile> registered = base == null ? Collections.emptyList()
          : base.statisticsFiles();
      for (long snapshotId : required_) {
        if (base == null || base.snapshot(snapshotId) == null) {
          throw new ValidationException("%s", snapshotGone(snapshotId, tableName_));
        }
      }
      for (Map.Entry<Long, String> e : guarded_.entrySet()) {
        String path = registeredPath(registered, e.getKey());
        if (path == null ? e.getValue() != null : !path.equals(e.getValue())) {
          throw new ValidationException(
              "Statistics of snapshot %d of table %s changed concurrently", e.getKey(),
              tableName_);
        }
      }
      delegate_.commit(base, metadata);
    }

    @Override
    public TableMetadata current() { return delegate_.current(); }

    @Override
    public TableMetadata refresh() { return delegate_.refresh(); }

    @Override
    public FileIO io() { return delegate_.io(); }

    @Override
    public EncryptionManager encryption() { return delegate_.encryption(); }

    @Override
    public String metadataFileLocation(String fileName) {
      return delegate_.metadataFileLocation(fileName);
    }

    @Override
    public LocationProvider locationProvider() { return delegate_.locationProvider(); }

    @Override
    public TableOperations temp(TableMetadata uncommittedMetadata) {
      return delegate_.temp(uncommittedMetadata);
    }

    @Override
    public long newSnapshotId() { return delegate_.newSnapshotId(); }

    @Override
    public boolean requireStrictCleanup() { return delegate_.requireStrictCleanup(); }
  }
}
