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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.annotation.Nullable;

import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.Sketch;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.IcebergColumn;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.iceberg.IcebergPuffinBlobReader;
import org.apache.impala.catalog.iceberg.IcebergPuffinStatsWriter;
import org.apache.impala.catalog.iceberg.IcebergStatsMerger;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.thrift.TAlterTableUpdateStatsParams;
import org.apache.impala.thrift.TColumnStats;
import org.apache.impala.thrift.TIcebergNdvBase;
import org.apache.impala.thrift.TIcebergPuffinStatsParams;
import org.apache.impala.util.IcebergUtil.ComputeStatsSnapshotPropertyConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Rules of incremental statistics in catalogd: whether a request still matches the
 * stored statistics, and the statistics of each column merged from its base and the
 * gap. Writes nothing and takes no locks; the caller holds the write lock of the table
 * and does all writing.
 */
final class IcebergIncrementalReconciler {
  private static final Logger LOG =
      LoggerFactory.getLogger(IcebergIncrementalReconciler.class);

  enum Verdict { WRITE, ALREADY_APPLIED, CONFLICT, BASE_MISMATCH }

  static final class Outcome {
    final Verdict verdict;
    /** Columns whose base does not match HMS, in the order of the field ids. */
    final List<String> mismatchedColumns;

    Outcome(Verdict verdict, List<String> mismatchedColumns) {
      this.verdict = verdict;
      this.mismatchedColumns = Collections.unmodifiableList(mismatchedColumns);
    }
  }

  /** Column statistics of HMS, read at most once and only when a rule needs them. */
  private static final class HmsColumnStats {
    private final org.apache.hadoop.hive.metastore.api.Table msTbl_;
    private final List<String> names_;
    private final IcebergStatsUpdater.HmsStatsWriter hms_;
    private Map<String, ColumnStatisticsObj> stats_;

    HmsColumnStats(org.apache.hadoop.hive.metastore.api.Table msTbl, List<String> names,
        IcebergStatsUpdater.HmsStatsWriter hms) {
      msTbl_ = msTbl;
      names_ = names;
      hms_ = hms;
    }

    Map<String, ColumnStatisticsObj> get() throws ImpalaException {
      if (stats_ == null) stats_ = hms_.readColumnStats(msTbl_, names_);
      return stats_;
    }
  }

  private IcebergIncrementalReconciler() {}

  /**
   * Decides what an incremental request does with the statistics stored now. 'msTbl'
   * carries the stored snapshot ids; the registered statistics files are those of
   * 'fresh'. 'fieldIds' are the columns of the request, in the order mismatches are
   * listed. Throws on a malformed request; a failure to read HMS is thrown as is.
   */
  static Outcome reconcile(IcebergTable table, org.apache.iceberg.Table fresh,
      org.apache.hadoop.hive.metastore.api.Table msTbl, TIcebergPuffinStatsParams p,
      Set<Integer> fieldIds, IcebergStatsUpdater.HmsStatsWriter hms)
      throws ImpalaException {
    Preconditions.checkState(table.isWriteLockedByCurrentThread());
    checkFormat(table, p);
    long s1 = p.getCalc_snapshot_id();
    long bh = p.getMetrics_base_snapshot_id();
    Map<Integer, Long> ids = ComputeStatsSnapshotPropertyConverter.stringToMap(
        msTbl.getParameters().get(IcebergTable.COMPUTE_STATS_SNAPSHOT_IDS));
    Map<Integer, String> names = names(p);
    Map<Integer, IcebergColumn> columns = cachedColumns(table);
    List<String> fieldNames = new ArrayList<>();
    for (int f : fieldIds) fieldNames.add(names.get(f));
    HmsColumnStats hmsStats = new HmsColumnStats(msTbl, fieldNames, hms);

    Outcome outcome;
    int rule;
    List<String> failures = new ArrayList<>();
    if (allEqual(ids, fieldIds, bh) && fresh.snapshot(bh) != null
        && basesRegistered(fresh, p.getNdv_bases())) {
      rule = 1;
      List<String> mismatched = new ArrayList<>();
      for (int f : fieldIds) {
        String failure = checkBase(fresh, columns.get(f), names.get(f), f, bh, hmsStats);
        if (failure == null) continue;
        mismatched.add(names.get(f));
        failures.add(names.get(f) + ": " + failure);
      }
      outcome = new Outcome(mismatched.isEmpty() ? Verdict.WRITE : Verdict.BASE_MISMATCH,
          mismatched);
    } else if (allEqual(ids, fieldIds, s1)
        && appliedAtS1(fresh, p, columns, names, hmsStats)) {
      rule = 2;
      outcome = new Outcome(Verdict.ALREADY_APPLIED, new ArrayList<>());
    } else {
      rule = 3;
      outcome = new Outcome(Verdict.CONFLICT, new ArrayList<>());
    }
    LOG.info("Incremental statistics of table {} for snapshot {} with metrics base {}: "
        + "{} by rule {}{}", table.getFullName(), s1, bh, outcome.verdict, rule,
        failures.isEmpty() ? "" : "; failed checks: " + String.join(", ", failures));
    return outcome;
  }

  /**
   * Statistics of every column of the request: the HMS statistics of its metrics base
   * merged with the gap, and the union of its NDV base sketch with the gap sketch.
   * Valid only after reconcile() returned WRITE for the same 'fresh'; the caller holds
   * the write lock of 'table'.
   */
  static Map<Integer, IcebergStatsUpdater.ColumnResult> finalStats(IcebergTable table,
      org.apache.iceberg.Table fresh, TAlterTableUpdateStatsParams params)
      throws ImpalaException {
    Preconditions.checkState(table.isWriteLockedByCurrentThread());
    TIcebergPuffinStatsParams p = params.getIceberg_puffin_stats();
    long s1 = p.getCalc_snapshot_id();
    long numRows = params.isSetTable_stats() ? params.getTable_stats().getNum_rows() : -1;
    Map<Integer, String> names = names(p);
    Map<Integer, IcebergColumn> columns = cachedColumns(table);
    Map<Integer, TIcebergNdvBase> bases = new HashMap<>();
    Map<Integer, byte[]> baseSketches = readBaseSketches(fresh, p, names, bases);
    Long baseRecords = totalRecords(fresh, p.getMetrics_base_snapshot_id());
    Set<Integer> theta = p.isSetTheta_field_ids()
        ? new HashSet<>(p.getTheta_field_ids()) : Collections.emptySet();

    Map<Integer, IcebergStatsUpdater.ColumnResult> res = new TreeMap<>();
    for (Map.Entry<String, Integer> e : p.getColumn_field_ids().entrySet()) {
      String colName = e.getKey();
      int f = e.getValue();
      Column col = columns.get(f);
      Preconditions.checkState(col != null, "no cached column with field id %s", f);
      Type type = col.getType();
      TColumnStats stats = IcebergStatsMerger.mergeMetrics(col.getStats().toThrift(),
          params.getColumn_stats().get(colName), baseRecords, type);
      byte[] sketch = null;
      if (theta.contains(f)) {
        Sketch base = IcebergStatsMerger.parse(baseSketches.get(f), colName,
            bases.get(f).getSnapshot_id());
        Sketch gap = IcebergStatsMerger.parse(
            IcebergStatsUpdater.bytes(p.getTheta_sketches().get(f)), colName, s1);
        CompactSketch union = IcebergStatsMerger.union(base, gap);
        sketch = union.toByteArray();
        stats.setNum_distinct_values(IcebergStatsMerger.estimate(union));
      } else {
        stats.setNum_distinct_values(-1);
      }
      res.put(f, IcebergStatsUpdater.columnResult(colName, type, stats, sketch, numRows));
    }
    return res;
  }

  private static void checkFormat(IcebergTable table, TIcebergPuffinStatsParams p)
      throws ImpalaRuntimeException {
    if (!p.isSetMetrics_base_snapshot_id()) {
      throw invalidRequest(table, "no metrics_base_snapshot_id");
    }
    if (!p.isSetNdv_bases()) throw invalidRequest(table, "no ndv_bases");
    Set<Integer> baseFields = new TreeSet<>();
    for (TIcebergNdvBase base : p.getNdv_bases()) {
      if (!baseFields.add(base.getField_id())) {
        throw invalidRequest(table,
            "field id " + base.getField_id() + " repeated in ndv_bases");
      }
    }
    Set<Integer> theta = new TreeSet<>();
    if (p.isSetTheta_field_ids()) {
      for (int f : p.getTheta_field_ids()) {
        if (!theta.add(f)) {
          throw invalidRequest(table, "field id " + f + " repeated in theta_field_ids");
        }
      }
    }
    if (!baseFields.equals(theta)) {
      throw invalidRequest(table, String.format(
          "field ids of ndv_bases %s differ from theta_field_ids %s", baseFields, theta));
    }
  }

  private static ImpalaRuntimeException invalidRequest(IcebergTable table, String what) {
    return new ImpalaRuntimeException(String.format(
        "Invalid INCREMENTAL statistics request for table %s: %s", table.getFullName(),
        what));
  }

  private static Map<Integer, String> names(TIcebergPuffinStatsParams p) {
    Map<Integer, String> res = new HashMap<>();
    for (Map.Entry<String, Integer> e : p.getColumn_field_ids().entrySet()) {
      res.put(e.getValue(), e.getKey());
    }
    return res;
  }

  private static Map<Integer, IcebergColumn> cachedColumns(IcebergTable table) {
    Map<Integer, IcebergColumn> res = new HashMap<>();
    for (Column c : table.getColumns()) {
      if (c instanceof IcebergColumn) {
        res.put(((IcebergColumn) c).getFieldId(), (IcebergColumn) c);
      }
    }
    return res;
  }

  private static boolean allEqual(Map<Integer, Long> ids, Set<Integer> fieldIds,
      long snapshotId) {
    for (int f : fieldIds) {
      Long id = ids.get(f);
      if (id == null || id != snapshotId) return false;
    }
    return true;
  }

  /** Every NDV base is a theta blob of its field in a file registered at its path. */
  private static boolean basesRegistered(org.apache.iceberg.Table fresh,
      List<TIcebergNdvBase> bases) {
    for (TIcebergNdvBase base : bases) {
      boolean found = false;
      for (StatisticsFile file : fresh.statisticsFiles()) {
        if (!file.path().equals(base.getStats_file_path())) continue;
        for (BlobMetadata blob : file.blobMetadata()) {
          if (isThetaBlob(blob, base.getField_id(), base.getSnapshot_id())) {
            found = true;
            break;
          }
        }
        if (found) break;
      }
      if (!found) return false;
    }
    return true;
  }

  /**
   * Why the HMS statistics of column 'f' cannot be the metrics base 'bh', or null if
   * they can. The NDV is checked only where 'bh' has an Impala sketch of the column:
   * HMS then got its NDV from that sketch.
   */
  @Nullable
  private static String checkBase(org.apache.iceberg.Table fresh,
      @Nullable IcebergColumn col, String name, int f, long bh, HmsColumnStats hmsStats)
      throws ImpalaException {
    if (col == null) return "no cached column";
    if (col.getStats().getNumNulls() < 0) return "num_nulls < 0";
    ColumnStatisticsObj obj = hmsStats.get().get(name);
    if (obj == null) return "no HMS statistics";
    if (!ColumnStats.supportsNdv(col.getType())) return null;
    Long blobNdv =
        IcebergPuffinStatsWriter.impalaNdv(fresh.statisticsFiles(), f, bh);
    if (blobNdv == null) return null;
    Long hmsNdv = hmsNdv(obj, col.getType());
    if (hmsNdv != null && hmsNdv.longValue() == blobNdv) return null;
    return String.format("HMS NDV %s differs from ndv %d of the Impala sketch", hmsNdv,
        blobNdv);
  }

  /**
   * Whether the same request was already written: every theta column has an Impala
   * sketch at 's1' whose ndv HMS holds, where HMS keeps an NDV for the type.
   */
  private static boolean appliedAtS1(org.apache.iceberg.Table fresh,
      TIcebergPuffinStatsParams p, Map<Integer, IcebergColumn> columns,
      Map<Integer, String> names, HmsColumnStats hmsStats) throws ImpalaException {
    long s1 = p.getCalc_snapshot_id();
    List<Integer> theta = p.isSetTheta_field_ids() ? p.getTheta_field_ids()
        : Collections.emptyList();
    for (int t : theta) {
      if (!hasImpalaBlob(fresh, t, s1)) return false;
    }
    for (int t : theta) {
      IcebergColumn col = columns.get(t);
      if (col == null) return false;
      if (!ColumnStats.supportsNdv(col.getType())) continue;
      ColumnStatisticsObj obj = hmsStats.get().get(names.get(t));
      Long blobNdv =
          IcebergPuffinStatsWriter.impalaNdv(fresh.statisticsFiles(), t, s1);
      Long hmsNdv = obj == null ? null : hmsNdv(obj, col.getType());
      if (blobNdv == null || hmsNdv == null || hmsNdv.longValue() != blobNdv) {
        return false;
      }
    }
    return true;
  }

  /**
   * NDV of HMS statistics. HMS 4 returns TIMESTAMP statistics in their own field, which
   * the shim turns into the long statistics Impala writes.
   */
  @Nullable
  private static Long hmsNdv(ColumnStatisticsObj obj, Type type) {
    ColumnStatisticsData data = obj.getStatsData();
    if (data == null) return null;
    if (type.getPrimitiveType() == PrimitiveType.TIMESTAMP) {
      LongColumnStatsData ts = MetastoreShim.getCompatibleTimestampStats(data);
      return ts == null ? null : ts.getNumDVs();
    }
    if (data.isSetLongStats()) return data.getLongStats().getNumDVs();
    if (data.isSetDoubleStats()) return data.getDoubleStats().getNumDVs();
    if (data.isSetStringStats()) return data.getStringStats().getNumDVs();
    if (data.isSetDecimalStats()) return data.getDecimalStats().getNumDVs();
    if (data.isSetDateStats()) return data.getDateStats().getNumDVs();
    return null;
  }

  private static boolean hasImpalaBlob(org.apache.iceberg.Table fresh, int fieldId,
      long snapshotId) {
    for (StatisticsFile file : fresh.statisticsFiles()) {
      for (BlobMetadata blob : file.blobMetadata()) {
        if (isThetaBlob(blob, fieldId, snapshotId)
            && IcebergPuffinStatsWriter.isImpalaBlob(blob)) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean isThetaBlob(BlobMetadata blob, int fieldId, long snapshotId) {
    return StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1.equals(blob.type())
        && blob.fields() != null && blob.fields().size() == 1
        && blob.fields().get(0) == fieldId && blob.sourceSnapshotId() == snapshotId;
  }

  /**
   * Base sketch bytes of every theta column, reading each statistics file once; fills
   * 'bases' with the NDV base of each column.
   */
  private static Map<Integer, byte[]> readBaseSketches(org.apache.iceberg.Table fresh,
      TIcebergPuffinStatsParams p, Map<Integer, String> names,
      Map<Integer, TIcebergNdvBase> bases) throws ImpalaException {
    Map<String, Map<Integer, Long>> byPath = new LinkedHashMap<>();
    for (TIcebergNdvBase base : p.getNdv_bases()) {
      bases.put(base.getField_id(), base);
      byPath.computeIfAbsent(base.getStats_file_path(), k -> new HashMap<>())
          .put(base.getField_id(), base.getSnapshot_id());
    }
    Map<Integer, byte[]> res = new HashMap<>();
    for (Map.Entry<String, Map<Integer, Long>> e : byPath.entrySet()) {
      String path = e.getKey();
      Map<Integer, ByteBuffer> blobs =
          IcebergPuffinBlobReader.readThetaBlobs(fresh.io(), path, e.getValue());
      for (Map.Entry<Integer, Long> req : e.getValue().entrySet()) {
        ByteBuffer blob = blobs.get(req.getKey());
        if (blob == null) {
          throw new ImpalaRuntimeException(String.format("Puffin statistics file %s has "
              + "no theta blob for column %s (snapshot %d); run COMPUTE STATS.", path,
              names.get(req.getKey()), req.getValue()));
        }
        res.put(req.getKey(), IcebergStatsUpdater.bytes(blob));
      }
    }
    return res;
  }

  /** Row count of snapshot 'snapshotId'; null if unknown. */
  @Nullable
  private static Long totalRecords(org.apache.iceberg.Table fresh, long snapshotId) {
    Snapshot snapshot = fresh.snapshot(snapshotId);
    if (snapshot == null || snapshot.summary() == null) return null;
    String value = snapshot.summary().get(SnapshotSummary.TOTAL_RECORDS_PROP);
    if (value == null) return null;
    try {
      return Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
