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

package org.apache.impala.analysis;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.impala.analysis.IcebergStatsPlan.MetricColumn;
import org.apache.impala.analysis.IcebergStatsPlan.Mode;
import org.apache.impala.analysis.IcebergStatsPlan.NdvBase;
import org.apache.impala.analysis.IcebergStatsPlan.Reason;
import org.apache.impala.analysis.IcebergStatsPlan.ReasonKind;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.IcebergColumn;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.iceberg.IcebergAddedFiles;
import org.apache.impala.catalog.iceberg.IcebergAddedFiles.AddedFiles;
import org.apache.impala.catalog.iceberg.IcebergAddedFiles.Gap;
import org.apache.impala.catalog.iceberg.IcebergAddedFiles.GapException;
import org.apache.impala.catalog.iceberg.IcebergPuffinStatsWriter;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.util.IcebergSchemaConverter;
import org.apache.impala.util.IcebergUtil;
import org.apache.impala.util.IcebergUtil.ComputeStatsSnapshotPropertyConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Chooses and checks the bases of COMPUTE INCREMENTAL STATS on an Iceberg table: the
 * snapshot the HMS metrics of the columns describe and the theta sketches their NDV
 * continues from. Reads only the table metadata in the impalad cache, never data, HMS
 * or Puffin files. An unusable base gives full statistics with a reason, not an error.
 */
final class IcebergIncrementalBaseRules {
  private static final Logger LOG =
      LoggerFactory.getLogger(IcebergIncrementalBaseRules.class);

  /** Table property: the deleted-row ratio of the gap above which statistics are full. */
  static final String MAX_DELETED_RATIO =
      "impala.iceberg_incremental_stats_max_deleted_ratio";

  static final String PROFILE_REASON = "Iceberg stats reason";
  static final String PROFILE_METRICS_BASE = "Iceberg stats metrics base";
  static final String PROFILE_NDV_BASE = "Iceberg stats NDV base";
  static final String PROFILE_DELETED_RATIO = "Iceberg stats deleted ratio";
  static final String PROFILE_GAP_FILES = "IcebergStatsGapFiles";
  static final String PROFILE_GAP_BYTES = "IcebergStatsGapBytes";
  static final String PROFILE_GAP_RECORDS = "IcebergStatsGapRecords";
  static final String PROFILE_THETA_GAP_FILES = "IcebergStatsThetaGapFiles";
  static final String PROFILE_THETA_GAP_BYTES = "IcebergStatsThetaGapBytes";

  private static final String THETA_BLOB_TYPE =
      StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1;
  /** Column lists in reasons name at most this many columns. */
  private static final int MAX_LISTED_COLUMNS = 10;

  private IcebergIncrementalBaseRules() {}

  /** Immutable outcome of decide(). */
  static final class Decision {
    final Mode mode;
    /** Set only in FULL that has a reason. */
    @Nullable final Reason reason;
    /** FULL whose gap holds nothing but deletes: only the table query runs. */
    final boolean deletesOnly;
    /** Set in NOOP, INCREMENTAL and FULL with deletesOnly. */
    @Nullable final Long bh;
    @Nullable final Long bn;
    /** Keyed by the theta field ids in NOOP and INCREMENTAL; empty in FULL. */
    final Map<Integer, NdvBase> ndvBases;
    final List<String> warnings;
    /** Insertion-ordered. */
    final Map<String, String> profileInfo;
    /** Insertion-ordered. */
    final Map<String, Long> profileCounters;

    private Decision(Mode mode, @Nullable Reason reason, boolean deletesOnly,
        @Nullable Long bh, @Nullable Long bn, Map<Integer, NdvBase> ndvBases,
        List<String> warnings, Map<String, String> profileInfo,
        Map<String, Long> profileCounters) {
      this.mode = mode;
      this.reason = reason;
      this.deletesOnly = deletesOnly;
      this.bh = bh;
      this.bn = bn;
      this.ndvBases = ImmutableMap.copyOf(ndvBases);
      this.warnings = ImmutableList.copyOf(warnings);
      this.profileInfo = ImmutableMap.copyOf(profileInfo);
      this.profileCounters = ImmutableMap.copyOf(profileCounters);
    }
  }

  /** What the rules learn on the way to a decision of one table. */
  private static final class Rules {
    final FeIcebergTable t;
    final List<String> warnings = new ArrayList<>();
    /** Property value as written; null if the threshold is off. */
    @Nullable String maxRatioText;
    double maxRatio;
    /** Formatted estimate of the deleted-row ratio once computed. */
    @Nullable String deletedRatio;

    Rules(FeIcebergTable t) { this.t = t; }

    Decision full(ReasonKind kind, List<String> args) {
      Reason reason = new Reason(kind, args);
      List<String> allWarnings = new ArrayList<>(warnings);
      allWarnings.add(fullStatsWarning(t, reason));
      Map<String, String> info = new LinkedHashMap<>();
      info.put(PROFILE_REASON, kind.name());
      if (deletedRatio != null) info.put(PROFILE_DELETED_RATIO, deletedRatio);
      return new Decision(Mode.FULL, reason, false, null, null, Collections.emptyMap(),
          allWarnings, info, Collections.emptyMap());
    }

    Decision full(ReasonKind kind, long snapshotId) {
      return full(kind, ImmutableList.of(Long.toString(snapshotId)));
    }

    /**
     * NOOP, INCREMENTAL or FULL of a gap with only deletes, with the gap of the metrics
     * 'gh' and, if there are theta columns, the gap of the sketches 'gn'.
     */
    Decision onGap(Mode mode, boolean deletesOnly, long bh, @Nullable Long bn,
        Map<Integer, NdvBase> ndvBases, AddedFiles gh, @Nullable AddedFiles gn) {
      Map<String, String> info = new LinkedHashMap<>();
      info.put(PROFILE_METRICS_BASE, Long.toString(bh));
      if (bn != null) info.put(PROFILE_NDV_BASE, Long.toString(bn));
      if (deletedRatio != null) info.put(PROFILE_DELETED_RATIO, deletedRatio);
      Map<String, Long> counters = new LinkedHashMap<>();
      counters.put(PROFILE_GAP_FILES, gh.fileCount);
      counters.put(PROFILE_GAP_BYTES, gh.bytes);
      counters.put(PROFILE_GAP_RECORDS, gh.records);
      if (gn != null) {
        counters.put(PROFILE_THETA_GAP_FILES, gn.fileCount);
        counters.put(PROFILE_THETA_GAP_BYTES, gn.bytes);
      }
      return new Decision(mode, null, deletesOnly, bh, bn, ndvBases, warnings, info,
          counters);
    }

    /** Reads the threshold property; an invalid value turns it off with a warning. */
    void readMaxDeletedRatio() {
      String text = IcebergStatsBaseResolver.tableParam(t, MAX_DELETED_RATIO);
      if (text == null) return;
      double ratio;
      try {
        ratio = Double.parseDouble(text.trim());
      } catch (NumberFormatException e) {
        ratio = Double.NaN;
      }
      // NaN fails both comparisons.
      if (!(ratio >= 0 && ratio <= 1)) {
        warnings.add("Ignoring invalid value '" + text + "' of table property '"
            + MAX_DELETED_RATIO + "'.");
        return;
      }
      maxRatioText = text;
      maxRatio = ratio;
    }
  }

  /**
   * Bases of 'metricCols' for COMPUTE INCREMENTAL STATS of 't' at snapshot 's1'.
   * 'thetaFieldIds' are the field ids of the columns among 'metricCols' that get theta
   * sketches. The first rule that applies decides; column lists of reasons follow the
   * order of 'metricCols'. Iceberg failures to read metadata throw AnalysisException.
   */
  static Decision decide(FeIcebergTable t, List<MetricColumn> metricCols,
      List<Integer> thetaFieldIds, long s1) throws AnalysisException {
    // Only the table query remains.
    if (metricCols.isEmpty()) {
      return new Decision(Mode.FULL, null, false, null, null, Collections.emptyMap(),
          Collections.emptyList(), Collections.emptyMap(), Collections.emptyMap());
    }
    try {
      return decideBases(new Rules(t), metricCols, thetaFieldIds, s1);
    } catch (RuntimeIOException | NotFoundException | ValidationException e) {
      throw new AnalysisException("Failed to read Iceberg metadata of table "
          + t.getFullName() + ": " + e.getMessage(), e);
    }
  }

  private static Decision decideBases(Rules rules, List<MetricColumn> metricCols,
      List<Integer> thetaFieldIds, long s1) {
    FeIcebergTable t = rules.t;
    rules.readMaxDeletedRatio();
    Map<Integer, Long> ids = metricBaseIds(t);
    List<String> withoutBase = new ArrayList<>();
    Set<Long> metricBases = new LinkedHashSet<>();
    for (MetricColumn c : metricCols) {
      Long base = metricBase(ids, c);
      if (base == null) {
        withoutBase.add(c.col().getName());
      } else {
        metricBases.add(base);
      }
    }
    if (withoutBase.size() == metricCols.size()) {
      return rules.full(ReasonKind.NO_BASE, Collections.emptyList());
    }
    if (!withoutBase.isEmpty()) {
      return rules.full(ReasonKind.COLUMNS_WITHOUT_BASE, withoutBase);
    }
    if (metricBases.size() > 1) {
      return rules.full(ReasonKind.BASES_DIFFER, Collections.emptyList());
    }
    long bh = metricBases.iterator().next();

    Table api = t.getIcebergApiTable();
    // One read: a metadata refresh between the rules must not change the files.
    List<StatisticsFile> files = api.statisticsFiles();
    if (!isInChain(api, s1, bh)) {
      return rules.full(ReasonKind.BASE_NOT_ANCESTOR,
          ImmutableList.of(Long.toString(bh), Long.toString(s1)));
    }

    List<String> ndvMismatch = new ArrayList<>();
    for (MetricColumn c : metricCols) {
      if (hmsNdvDiffersFromSketch(api, files, c, bh)) {
        ndvMismatch.add(c.col().getName());
      }
    }
    if (!ndvMismatch.isEmpty()) {
      List<String> args = new ArrayList<>(ndvMismatch);
      args.add(Long.toString(bh));
      return rules.full(ReasonKind.BASE_NDV_MISMATCH, args);
    }

    Map<Long, Integer> chain = chainPositions(api, s1);
    Set<Integer> thetaFields = new HashSet<>(thetaFieldIds);
    Map<Integer, NdvBase> ndvBases = new LinkedHashMap<>();
    List<String> withoutNdvBase = new ArrayList<>();
    Long bn = null;
    for (MetricColumn c : metricCols) {
      if (!thetaFields.contains(c.fieldId())) continue;
      NdvBase base = ndvBase(files, c.fieldId(), chain);
      if (base == null) {
        withoutNdvBase.add(c.col().getName());
        continue;
      }
      ndvBases.put(c.fieldId(), base);
      if (bn == null || chain.get(base.blobSnapshotId()) > chain.get(bn)) {
        bn = base.blobSnapshotId();
      }
    }
    if (!withoutNdvBase.isEmpty()) {
      return rules.full(ReasonKind.COLUMNS_WITHOUT_NDV_BASE, withoutNdvBase);
    }
    Preconditions.checkState(ndvBases.keySet().equals(thetaFields),
        "theta field ids %s of table %s are not metric columns", thetaFieldIds,
        t.getFullName());
    long b = bn != null && chain.get(bn) > chain.get(bh) ? bn : bh;

    AddedFiles gh;
    AddedFiles gn;
    boolean hasDeletes;
    try {
      // Read once: sub-gaps of this read give the same files as the child scans.
      Gap gap = IcebergAddedFiles.read(api, b, s1);
      AddedFiles all = gap.after(b);
      if (rules.maxRatioText != null) {
        Decision tooManyDeletes = checkDeletedRatio(rules, api, b, s1);
        if (tooManyDeletes != null) return tooManyDeletes;
      }
      // Metrics of the base count rows of files the replace removed, while the gap
      // adds their rewritten rows again.
      if (all.rewrittenByReplace) {
        return rules.full(ReasonKind.REPLACE_REWROTE_NEW_FILES, b);
      }
      hasDeletes = all.hasDeletes;
      gh = gap.after(bh);
      gn = bn == null ? null : gap.after(bn);
    } catch (GapException e) {
      return gapFailure(rules, e, s1);
    }
    // Statistics of an unchanged set of files stay valid whatever the types are now.
    if (gh.paths.isEmpty() && (gn == null || gn.paths.isEmpty())) {
      if (hasDeletes || !rowCountMatches(t, api, s1)) {
        return rules.onGap(Mode.FULL, true, bh, bn, Collections.emptyMap(), gh, gn);
      }
      return rules.onGap(Mode.NOOP, false, bh, bn, ndvBases, gh, gn);
    }

    Long withoutSchema = snapshotWithoutSchema(api, bh, ndvBases);
    if (withoutSchema != null) {
      return rules.full(ReasonKind.SNAPSHOT_WITHOUT_SCHEMA, withoutSchema);
    }
    List<String> typeChanged = typeChangedColumns(api, metricCols, bh, ndvBases);
    if (!typeChanged.isEmpty()) {
      return rules.full(ReasonKind.COLUMNS_TYPE_CHANGED, typeChanged);
    }
    return rules.onGap(Mode.INCREMENTAL, false, bh, bn, ndvBases, gh, gn);
  }

  /**
   * Whether the row count in HMS is that of 's1' when no files were added since the
   * bases. The HiveCatalog sets it on every commit; otherwise 's1' must have become
   * current before the last COMPUTE STATS, whose time has only seconds: a rollback to
   * the base after statistics of a later snapshot leaves their row count. A missing or
   * invalid time counts as not before.
   */
  private static boolean rowCountMatches(FeIcebergTable t, Table api, long s1) {
    org.apache.hadoop.hive.metastore.api.Table msTbl = t.getMetaStoreTable();
    if (msTbl != null && IcebergUtil.isHmsIntegrated(msTbl)) return true;
    String text = IcebergStatsBaseResolver.tableParam(
        t, org.apache.impala.catalog.Table.TBL_PROP_LAST_COMPUTE_STATS_TIME);
    if (text == null) return false;
    long computedSeconds;
    try {
      computedSeconds = Long.parseLong(text.trim());
    } catch (NumberFormatException e) {
      return false;
    }
    Long currentSince = null;
    for (HistoryEntry e : api.history()) {
      if (e.snapshotId() == s1) currentSince = e.timestampMillis();
    }
    return currentSince != null
        && Math.floorDiv(currentSince, 1000L) < computedSeconds;
  }

  /**
   * FULL with DELETED_RATIO if the rows the gap (b, s1] deleted exceed the threshold
   * share of the rows of 'b', FULL with NO_TOTAL_RECORDS if 'b' does not tell its rows;
   * null otherwise.
   */
  @Nullable
  private static Decision checkDeletedRatio(Rules rules, Table api, long b, long s1)
      throws GapException {
    Map<String, String> summary = api.snapshot(b).summary();
    String total =
        summary == null ? null : summary.get(SnapshotSummary.TOTAL_RECORDS_PROP);
    long totalRecords;
    try {
      totalRecords = total == null ? 0 : Long.parseLong(total.trim());
    } catch (NumberFormatException e) {
      totalRecords = 0;
    }
    if (totalRecords <= 0) return rules.full(ReasonKind.NO_TOTAL_RECORDS, b);
    double ratio =
        (double) IcebergAddedFiles.estimateDeletedRows(api, b, s1) / totalRecords;
    rules.deletedRatio = String.format(Locale.ROOT, "%.4f", ratio);
    if (ratio <= rules.maxRatio) return null;
    return rules.full(ReasonKind.DELETED_RATIO,
        ImmutableList.of(Double.toString(ratio), rules.maxRatioText));
  }

  /** FULL with the reason of a gap that cannot be read. */
  private static Decision gapFailure(Rules rules, GapException e, long s1) {
    switch (e.kind()) {
      case UNSUPPORTED_SNAPSHOT:
        return rules.full(ReasonKind.UNSUPPORTED_SNAPSHOT, e.snapshotId());
      case NOT_ANCESTOR:
        return rules.full(ReasonKind.BASE_NOT_ANCESTOR,
            ImmutableList.of(Long.toString(e.snapshotId()), Long.toString(s1)));
      default:
        throw new IllegalStateException("unknown gap failure " + e.kind());
    }
  }

  /**
   * Whether COMPUTE INCREMENTAL STATS of every column of 't' at its current snapshot
   * would continue from bases instead of computing full statistics. Reads only the
   * metadata in memory, no manifests; false if Iceberg fails to read it.
   */
  static boolean hasBase(FeIcebergTable t) {
    long s = t.snapshotId();
    if (s == -1) return false;
    List<MetricColumn> cols = new ArrayList<>();
    List<Column> all = t.getColumns();
    for (Column c : all.subList(t.getNumClusteringCols(), all.size())) {
      if (ComputeStatsStmt.ignoreColumn(c)) continue;
      Preconditions.checkState(c instanceof IcebergColumn,
          "column %s of Iceberg table %s has no field id", c.getName(), t.getFullName());
      cols.add(new MetricColumn(c, ((IcebergColumn) c).getFieldId(),
          ToSqlUtils.getIdentSql(c.getName())));
    }
    if (cols.isEmpty()) return false;
    try {
      return hasBase(t, cols, s);
    } catch (RuntimeIOException | NotFoundException | ValidationException e) {
      LOG.warn("Failed to read Iceberg metadata of table " + t.getFullName(), e);
      return false;
    }
  }

  private static boolean hasBase(FeIcebergTable t, List<MetricColumn> cols, long s) {
    Map<Integer, Long> ids = metricBaseIds(t);
    Long bh = null;
    for (MetricColumn c : cols) {
      Long base = metricBase(ids, c);
      if (base == null || (bh != null && !bh.equals(base))) return false;
      bh = base;
    }
    Table api = t.getIcebergApiTable();
    if (!isInChain(api, s, bh)) return false;
    Map<Long, Integer> chain = chainPositions(api, s);
    List<StatisticsFile> files = api.statisticsFiles();
    Map<Integer, NdvBase> ndvBases = new LinkedHashMap<>();
    boolean emptyGap = bh == s;
    for (MetricColumn c : cols) {
      if (!IcebergStatsBaseResolver.THETA_TYPES.contains(
          c.col().getType().getPrimitiveType())) {
        continue;
      }
      NdvBase base = ndvBase(files, c.fieldId(), chain);
      if (base == null) return false;
      ndvBases.put(c.fieldId(), base);
      emptyGap &= base.blobSnapshotId() == s;
    }
    if (emptyGap) return true;
    return snapshotWithoutSchema(api, bh, ndvBases) == null
        && typeChangedColumns(api, cols, bh, ndvBases).isEmpty();
  }

  private static Map<Integer, Long> metricBaseIds(FeIcebergTable t) {
    return ComputeStatsSnapshotPropertyConverter.stringToMap(
        IcebergStatsBaseResolver.tableParam(t, IcebergTable.COMPUTE_STATS_SNAPSHOT_IDS));
  }

  /**
   * Snapshot the HMS metrics of 'c' describe; null if there is none. Without HMS
   * statistics there is nothing to merge with, whatever the property says.
   */
  @Nullable
  private static Long metricBase(Map<Integer, Long> ids, MetricColumn c) {
    Long base = ids.get(c.fieldId());
    if (base == null || base == -1 || c.col().getStats().getNumNulls() < 0) return null;
    return base;
  }

  /** An expired ancestor breaks the chain, so an expired base is not in it. */
  private static boolean isInChain(Table api, long s1, long base) {
    return api.snapshot(s1) != null
        && (base == s1 || SnapshotUtil.isAncestorOf(api, s1, base));
  }

  /**
   * The first of 'bh' and the NDV bases, in this order, whose schema is not known;
   * null if all are known.
   */
  @Nullable
  private static Long snapshotWithoutSchema(Table api, long bh,
      Map<Integer, NdvBase> ndvBases) {
    if (schemaOf(api, bh) == null) return bh;
    for (NdvBase base : ndvBases.values()) {
      if (schemaOf(api, base.blobSnapshotId()) == null) return base.blobSnapshotId();
    }
    return null;
  }

  /**
   * Columns of 'cols' whose field is missing or has another type in the schema of 'bh'
   * or of their NDV base. All these schemas must be known.
   */
  private static List<String> typeChangedColumns(Table api, List<MetricColumn> cols,
      long bh, Map<Integer, NdvBase> ndvBases) {
    Schema bhSchema = schemaOf(api, bh);
    List<String> res = new ArrayList<>();
    for (MetricColumn c : cols) {
      NdvBase ndvBase = ndvBases.get(c.fieldId());
      if (typeDiffers(bhSchema, c) || (ndvBase != null
          && typeDiffers(schemaOf(api, ndvBase.blobSnapshotId()), c))) {
        res.add(c.col().getName());
      }
    }
    return res;
  }

  /**
   * Compares with the column rather than the current Iceberg schema: the Iceberg table
   * of the cache may be newer than its columns, and the child queries read the type of
   * the column.
   */
  private static boolean typeDiffers(Schema schema, MetricColumn c) {
    Types.NestedField field = schema.findField(c.fieldId());
    if (field == null) return true;
    try {
      return !IcebergSchemaConverter.toImpalaType(field.type()).equals(c.col().getType());
    } catch (ImpalaRuntimeException e) {
      // A type Impala cannot read is not the type of the column.
      return true;
    }
  }

  /** Schema of snapshot 'snapshotId'; null unless it records a known one. */
  @Nullable
  private static Schema schemaOf(Table api, long snapshotId) {
    Integer schemaId = api.snapshot(snapshotId).schemaId();
    return schemaId == null ? null : api.schemas().get(schemaId);
  }

  /**
   * Whether the HMS NDV of 'c' cannot come from the Impala sketch of base 'bh': it
   * differs from the "ndv" of that blob and of every theta blob of the field that table
   * loading may have put in its place, that is one of a live snapshot not older than
   * 'bh'. False when 'bh' has no Impala blob of the field or the type has no HMS NDV.
   */
  private static boolean hmsNdvDiffersFromSketch(Table api,
      List<StatisticsFile> files, MetricColumn c, long bh) {
    if (!ColumnStats.supportsNdv(c.col().getType())) return false;
    int fieldId = c.fieldId();
    Long impalaNdv = IcebergPuffinStatsWriter.impalaNdv(files, fieldId, bh);
    if (impalaNdv == null) return false;
    long hmsNdv = c.col().getStats().getNumDistinctValues();
    if (hmsNdv == impalaNdv) return false;
    long bhMillis = api.snapshot(bh).timestampMillis();
    for (BlobMetadata blob : thetaBlobs(files, fieldId)) {
      Snapshot source = api.snapshot(blob.sourceSnapshotId());
      if (source == null || source.timestampMillis() < bhMillis) continue;
      Long ndv = IcebergPuffinStatsWriter.ndvOf(blob);
      if (ndv != null && ndv == hmsNdv) return false;
    }
    return true;
  }

  /**
   * NDV base of field 'fieldId': the theta blob of any author whose source snapshot is
   * the newest in 'chain'; of several files with such a blob, the first one. Null if
   * there is none or that file holds more than one such blob, as reading the sketch
   * rejects such a file.
   */
  @Nullable
  private static NdvBase ndvBase(List<StatisticsFile> files, int fieldId,
      Map<Long, Integer> chain) {
    Long newest = null;
    for (BlobMetadata blob : thetaBlobs(files, fieldId)) {
      Integer pos = chain.get(blob.sourceSnapshotId());
      if (pos != null && (newest == null || pos < chain.get(newest))) {
        newest = blob.sourceSnapshotId();
      }
    }
    if (newest == null) return null;
    for (StatisticsFile file : files) {
      int count = 0;
      for (BlobMetadata blob : file.blobMetadata()) {
        if (isThetaBlobOf(blob, fieldId) && blob.sourceSnapshotId() == newest) ++count;
      }
      if (count == 1) return new NdvBase(newest, file.path());
      if (count > 1) return null;
    }
    throw new IllegalStateException("blob of snapshot " + newest + " vanished");
  }

  /** Position of each snapshot in the chain of 's1': 0 for 's1', older is greater. */
  private static Map<Long, Integer> chainPositions(Table api, long s1) {
    Map<Long, Integer> res = new HashMap<>();
    for (Snapshot s : SnapshotUtil.ancestorsOf(s1, api::snapshot)) {
      res.put(s.snapshotId(), res.size());
    }
    return res;
  }

  private static List<BlobMetadata> thetaBlobs(List<StatisticsFile> files, int fieldId) {
    List<BlobMetadata> res = new ArrayList<>();
    for (StatisticsFile file : files) {
      for (BlobMetadata blob : file.blobMetadata()) {
        if (isThetaBlobOf(blob, fieldId)) res.add(blob);
      }
    }
    return res;
  }

  private static boolean isThetaBlobOf(BlobMetadata blob, int fieldId) {
    return THETA_BLOB_TYPE.equals(blob.type()) && blob.fields() != null
        && blob.fields().size() == 1 && blob.fields().get(0) == fieldId;
  }

  /**
   * Warning that COMPUTE INCREMENTAL STATS on 't' computes full statistics because of
   * 'reason'. Arguments of the reason are, in order: the column names for kinds that
   * list columns, followed by the snapshot ids or values the text names.
   */
  static String fullStatsWarning(FeIcebergTable t, Reason reason) {
    return "COMPUTE INCREMENTAL STATS on Iceberg table " + t.getFullName()
        + " computes full statistics: " + reasonText(reason) + ".";
  }

  private static String reasonText(Reason reason) {
    List<String> args = reason.args();
    switch (reason.kind()) {
      case NO_BASE:
        return "no statistics computed by Impala to use as a base";
      case COLUMNS_WITHOUT_BASE:
        return "no base statistics for column(s) " + columns(args);
      case BASES_DIFFER:
        return "columns have base statistics from different snapshots";
      case BASE_NOT_ANCESTOR:
        checkArgCount(reason, 2);
        return "base snapshot " + args.get(0) + " is not an ancestor of snapshot "
            + args.get(1);
      case BASE_NDV_MISMATCH:
        Preconditions.checkArgument(args.size() >= 2, "%s needs columns and a snapshot",
            reason.kind());
        return "NDV of column(s) " + columns(args.subList(0, args.size() - 1))
            + " in the Hive Metastore differs from the theta sketch of snapshot "
            + args.get(args.size() - 1);
      case COLUMNS_WITHOUT_NDV_BASE:
        return "no theta sketch for column(s) " + columns(args);
      case UNSUPPORTED_SNAPSHOT:
        checkArgCount(reason, 1);
        return "snapshot " + args.get(0) + " has no manifest list or operation";
      case NO_TOTAL_RECORDS:
        checkArgCount(reason, 1);
        return "snapshot " + args.get(0) + " has no usable 'total-records' to evaluate '"
            + MAX_DELETED_RATIO + "'";
      case DELETED_RATIO:
        checkArgCount(reason, 2);
        // The root locale keeps the decimal point whatever the locale of the server.
        return "estimated ratio of deleted rows "
            + String.format(Locale.ROOT, "%.4f", Double.parseDouble(args.get(0)))
            + " exceeds '" + MAX_DELETED_RATIO
            + "'=" + args.get(1);
      case REPLACE_REWROTE_NEW_FILES:
        checkArgCount(reason, 1);
        return "data files added after snapshot " + args.get(0)
            + " were rewritten by a 'replace' snapshot";
      case SNAPSHOT_WITHOUT_SCHEMA:
        checkArgCount(reason, 1);
        return "snapshot " + args.get(0) + " has no schema id";
      case COLUMNS_TYPE_CHANGED:
        return "type of column(s) " + columns(args) + " changed since their base"
            + " statistics";
      case PUFFIN_DISABLED:
        return "writing Puffin statistics is disabled ('enable_writing_puffin_stats' or '"
            + IcebergTable.ICEBERG_WRITE_PUFFIN_STATS + "')";
      default:
        throw new IllegalStateException("unknown reason " + reason.kind());
    }
  }

  private static void checkArgCount(Reason reason, int count) {
    Preconditions.checkArgument(reason.args().size() == count,
        "%s needs %s arguments: %s", reason.kind(), count, reason.args());
  }

  private static String columns(List<String> names) {
    Preconditions.checkArgument(!names.isEmpty(), "no columns");
    if (names.size() <= MAX_LISTED_COLUMNS) return String.join(", ", names);
    return String.join(", ", names.subList(0, MAX_LISTED_COLUMNS)) + ", …";
  }
}
