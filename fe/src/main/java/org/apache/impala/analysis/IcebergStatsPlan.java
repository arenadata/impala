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

import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.impala.catalog.Column;

/**
 * Immutable plan of COMPUTE [INCREMENTAL] STATS on an Iceberg table: the snapshot S1 all
 * child queries read, the columns they compute metrics for and the NDV bases. Built by
 * IcebergStatsBaseResolver during analysis of the parent statement.
 */
public final class IcebergStatsPlan {
  public enum Mode { LEGACY, FULL, INCREMENTAL, NOOP }

  /**
   * Why COMPUTE INCREMENTAL STATS falls back to full statistics. The names are values of
   * the "Iceberg stats reason" profile string, so they must not be renamed.
   */
  public enum ReasonKind {
    NO_BASE,
    COLUMNS_WITHOUT_BASE,
    BASES_DIFFER,
    BASE_NOT_ANCESTOR,
    BASE_NDV_MISMATCH,
    COLUMNS_WITHOUT_NDV_BASE,
    UNSUPPORTED_SNAPSHOT,
    NO_TOTAL_RECORDS,
    DELETED_RATIO,
    REPLACE_REWROTE_NEW_FILES,
    SNAPSHOT_WITHOUT_SCHEMA,
    COLUMNS_TYPE_CHANGED,
    PUFFIN_DISABLED
  }

  /** Column of the metrics query. */
  public static final class MetricColumn {
    private final Column col_;
    private final int fieldId_;
    private final String sourceSql_;

    public MetricColumn(Column col, int fieldId, String sourceSql) {
      col_ = Preconditions.checkNotNull(col);
      fieldId_ = fieldId;
      sourceSql_ = Preconditions.checkNotNull(sourceSql);
    }

    /** Column of the current schema; its name is the alias of the expressions. */
    public Column col() { return col_; }

    /** Field id in the current schema. */
    public int fieldId() { return fieldId_; }

    /**
     * Expression that reads the column from S1, which is resolved by the schema of S1:
     * the name of the field there, cast to the current type if that differs, or a typed
     * NULL if S1 has no such field.
     */
    public String sourceSql() { return sourceSql_; }
  }

  /** Puffin blob that holds the NDV base of a column. */
  public static final class NdvBase {
    private final long blobSnapshotId_;
    private final String statsFilePath_;

    public NdvBase(long blobSnapshotId, String statsFilePath) {
      blobSnapshotId_ = blobSnapshotId;
      statsFilePath_ = Preconditions.checkNotNull(statsFilePath);
    }

    public long blobSnapshotId() { return blobSnapshotId_; }

    public String statsFilePath() { return statsFilePath_; }
  }

  public static final class Reason {
    private final ReasonKind kind_;
    private final List<String> args_;

    public Reason(ReasonKind kind, List<String> args) {
      kind_ = Preconditions.checkNotNull(kind);
      args_ = ImmutableList.copyOf(args);
    }

    public ReasonKind kind() { return kind_; }

    public List<String> args() { return args_; }
  }

  private final Mode mode_;
  private final long s1_;
  private final List<MetricColumn> metricColumns_;
  private final List<Integer> thetaFieldIds_;
  private final boolean deletesOnly_;
  @Nullable private final Long bh_;
  @Nullable private final Long bn_;
  private final Map<Integer, NdvBase> ndvBases_;
  @Nullable private final Reason reason_;
  private final List<String> warnings_;
  private final Map<String, String> profileInfo_;
  private final Map<String, Long> profileCounters_;

  IcebergStatsPlan(Mode mode, long s1, List<MetricColumn> metricColumns,
      List<Integer> thetaFieldIds, boolean deletesOnly, @Nullable Long bh,
      @Nullable Long bn, Map<Integer, NdvBase> ndvBases, @Nullable Reason reason,
      List<String> warnings, Map<String, String> profileInfo,
      Map<String, Long> profileCounters) {
    mode_ = Preconditions.checkNotNull(mode);
    s1_ = s1;
    metricColumns_ = ImmutableList.copyOf(metricColumns);
    thetaFieldIds_ = ImmutableList.copyOf(thetaFieldIds);
    deletesOnly_ = deletesOnly;
    bh_ = bh;
    bn_ = bn;
    ndvBases_ = ImmutableMap.copyOf(ndvBases);
    reason_ = reason;
    warnings_ = ImmutableList.copyOf(warnings);
    profileInfo_ = ImmutableMap.copyOf(profileInfo);
    profileCounters_ = ImmutableMap.copyOf(profileCounters);
  }

  public Mode mode() { return mode_; }

  /** Snapshot all child queries read; -1 if the table has no snapshots. */
  public long s1() { return s1_; }

  /** In the order of the metrics query. */
  public List<MetricColumn> metricColumns() { return metricColumns_; }

  /** Field ids of the theta sketch columns, in the order of the theta expressions. */
  public List<Integer> thetaFieldIds() { return thetaFieldIds_; }

  /**
   * FULL of COMPUTE INCREMENTAL STATS whose gap from the bases holds only deletes: it has
   * no columns, so only the row count is computed and the column statistics are kept.
   */
  public boolean deletesOnly() { return deletesOnly_; }

  /** Base snapshot of the HMS metrics; set only in INCREMENTAL. */
  @Nullable public Long bh() { return bh_; }

  /** Base snapshot of the NDV sketches; set only in INCREMENTAL. */
  @Nullable public Long bn() { return bn_; }

  /** Keyed by field id; set only in INCREMENTAL. */
  public Map<Integer, NdvBase> ndvBases() { return ndvBases_; }

  @Nullable public Reason reason() { return reason_; }

  /** Texts for Analyzer#addWarning. */
  public List<String> warnings() { return warnings_; }

  /** Info strings of the parent query profile, in insertion order. */
  public Map<String, String> profileInfo() { return profileInfo_; }

  /** Unit counters of the parent query profile, in insertion order. */
  public Map<String, Long> profileCounters() { return profileCounters_; }
}
