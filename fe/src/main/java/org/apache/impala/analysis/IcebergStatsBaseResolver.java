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

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.impala.analysis.IcebergIncrementalBaseRules.Decision;
import org.apache.impala.analysis.IcebergStatsPlan.MetricColumn;
import org.apache.impala.analysis.IcebergStatsPlan.Mode;
import org.apache.impala.analysis.IcebergStatsPlan.Reason;
import org.apache.impala.analysis.IcebergStatsPlan.ReasonKind;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.IcebergColumn;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.util.IcebergSchemaConverter;
import org.apache.impala.util.IcebergUtil.ComputeStatsSnapshotPropertyConverter;

/**
 * Chooses how COMPUTE [INCREMENTAL] STATS computes statistics of an Iceberg table and
 * builds its IcebergStatsPlan. Works on the table snapshot in the impalad cache and reads
 * only Iceberg metadata, never data.
 */
public final class IcebergStatsBaseResolver {
  public static final String PROFILE_MODE = "Iceberg stats mode";
  public static final String PROFILE_SNAPSHOT = "Iceberg stats snapshot";

  /** Argument types of ds_theta_sketch_iceberg() that Iceberg columns can have. */
  static final Set<PrimitiveType> THETA_TYPES = EnumSet.of(
      PrimitiveType.BOOLEAN, PrimitiveType.INT, PrimitiveType.BIGINT,
      PrimitiveType.FLOAT, PrimitiveType.DOUBLE, PrimitiveType.STRING,
      PrimitiveType.DATE, PrimitiveType.TIMESTAMP, PrimitiveType.DECIMAL);

  private IcebergStatsBaseResolver() {}

  /**
   * Plan for 'metricCols' of 't' read at the current snapshot of 't'. 'sampled' means
   * that TABLESAMPLE leaves out part of the data.
   */
  public static IcebergStatsPlan resolve(FeIcebergTable t, List<Column> metricCols,
      boolean incremental, boolean sampled) throws AnalysisException {
    long s1 = t.snapshotId();
    List<MetricColumn> metricColumns = metricColumns(t, metricCols, s1);
    boolean puffinDisabled = s1 != -1 && !isPuffinWriteEnabled(t);
    // Sketches of sampled data do not describe the table, and NDV bases need sketches.
    if (s1 == -1 || puffinDisabled || sampled) {
      if (!incremental || !puffinDisabled) {
        return withoutBases(Mode.LEGACY, s1, metricColumns, Collections.emptyList());
      }
      Reason reason = new Reason(ReasonKind.PUFFIN_DISABLED, Collections.emptyList());
      return new IcebergStatsPlan(Mode.LEGACY, s1, metricColumns, Collections.emptyList(),
          false, null, null, Collections.emptyMap(), reason,
          Collections.singletonList(
              IcebergIncrementalBaseRules.fullStatsWarning(t, reason)),
          profileInfo(Mode.LEGACY, s1, Collections.singletonMap(
              IcebergIncrementalBaseRules.PROFILE_REASON, reason.kind().name())),
          Collections.emptyMap());
    }

    List<Integer> thetaFieldIds = new ArrayList<>();
    for (MetricColumn c : metricColumns) {
      if (THETA_TYPES.contains(c.col().getType().getPrimitiveType())) {
        thetaFieldIds.add(c.fieldId());
      }
    }
    if (!incremental) return withoutBases(Mode.FULL, s1, metricColumns, thetaFieldIds);
    Decision d = IcebergIncrementalBaseRules.decide(t, metricColumns, thetaFieldIds, s1);
    Map<String, String> profileInfo = profileInfo(d.mode, s1, d.profileInfo);
    if (d.deletesOnly) {
      // Without columns only the row count changes; the bases stay for the next run.
      return new IcebergStatsPlan(Mode.FULL, s1, Collections.emptyList(),
          Collections.emptyList(), true, null, null, Collections.emptyMap(), null,
          d.warnings, profileInfo, d.profileCounters);
    }
    // NOOP keeps its bases only in the profile.
    boolean withBases = d.mode == Mode.INCREMENTAL;
    return new IcebergStatsPlan(d.mode, s1, metricColumns, thetaFieldIds, false,
        withBases ? d.bh : null, withBases ? d.bn : null,
        withBases ? d.ndvBases : Collections.emptyMap(), d.reason, d.warnings,
        profileInfo, d.profileCounters);
  }

  private static IcebergStatsPlan withoutBases(Mode mode, long s1,
      List<MetricColumn> metricColumns, List<Integer> thetaFieldIds) {
    return new IcebergStatsPlan(mode, s1, metricColumns, thetaFieldIds, false, null,
        null, Collections.emptyMap(), null, Collections.emptyList(),
        profileInfo(mode, s1, Collections.emptyMap()), Collections.emptyMap());
  }

  private static Map<String, String> profileInfo(Mode mode, long s1,
      Map<String, String> more) {
    Map<String, String> res = new LinkedHashMap<>();
    res.put(PROFILE_MODE, mode.name());
    res.put(PROFILE_SNAPSHOT, Long.toString(s1));
    res.putAll(more);
    return res;
  }

  /**
   * Whether "Incremental stats" of SHOW TABLE STATS is true: Puffin writing is enabled,
   * the table records the snapshots its column statistics were computed at and every
   * column has a base for COMPUTE INCREMENTAL STATS.
   */
  public static boolean hasIncrementalBase(FeIcebergTable t) {
    if (!isPuffinWriteEnabled(t)) return false;
    // An unparsable value gives an empty map; the converter logs it.
    return !ComputeStatsSnapshotPropertyConverter.stringToMap(
        tableParam(t, IcebergTable.COMPUTE_STATS_SNAPSHOT_IDS)).isEmpty()
        && IcebergIncrementalBaseRules.hasBase(t);
  }

  static boolean isPuffinWriteEnabled(FeIcebergTable t) {
    return BackendConfig.INSTANCE.isWritingPuffinStatsEnabled()
        && !"false".equalsIgnoreCase(
            tableParam(t, IcebergTable.ICEBERG_WRITE_PUFFIN_STATS));
  }

  static String tableParam(FeIcebergTable t, String key) {
    if (t.getMetaStoreTable() == null) return null;
    Map<String, String> params = t.getMetaStoreTable().getParameters();
    return params == null ? null : params.get(key);
  }

  private static List<MetricColumn> metricColumns(FeIcebergTable t,
      List<Column> metricCols, long s1) throws AnalysisException {
    Schema s1Schema = s1 == -1 ? null : schemaOfSnapshot(t, s1);
    List<MetricColumn> res = new ArrayList<>(metricCols.size());
    for (Column col : metricCols) {
      Preconditions.checkState(col instanceof IcebergColumn,
          "column %s of Iceberg table %s has no field id", col.getName(), t.getFullName());
      int fieldId = ((IcebergColumn) col).getFieldId();
      String sourceSql;
      if (s1Schema == null) {
        sourceSql = ToSqlUtils.getIdentSql(col.getName());
      } else {
        // The Iceberg API table of the cache may be newer than its columns, so the
        // column, not the current Iceberg schema, gives the type the query reads.
        Types.NestedField s1Field = s1Schema.asStruct().field(fieldId);
        String typeSql = col.getType().toSql();
        if (s1Field == null) {
          sourceSql = "CAST(NULL AS " + typeSql + ")";
        } else {
          String name = ToSqlUtils.getIdentSql(s1Field.name().toLowerCase(Locale.ROOT));
          sourceSql = impalaType(t, s1Field).equals(col.getType()) ?
              name : "CAST(" + name + " AS " + typeSql + ")";
        }
      }
      res.add(new MetricColumn(col, fieldId, sourceSql));
    }
    return res;
  }

  private static Type impalaType(FeIcebergTable t, Types.NestedField field)
      throws AnalysisException {
    try {
      return IcebergSchemaConverter.toImpalaType(field.type());
    } catch (ImpalaRuntimeException e) {
      throw metadataError(t, e);
    }
  }

  private static AnalysisException metadataError(FeIcebergTable t, Exception e) {
    return new AnalysisException("Failed to read metadata of Iceberg table "
        + t.getFullName() + ": " + e.getMessage(), e);
  }

  /** Schema S1 was written with; the current schema if S1 does not record it. */
  private static Schema schemaOfSnapshot(FeIcebergTable t, long s1)
      throws AnalysisException {
    try {
      return SnapshotUtil.schemaFor(t.getIcebergApiTable(), s1, null);
    } catch (RuntimeIOException | NotFoundException | ValidationException
        | IllegalArgumentException e) {
      throw metadataError(t, e);
    }
  }
}
