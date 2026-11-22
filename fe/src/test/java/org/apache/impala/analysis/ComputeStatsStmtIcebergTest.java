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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.iceberg.IcebergTestTables;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.FrontendProfile;
import org.apache.impala.thrift.TComputeStatsParams;
import org.apache.impala.thrift.TIcebergPuffinStatsParams;
import org.apache.impala.thrift.TIcebergStatsMode;
import org.apache.impala.thrift.TQueryOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

/**
 * Child query texts and TComputeStatsParams that COMPUTE [INCREMENTAL] STATS builds for
 * Iceberg tables in the impalad cache.
 */
public class ComputeStatsStmtIcebergTest extends FrontendTestBase {
  private static final String DB = "iceberg_compute_stats";
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "date", Types.DateType.get()),
      optional(3, "UserId", Types.StringType.get()),
      optional(4, "b", Types.BinaryType.get()));
  private static final String EXTRAPOLATION =
      HdfsTable.TBL_PROP_ENABLE_STATS_EXTRAPOLATION;

  @Rule public TemporaryFolder tmp = new TemporaryFolder();
  @Rule public TestName testName = new TestName();

  private static final IcebergAnalysisFixture ifx =
      IcebergAnalysisFixture.create(feFixture_);

  private boolean savedWriteFlag_;

  @Before
  public void saveWriteFlag() {
    savedWriteFlag_ =
        BackendConfig.INSTANCE.getBackendCfg().isEnable_writing_puffin_stats();
    setWriteFlag(true);
  }

  @After
  public void restoreWriteFlag() {
    setWriteFlag(savedWriteFlag_);
  }

  private static void setWriteFlag(boolean enabled) {
    BackendConfig.INSTANCE.getBackendCfg().setEnable_writing_puffin_stats(enabled);
  }

  private String tableName() {
    return testName.getMethodName().toLowerCase();
  }

  private String fullName() {
    return DB + "." + tableName();
  }

  private IcebergTestTables create(Schema schema) {
    return IcebergTestTables.create(tmp, DB, tableName(), schema,
        PartitionSpec.unpartitioned(), 2);
  }

  /** Table of SCHEMA with 'numFiles' data files of 100 bytes, one snapshot each. */
  private IcebergTable table(int numFiles, Map<String, String> params)
      throws Exception {
    IcebergTestTables t = create(SCHEMA);
    for (int i = 0; i < numFiles; ++i) t.append(t.file(100, null));
    return ifx.addPathB(t, params);
  }

  private static final class Analyzed {
    final ComputeStatsStmt stmt;
    final TComputeStatsParams params;
    final List<String> warnings;

    Analyzed(AnalysisResult res) {
      stmt = (ComputeStatsStmt) res.getStmt();
      params = stmt.toThrift();
      warnings = res.getAnalyzer().getWarnings();
    }

    TIcebergPuffinStatsParams puffin() {
      assertTrue("no iceberg_puffin_stats", params.isSetIceberg_puffin_stats());
      return params.getIceberg_puffin_stats();
    }
  }

  private Analyzed analyze(String sql, AnalysisContext ctx) throws Exception {
    return new Analyzed(feFixture_.parseAndAnalyze(sql, ctx));
  }

  private Analyzed analyze(String sql) throws Exception {
    return analyze(sql, feFixture_.createAnalysisCtx());
  }

  private String asOf(long s1) {
    return " FROM " + fullName() + " FOR SYSTEM_VERSION AS OF " + s1;
  }

  /** Expected metric expressions of a fixed-size column read by 'src'. */
  private static String fixed(String slot0, String src, int size) {
    return slot0 + ", COUNT(CASE WHEN " + src + " IS NULL THEN 1 ELSE NULL END), "
        + size + ", CAST(" + size + " as DOUBLE), NULL, NULL, NULL, NULL";
  }

  /** Expected metric expressions of a STRING or BINARY column read by 'src'. */
  private static String string(String slot0, String src) {
    return slot0 + ", COUNT(CASE WHEN " + src + " IS NULL THEN 1 ELSE NULL END), "
        + "MAX(length(" + src + ")), AVG(length(" + src + ")), NULL, NULL, NULL, NULL";
  }

  private static Map<String, Integer> fieldIds(Object... keysAndIds) {
    Map<String, Integer> res = new LinkedHashMap<>();
    for (int i = 0; i < keysAndIds.length; i += 2) {
      res.put((String) keysAndIds[i], (Integer) keysAndIds[i + 1]);
    }
    return res;
  }

  /** Fields that are not part of LEGACY and FULL plans. */
  private static void assertNoIncrementalFields(Analyzed a) {
    assertFalse(a.params.isIs_incremental());
    assertFalse(a.params.isSetIceberg_theta_query());
    assertFalse(a.params.isSetNo_child_queries_message());
    assertFalse(a.puffin().isSetMetrics_base_snapshot_id());
    assertFalse(a.puffin().isSetNdv_bases());
    assertFalse(a.puffin().isSetTheta_sketches());
  }

  private static final String FULL_METRICS_SCHEMA = "SELECT "
      + "COUNT(id) AS id, COUNT(CASE WHEN id IS NULL THEN 1 ELSE NULL END), "
      + "4, CAST(4 as DOUBLE), NULL, NULL, NULL, NULL, "
      + "COUNT(`date`) AS `date`, COUNT(CASE WHEN `date` IS NULL THEN 1 ELSE NULL END), "
      + "4, CAST(4 as DOUBLE), NULL, NULL, NULL, NULL, "
      + "COUNT(userid) AS userid, COUNT(CASE WHEN userid IS NULL THEN 1 ELSE NULL END), "
      + "MAX(length(userid)), AVG(length(userid)), NULL, NULL, NULL, NULL, "
      + "COUNT(b) AS b, COUNT(CASE WHEN b IS NULL THEN 1 ELSE NULL END), "
      + "MAX(length(b)), AVG(length(b)), NULL, NULL, NULL, NULL, "
      + "ds_theta_sketch_iceberg(id) AS theta_1, "
      + "ds_theta_sketch_iceberg(`date`) AS theta_2, "
      + "ds_theta_sketch_iceberg(userid) AS theta_3";

  private void assertFullOfSchema(Analyzed a, long s1) {
    assertEquals("SELECT COUNT(*)" + asOf(s1), a.stmt.getTblStatsQuery());
    assertEquals(FULL_METRICS_SCHEMA + asOf(s1), a.stmt.getColStatsQuery());
    assertEquals(TIcebergStatsMode.FULL, a.puffin().getMode());
    assertEquals(s1, a.puffin().getCalc_snapshot_id());
    assertEquals(fieldIds("id", 1, "date", 2, "userid", 3, "b", 4),
        a.puffin().getColumn_field_ids());
    assertEquals(Arrays.asList(1, 2, 3), a.puffin().getTheta_field_ids());
    assertFalse(a.puffin().isSetSampled());
    assertNoIncrementalFields(a);
  }

  private void assertLegacyOfSchema(Analyzed a, long s1) {
    String from = s1 == -1 ? " FROM " + fullName() : asOf(s1);
    assertEquals("SELECT COUNT(*)" + from, a.stmt.getTblStatsQuery());
    assertEquals("SELECT "
        + fixed("NDV(id) AS id", "id", 4) + ", "
        + fixed("NDV(`date`) AS `date`", "`date`", 4) + ", "
        + string("NDV(userid) AS userid", "userid") + ", "
        + string("NULL AS b", "b") + from, a.stmt.getColStatsQuery());
    assertEquals(TIcebergStatsMode.LEGACY, a.puffin().getMode());
    assertEquals(s1, a.puffin().getCalc_snapshot_id());
    assertEquals(fieldIds("id", 1, "date", 2, "userid", 3, "b", 4),
        a.puffin().getColumn_field_ids());
    assertFalse(a.puffin().isSetTheta_field_ids());
    assertTrue(a.puffin().isSetSampled());
    assertFalse(a.puffin().isSampled());
    assertNoIncrementalFields(a);
  }

  @Test
  public void fullReadsS1WithCountAndThetaTail() throws Exception {
    IcebergTable tbl = table(1, Collections.emptyMap());

    Analyzed a = analyze("compute stats " + fullName());

    assertFullOfSchema(a, tbl.snapshotId());
    assertEquals(Collections.emptyList(), a.warnings);
  }

  @Test
  public void writeFlagAndPropertyChooseModeOfEachCommand() throws Exception {
    IcebergTable tbl = table(1, Collections.emptyMap());
    long s1 = tbl.snapshotId();
    String sql = "compute stats " + fullName();

    setWriteFlag(false);
    assertLegacyOfSchema(analyze(sql), s1);
    setWriteFlag(true);
    assertFullOfSchema(analyze(sql), s1);
    setWriteFlag(false);
    assertLegacyOfSchema(analyze(sql), s1);
  }

  @Test
  public void falsePropertyGivesLegacy() throws Exception {
    IcebergTable tbl = table(1,
        Collections.singletonMap(IcebergTable.ICEBERG_WRITE_PUFFIN_STATS, "FALSE"));

    assertLegacyOfSchema(analyze("compute stats " + fullName()), tbl.snapshotId());
  }

  @Test
  public void tableSampleGivesSampledLegacy() throws Exception {
    IcebergTable tbl = table(4, Collections.singletonMap(EXTRAPOLATION, "true"));
    long s1 = tbl.snapshotId();
    TQueryOptions opts = new TQueryOptions();
    opts.setCompute_stats_min_sample_size(0);

    Analyzed a = analyze("compute stats " + fullName()
        + " (id, UserId) tablesample system(10) repeatable(7)",
        feFixture_.createAnalysisCtx(opts));

    assertEquals(0.25, a.stmt.getEffectiveSamplingPerc(), 0);
    String from = asOf(s1) + " TABLESAMPLE SYSTEM(10) REPEATABLE(7)";
    assertEquals("SELECT CAST(ROUND(COUNT(*) / 0.2500000000) AS BIGINT)" + from,
        a.stmt.getTblStatsQuery());
    assertEquals("SELECT "
        + fixed("SAMPLED_NDV(id, 0.2500000000) AS id", "id", 4) + ", "
        + string("SAMPLED_NDV(userid, 0.2500000000) AS userid", "userid") + from,
        a.stmt.getColStatsQuery());
    assertEquals(TIcebergStatsMode.LEGACY, a.puffin().getMode());
    assertEquals(s1, a.puffin().getCalc_snapshot_id());
    assertEquals(fieldIds("id", 1, "userid", 3), a.puffin().getColumn_field_ids());
    assertFalse(a.puffin().isSetTheta_field_ids());
    assertTrue(a.puffin().isSampled());
    assertNoIncrementalFields(a);
  }

  @Test
  public void fullSampleRateGivesFullWithoutTableSample() throws Exception {
    IcebergTable tbl = table(4, Collections.singletonMap(EXTRAPOLATION, "true"));

    // The default minimum sample size covers the whole table.
    Analyzed a = analyze(
        "compute stats " + fullName() + " tablesample system(10) repeatable(7)");

    assertEquals(1.0, a.stmt.getEffectiveSamplingPerc(), 0);
    assertFullOfSchema(a, tbl.snapshotId());
    assertEquals(1, a.warnings.size());
    assertTrue(a.warnings.get(0), a.warnings.get(0).startsWith(
        "Ignoring TABLESAMPLE because the effective sampling rate is 100%."));
  }

  @Test
  public void incrementalWithoutBaseGivesFullWithWarning() throws Exception {
    IcebergTable tbl = table(1, Collections.emptyMap());

    try (FrontendProfile.Scope scope = FrontendProfile.createNewWithScope()) {
      Analyzed a = analyze("compute incremental stats " + fullName());

      assertFullOfSchema(a, tbl.snapshotId());
      assertEquals(Collections.singletonList("COMPUTE INCREMENTAL STATS on Iceberg table "
          + fullName() + " computes full statistics: no statistics computed by Impala"
          + " to use as a base."), a.warnings);
      assertEquals("NO_BASE",
          FrontendProfile.getCurrent().getInfoString("Iceberg stats reason"));
    }
  }

  @Test
  public void noMetricColumnsGivesOnlyTableQuery() throws Exception {
    IcebergTable tbl = table(1, Collections.emptyMap());
    long s1 = tbl.snapshotId();

    Analyzed a = analyze("compute stats " + fullName() + " ()");

    assertEquals("SELECT COUNT(*)" + asOf(s1), a.stmt.getTblStatsQuery());
    assertNull(a.stmt.getColStatsQuery());
    assertFalse(a.params.isSetCol_stats_query());
    assertEquals(TIcebergStatsMode.FULL, a.puffin().getMode());
    assertEquals(s1, a.puffin().getCalc_snapshot_id());
    assertEquals(Collections.emptyMap(), a.puffin().getColumn_field_ids());
    assertEquals(Collections.emptyList(), a.puffin().getTheta_field_ids());
  }

  @Test
  public void tableWithoutSnapshotsGivesLegacyWithoutAsOf() throws Exception {
    ifx.addPathB(create(SCHEMA), Collections.emptyMap());

    assertLegacyOfSchema(analyze("compute stats " + fullName()), -1);
  }

  @Test
  public void profileGetsModeAndSnapshotInsideScope() throws Exception {
    IcebergTable tbl = table(1, Collections.emptyMap());

    try (FrontendProfile.Scope scope = FrontendProfile.createNewWithScope()) {
      analyze("compute stats " + fullName());
      FrontendProfile profile = FrontendProfile.getCurrent();
      assertEquals("FULL", profile.getInfoString("Iceberg stats mode"));
      assertEquals(Long.toString(tbl.snapshotId()),
          profile.getInfoString("Iceberg stats snapshot"));
    }
  }

  @Test
  public void expiredS1FailsWithMetadataError() throws Exception {
    IcebergTestTables t = create(SCHEMA);
    t.append(t.file(100, null));
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    long s1 = tbl.snapshotId();
    t.append(t.file(100, null));
    t.expire(s1);
    // The cached table sees metadata committed after its load only on refresh.
    tbl.getIcebergApiTable().refresh();

    for (boolean writeEnabled : new boolean[] {true, false}) {
      setWriteFlag(writeEnabled);
      try {
        feFixture_.parseAndAnalyze(
            "compute stats " + fullName(), feFixture_.createAnalysisCtx());
        fail("S1 " + s1 + " is expired");
      } catch (AnalysisException e) {
        assertEquals("Failed to read metadata of Iceberg table " + fullName()
            + ": Cannot find snapshot with ID " + s1, e.getMessage());
      }
    }
  }

  @Test
  public void schemaChangesWithoutSnapshotReadS1InAllModes() throws Exception {
    IcebergTestTables t = create(new Schema(
        optional(1, "id", Types.IntegerType.get()),
        optional(2, "s", Types.StringType.get()),
        optional(3, "x", Types.StringType.get())));
    long s1 = t.append(t.file(100, null));
    t.table().updateSchema()
        .updateColumn("id", Types.LongType.get())
        .renameColumn("s", "s_new")
        .addColumn("n", Types.IntegerType.get())
        .commit();
    t.table().updateSchema().deleteColumn("x").commit();
    t.table().updateSchema().addColumn("x", Types.StringType.get()).commit();
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    assertEquals(s1, tbl.snapshotId());
    String sql = "compute stats " + fullName();
    Map<String, Integer> ids = fieldIds("id", 1, "s_new", 2, "n", 4, "x", 5);

    Analyzed full = analyze(sql);
    assertEquals("SELECT COUNT(*)" + asOf(s1), full.stmt.getTblStatsQuery());
    assertEquals("SELECT "
        + fixed("COUNT(CAST(id AS BIGINT)) AS id", "CAST(id AS BIGINT)", 8) + ", "
        + string("COUNT(s) AS s_new", "s") + ", "
        + fixed("COUNT(CAST(NULL AS INT)) AS n", "CAST(NULL AS INT)", 4) + ", "
        + string("COUNT(CAST(NULL AS STRING)) AS x", "CAST(NULL AS STRING)") + ", "
        + "ds_theta_sketch_iceberg(CAST(id AS BIGINT)) AS theta_1, "
        + "ds_theta_sketch_iceberg(s) AS theta_2, "
        + "ds_theta_sketch_iceberg(CAST(NULL AS INT)) AS theta_4, "
        + "ds_theta_sketch_iceberg(CAST(NULL AS STRING)) AS theta_5" + asOf(s1),
        full.stmt.getColStatsQuery());
    assertEquals(TIcebergStatsMode.FULL, full.puffin().getMode());
    assertEquals(ids, full.puffin().getColumn_field_ids());
    assertEquals(Arrays.asList(1, 2, 4, 5), full.puffin().getTheta_field_ids());

    setWriteFlag(false);
    Analyzed legacy = analyze(sql);
    assertEquals("SELECT COUNT(*)" + asOf(s1), legacy.stmt.getTblStatsQuery());
    assertEquals("SELECT "
        + fixed("NDV(CAST(id AS BIGINT)) AS id", "CAST(id AS BIGINT)", 8) + ", "
        + string("NDV(s) AS s_new", "s") + ", "
        + fixed("NDV(CAST(NULL AS INT)) AS n", "CAST(NULL AS INT)", 4) + ", "
        + string("NDV(CAST(NULL AS STRING)) AS x", "CAST(NULL AS STRING)")
        + asOf(s1), legacy.stmt.getColStatsQuery());
    assertEquals(TIcebergStatsMode.LEGACY, legacy.puffin().getMode());
    assertEquals(s1, legacy.puffin().getCalc_snapshot_id());
    assertEquals(ids, legacy.puffin().getColumn_field_ids());
  }

  @Test
  public void schemaChangesWithoutSnapshotReadS1InSampledLegacy() throws Exception {
    IcebergTestTables t = create(new Schema(
        optional(1, "id", Types.IntegerType.get()),
        optional(2, "s", Types.StringType.get()),
        optional(3, "x", Types.StringType.get())));
    for (int i = 0; i < 4; ++i) t.append(t.file(100, null));
    t.table().updateSchema()
        .updateColumn("id", Types.LongType.get())
        .renameColumn("s", "s_new")
        .addColumn("n", Types.IntegerType.get())
        .commit();
    t.table().updateSchema().deleteColumn("x").commit();
    t.table().updateSchema().addColumn("x", Types.StringType.get()).commit();
    IcebergTable tbl = ifx.addPathB(t, Collections.singletonMap(EXTRAPOLATION, "true"));
    long s1 = tbl.snapshotId();
    TQueryOptions opts = new TQueryOptions();
    opts.setCompute_stats_min_sample_size(0);

    Analyzed a = analyze("compute stats " + fullName()
        + " tablesample system(10) repeatable(7)", feFixture_.createAnalysisCtx(opts));

    assertEquals(0.25, a.stmt.getEffectiveSamplingPerc(), 0);
    String from = asOf(s1) + " TABLESAMPLE SYSTEM(10) REPEATABLE(7)";
    assertEquals("SELECT CAST(ROUND(COUNT(*) / 0.2500000000) AS BIGINT)" + from,
        a.stmt.getTblStatsQuery());
    assertEquals("SELECT "
        + fixed("SAMPLED_NDV(CAST(id AS BIGINT), 0.2500000000) AS id",
            "CAST(id AS BIGINT)", 8) + ", "
        + string("SAMPLED_NDV(s, 0.2500000000) AS s_new", "s") + ", "
        + fixed("SAMPLED_NDV(CAST(NULL AS INT), 0.2500000000) AS n",
            "CAST(NULL AS INT)", 4) + ", "
        + string("SAMPLED_NDV(CAST(NULL AS STRING), 0.2500000000) AS x",
            "CAST(NULL AS STRING)") + from,
        a.stmt.getColStatsQuery());
    assertEquals(TIcebergStatsMode.LEGACY, a.puffin().getMode());
    assertTrue(a.puffin().isSampled());
    assertEquals(fieldIds("id", 1, "s_new", 2, "n", 4, "x", 5),
        a.puffin().getColumn_field_ids());
  }

  @Test
  public void hdfsTableKeepsPreviousTexts() throws Exception {
    File dir = tmp.newFolder("hdfs");
    ifx.registerHdfsPathA(DB, tableName(), dir);

    Analyzed a = analyze("compute stats " + fullName());

    assertEquals("SELECT COUNT(*) FROM " + fullName(), a.stmt.getTblStatsQuery());
    assertEquals("SELECT " + fixed("NDV(id) AS id", "id", 4) + " FROM " + fullName(),
        a.stmt.getColStatsQuery());
    assertFalse(a.params.isSetIceberg_puffin_stats());
  }
}
