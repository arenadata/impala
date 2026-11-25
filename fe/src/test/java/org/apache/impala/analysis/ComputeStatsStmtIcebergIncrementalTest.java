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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mockStatic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.analysis.IcebergStatsPlan.Mode;
import org.apache.impala.analysis.IcebergStatsPlan.NdvBase;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.iceberg.IcebergTestTables;
import org.apache.impala.catalog.iceberg.PuffinFixtures;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.FrontendProfile;
import org.apache.impala.thrift.TComputeStatsParams;
import org.apache.impala.thrift.TIcebergNdvBase;
import org.apache.impala.thrift.TIcebergPuffinStatsParams;
import org.apache.impala.thrift.TIcebergStatsMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Child query texts and TComputeStatsParams that COMPUTE INCREMENTAL STATS builds for
 * Iceberg tables with statistics of an earlier computation.
 */
public class ComputeStatsStmtIcebergIncrementalTest extends FrontendTestBase {
  private static final String DB = "iceberg_compute_incremental_stats";
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "date", Types.DateType.get()),
      optional(3, "UserId", Types.StringType.get()),
      optional(4, "b", Types.BinaryType.get()));
  private static final String NOOP_MESSAGE =
      "No new data files since the last COMPUTE STATS; statistics are up to date.";

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

  private IcebergTestTables create(String suffix, Schema schema, PartitionSpec spec,
      int formatVersion) {
    return IcebergTestTables.create(tmp, DB,
        testName.getMethodName().toLowerCase() + suffix, schema, spec, formatVersion);
  }

  private IcebergTestTables create(Schema schema) {
    return create("", schema, PartitionSpec.unpartitioned(), 2);
  }

  /** Unpartitioned v2 table and partitioned v1 table, with partition path per table. */
  private IcebergTestTables[] createUnpartitionedAndPartitioned() {
    return new IcebergTestTables[] {
        create("_v2", SCHEMA, PartitionSpec.unpartitioned(), 2),
        create("_v1_part", SCHEMA,
            PartitionSpec.builderFor(SCHEMA).identity("id").build(), 1)};
  }

  private static final String[] PARTITIONS = {null, "id=1"};

  private static String fullName(IcebergTestTables t) {
    return t.db() + "." + t.name();
  }

  /** Registers theta blobs of 'fieldIds' at 'snapshotId'; returns the Puffin path. */
  private static String registerBlobs(IcebergTestTables t, long snapshotId,
      int... fieldIds) {
    List<PuffinFixtures.BlobSpec> blobs = new ArrayList<>();
    for (int f : fieldIds) {
      blobs.add(PuffinFixtures.blob(f, snapshotId, new byte[] {(byte) f}));
    }
    StatisticsFile file = PuffinFixtures.write(t.table(), snapshotId, blobs);
    t.setStats(snapshotId, file);
    return file.path();
  }

  /**
   * Loads 't' with metrics base 'bh' of every column and HMS statistics of every column,
   * as COMPUTE STATS after every commit so far leaves them.
   */
  private static IcebergTable loadWithBase(IcebergTestTables t, long bh,
      Map<String, String> params) throws Exception {
    Map<String, String> all = new HashMap<>(params);
    all.putAll(IcebergAnalysisFixture.computedAfterNow("1-4:" + bh));
    IcebergTable tbl = ifx.addPathB(t, all);
    for (Column c : tbl.getColumns()) c.getStats().setNumNulls(0);
    return tbl;
  }

  private static IcebergTable loadWithBase(IcebergTestTables t, long bh)
      throws Exception {
    return loadWithBase(t, bh, Collections.emptyMap());
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

  private static Analyzed analyze(String sql) throws Exception {
    return new Analyzed(
        feFixture_.parseAndAnalyze(sql, feFixture_.createAnalysisCtx()));
  }

  private static Analyzed analyzeIncremental(IcebergTestTables t) throws Exception {
    return analyze("compute incremental stats " + fullName(t));
  }

  private static String from(IcebergTestTables t, long s1) {
    return " FROM " + fullName(t) + " FOR SYSTEM_VERSION AS OF " + s1;
  }

  private static String after(long base) {
    return " /* +ICEBERG_ADDED_FILES_AFTER(" + base + ") */";
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

  private static final String METRICS_SCHEMA = "SELECT "
      + fixed("COUNT(id) AS id", "id", 4) + ", "
      + fixed("COUNT(`date`) AS `date`", "`date`", 4) + ", "
      + string("COUNT(userid) AS userid", "userid") + ", "
      + string("COUNT(b) AS b", "b");
  private static final String THETA_SCHEMA = "ds_theta_sketch_iceberg(id) AS theta_1, "
      + "ds_theta_sketch_iceberg(`date`) AS theta_2, "
      + "ds_theta_sketch_iceberg(userid) AS theta_3";
  private static final Map<String, Integer> FIELD_IDS_SCHEMA =
      fieldIds("id", 1, "date", 2, "userid", 3, "b", 4);

  private static Map<String, Integer> fieldIds(Object... keysAndIds) {
    Map<String, Integer> res = new LinkedHashMap<>();
    for (int i = 0; i < keysAndIds.length; i += 2) {
      res.put((String) keysAndIds[i], (Integer) keysAndIds[i + 1]);
    }
    return res;
  }

  /** Expected NDV bases of 'fieldIds', all in the blobs of 'snapshotId' at 'path'. */
  private static List<TIcebergNdvBase> bases(long snapshotId, String path,
      int... fieldIds) {
    List<TIcebergNdvBase> res = new ArrayList<>();
    for (int f : fieldIds) res.add(new TIcebergNdvBase(f, snapshotId, path));
    return res;
  }

  private static void assertIncremental(Analyzed a, long s1, long bh,
      Map<String, Integer> columnFieldIds, List<Integer> thetaFieldIds,
      List<TIcebergNdvBase> ndvBases) {
    assertFalse(a.params.isIs_incremental());
    assertFalse(a.params.isSetNo_child_queries_message());
    TIcebergPuffinStatsParams p = a.puffin();
    assertEquals(TIcebergStatsMode.INCREMENTAL, p.getMode());
    assertEquals(s1, p.getCalc_snapshot_id());
    assertTrue(p.isSetMetrics_base_snapshot_id());
    assertEquals(bh, p.getMetrics_base_snapshot_id());
    assertEquals(columnFieldIds, p.getColumn_field_ids());
    assertEquals(thetaFieldIds, p.getTheta_field_ids());
    assertTrue(p.isSetNdv_bases());
    assertEquals(ndvBases, p.getNdv_bases());
    assertFalse(p.isSetSampled());
    assertFalse(p.isSetTheta_sketches());
  }

  @Test
  public void ndvBaseEqualToMetricsBaseGivesThetaTail() throws Exception {
    // Unpartitioned v2 and partitioned v1 tables give the same texts.
    IcebergTestTables[] tables = createUnpartitionedAndPartitioned();
    for (int i = 0; i < tables.length; ++i) {
      IcebergTestTables t = tables[i];
      long bh = t.append(t.file(100, PARTITIONS[i]));
      String path = registerBlobs(t, bh, 1, 2, 3);
      long s1 = t.append(t.file(100, PARTITIONS[i]));
      loadWithBase(t, bh);

      try (FrontendProfile.Scope scope = FrontendProfile.createNewWithScope()) {
        Analyzed a = analyzeIncremental(t);

        assertEquals("SELECT COUNT(*)" + from(t, s1), a.stmt.getTblStatsQuery());
        assertEquals(METRICS_SCHEMA + ", " + THETA_SCHEMA + from(t, s1) + after(bh),
            a.stmt.getColStatsQuery());
        assertEquals(a.stmt.getTblStatsQuery(), a.params.getTbl_stats_query());
        assertEquals(a.stmt.getColStatsQuery(), a.params.getCol_stats_query());
        assertFalse(a.params.isSetIceberg_theta_query());
        assertIncremental(a, s1, bh, FIELD_IDS_SCHEMA, Arrays.asList(1, 2, 3),
            bases(bh, path, 1, 2, 3));
        assertEquals(Collections.emptyList(), a.warnings);
        FrontendProfile profile = FrontendProfile.getCurrent();
        assertEquals("INCREMENTAL", profile.getInfoString("Iceberg stats mode"));
        assertEquals(Long.toString(s1),
            profile.getInfoString("Iceberg stats snapshot"));
        assertEquals(Long.toString(bh),
            profile.getInfoString("Iceberg stats metrics base"));
        assertEquals(Long.toString(bh), profile.getInfoString("Iceberg stats NDV base"));
      }
    }
  }

  @Test
  public void newerNdvBaseGivesThetaQueryAfterIt() throws Exception {
    IcebergTestTables[] tables = createUnpartitionedAndPartitioned();
    for (int i = 0; i < tables.length; ++i) {
      IcebergTestTables t = tables[i];
      long bh = t.append(t.file(100, PARTITIONS[i]));
      registerBlobs(t, bh, 1, 2, 3);
      long bn = t.append(t.file(100, PARTITIONS[i]));
      String bnPath = registerBlobs(t, bn, 1, 2, 3);
      long s1 = t.append(t.file(100, PARTITIONS[i]));
      loadWithBase(t, bh, Collections.singletonMap(
          IcebergIncrementalBaseRules.MAX_DELETED_RATIO, "x"));

      Analyzed a = analyzeIncremental(t);

      assertEquals("SELECT COUNT(*)" + from(t, s1), a.stmt.getTblStatsQuery());
      assertEquals(METRICS_SCHEMA + from(t, s1) + after(bh), a.stmt.getColStatsQuery());
      assertEquals("SELECT " + THETA_SCHEMA + from(t, s1) + after(bn),
          a.params.getIceberg_theta_query());
      assertIncremental(a, s1, bh, FIELD_IDS_SCHEMA, Arrays.asList(1, 2, 3),
          bases(bn, bnPath, 1, 2, 3));
      assertEquals(Collections.singletonList("Ignoring invalid value 'x' of table"
          + " property '" + IcebergIncrementalBaseRules.MAX_DELETED_RATIO + "'."),
          a.warnings);

      a.stmt.reset();
      assertNull(a.stmt.getTblStatsQuery());
      assertNull(a.stmt.getColStatsQuery());
      assertFalse(a.stmt.toThrift().isSetIceberg_theta_query());
    }
  }

  @Test
  public void columnListComputesOnlyListedColumns() throws Exception {
    IcebergTestTables[] tables = createUnpartitionedAndPartitioned();
    for (int i = 0; i < tables.length; ++i) {
      IcebergTestTables t = tables[i];
      long bh = t.append(t.file(100, PARTITIONS[i]));
      String path = registerBlobs(t, bh, 1, 2, 3);
      long s1 = t.append(t.file(100, PARTITIONS[i]));
      loadWithBase(t, bh);

      Analyzed a = analyze("compute incremental stats " + fullName(t) + " (UserId)");
      assertEquals("SELECT COUNT(*)" + from(t, s1), a.stmt.getTblStatsQuery());
      assertEquals("SELECT " + string("COUNT(userid) AS userid", "userid")
          + ", ds_theta_sketch_iceberg(userid) AS theta_3" + from(t, s1) + after(bh),
          a.stmt.getColStatsQuery());
      assertFalse(a.params.isSetIceberg_theta_query());
      assertIncremental(a, s1, bh, fieldIds("userid", 3), Collections.singletonList(3),
          bases(bh, path, 3));

      // A column without theta sketches has no NDV base: neither tail nor theta query.
      a = analyze("compute incremental stats " + fullName(t) + " (b)");
      assertEquals("SELECT COUNT(*)" + from(t, s1), a.stmt.getTblStatsQuery());
      assertEquals("SELECT " + string("COUNT(b) AS b", "b") + from(t, s1) + after(bh),
          a.stmt.getColStatsQuery());
      assertFalse(a.params.isSetIceberg_theta_query());
      assertIncremental(a, s1, bh, fieldIds("b", 4), Collections.emptyList(),
          Collections.emptyList());
      assertEquals(Collections.emptyList(), a.warnings);
    }
  }

  @Test
  public void renamedColumnReadsNameOfS1AndPromotedTypeGivesFull() throws Exception {
    IcebergTestTables t = create(new Schema(
        optional(1, "id", Types.IntegerType.get()),
        optional(2, "s", Types.StringType.get())));
    long bh = t.append(t.file(100, null));
    registerBlobs(t, bh, 1, 2);
    long bn = t.append(t.file(100, null));
    String bnPath = registerBlobs(t, bn, 1, 2);
    long s1 = t.append(t.file(100, null));
    t.table().updateSchema().renameColumn("s", "s_new").commit();
    loadWithBase(t, bh);

    Analyzed a = analyzeIncremental(t);
    assertEquals("SELECT COUNT(*)" + from(t, s1), a.stmt.getTblStatsQuery());
    assertEquals("SELECT " + fixed("COUNT(id) AS id", "id", 4) + ", "
        + string("COUNT(s) AS s_new", "s") + from(t, s1) + after(bh),
        a.stmt.getColStatsQuery());
    assertEquals("SELECT ds_theta_sketch_iceberg(id) AS theta_1, "
        + "ds_theta_sketch_iceberg(s) AS theta_2" + from(t, s1) + after(bn),
        a.params.getIceberg_theta_query());
    assertIncremental(a, s1, bh, fieldIds("id", 1, "s_new", 2), Arrays.asList(1, 2),
        bases(bn, bnPath, 1, 2));

    t.table().updateSchema().updateColumn("id", Types.LongType.get()).commit();
    loadWithBase(t, bh);
    try (FrontendProfile.Scope scope = FrontendProfile.createNewWithScope()) {
      a = analyzeIncremental(t);

      assertEquals(Collections.singletonList("COMPUTE INCREMENTAL STATS on Iceberg table "
          + fullName(t) + " computes full statistics: type of column(s) id changed"
          + " since their base statistics."), a.warnings);
      assertEquals("COLUMNS_TYPE_CHANGED",
          FrontendProfile.getCurrent().getInfoString("Iceberg stats reason"));
    }
    assertEquals("SELECT COUNT(*)" + from(t, s1), a.stmt.getTblStatsQuery());
    assertEquals("SELECT "
        + fixed("COUNT(CAST(id AS BIGINT)) AS id", "CAST(id AS BIGINT)", 8) + ", "
        + string("COUNT(s) AS s_new", "s") + ", "
        + "ds_theta_sketch_iceberg(CAST(id AS BIGINT)) AS theta_1, "
        + "ds_theta_sketch_iceberg(s) AS theta_2" + from(t, s1),
        a.stmt.getColStatsQuery());
    assertFalse(a.params.isSetIceberg_theta_query());
    assertEquals(TIcebergStatsMode.FULL, a.puffin().getMode());
    assertEquals(s1, a.puffin().getCalc_snapshot_id());
    assertEquals(fieldIds("id", 1, "s_new", 2), a.puffin().getColumn_field_ids());
    assertEquals(Arrays.asList(1, 2), a.puffin().getTheta_field_ids());
    assertFalse(a.puffin().isSetMetrics_base_snapshot_id());
    assertFalse(a.puffin().isSetNdv_bases());
  }

  @Test
  public void emptyGapGivesNoopAndOnlyDeletesGiveOnlyTableQuery() throws Exception {
    IcebergTestTables t = create(SCHEMA);
    DataFile f0 = t.file(100, null);
    long bh = t.append(f0, t.file(100, null));
    registerBlobs(t, bh, 1, 2, 3);
    loadWithBase(t, bh);

    try (FrontendProfile.Scope scope = FrontendProfile.createNewWithScope()) {
      Analyzed noop = analyzeIncremental(t);

      assertNull(noop.stmt.getTblStatsQuery());
      assertNull(noop.stmt.getColStatsQuery());
      assertFalse(noop.params.isSetTbl_stats_query());
      assertFalse(noop.params.isSetCol_stats_query());
      assertFalse(noop.params.isSetIceberg_theta_query());
      assertFalse(noop.params.isSetIceberg_puffin_stats());
      assertEquals(NOOP_MESSAGE, noop.params.getNo_child_queries_message());
      assertEquals(Collections.emptyList(), noop.warnings);
      assertEquals("NOOP",
          FrontendProfile.getCurrent().getInfoString("Iceberg stats mode"));
    }

    long s1 = t.delete(f0);
    loadWithBase(t, bh);
    Analyzed deletes = analyzeIncremental(t);

    assertEquals("SELECT COUNT(*)" + from(t, s1), deletes.stmt.getTblStatsQuery());
    assertEquals(deletes.stmt.getTblStatsQuery(), deletes.params.getTbl_stats_query());
    assertNull(deletes.stmt.getColStatsQuery());
    assertFalse(deletes.params.isSetCol_stats_query());
    assertFalse(deletes.params.isSetIceberg_theta_query());
    assertFalse(deletes.params.isSetNo_child_queries_message());
    assertEquals(TIcebergStatsMode.FULL, deletes.puffin().getMode());
    assertEquals(s1, deletes.puffin().getCalc_snapshot_id());
    assertEquals(Collections.emptyMap(), deletes.puffin().getColumn_field_ids());
    assertEquals(Collections.emptyList(), deletes.puffin().getTheta_field_ids());
    assertFalse(deletes.puffin().isSetMetrics_base_snapshot_id());
    assertFalse(deletes.puffin().isSetNdv_bases());
    assertEquals(Collections.emptyList(), deletes.warnings);
  }

  /** Plan of the resolver with other bases, as a defective resolver could build it. */
  private static IcebergStatsPlan withBases(IcebergStatsPlan p, Long bh, Long bn,
      Map<Integer, NdvBase> ndvBases) {
    assertEquals(Mode.INCREMENTAL, p.mode());
    return new IcebergStatsPlan(p.mode(), p.s1(), p.metricColumns(), p.thetaFieldIds(),
        p.deletesOnly(), bh, bn, ndvBases, p.reason(), p.warnings(),
        p.profileInfo(), p.profileCounters());
  }

  private static void assertPlanRejected(IcebergTestTables t,
      UnaryOperator<IcebergStatsPlan> defect, String message) throws Exception {
    try (MockedStatic<IcebergStatsBaseResolver> resolver =
             mockStatic(IcebergStatsBaseResolver.class, Mockito.CALLS_REAL_METHODS)) {
      resolver.when(() -> IcebergStatsBaseResolver.resolve(
          any(FeIcebergTable.class), anyList(), anyBoolean(), anyBoolean()))
          .thenAnswer(inv -> {
            IcebergStatsPlan p = (IcebergStatsPlan) inv.callRealMethod();
            return defect.apply(p);
          });
      analyzeIncremental(t);
      fail("plan accepted");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage(), e.getMessage().contains(message));
    }
  }

  @Test
  public void incrementalPlanWithoutBasesFailsPrecondition() throws Exception {
    IcebergTestTables t = create(SCHEMA);
    long bh = t.append(t.file(100, null));
    registerBlobs(t, bh, 1, 2, 3);
    t.append(t.file(100, null));
    loadWithBase(t, bh);

    assertPlanRejected(t, p -> withBases(p, null, p.bn(), p.ndvBases()),
        "has no metrics base");
    assertPlanRejected(t, p -> {
      Map<Integer, NdvBase> ndvBases = new HashMap<>(p.ndvBases());
      ndvBases.remove(3);
      return withBases(p, p.bh(), p.bn(), ndvBases);
    }, "do not match theta field ids [1, 2, 3]");
    assertPlanRejected(t, p -> withBases(p, p.bh(), null, p.ndvBases()),
        "NDV base snapshot of table");
  }
}
