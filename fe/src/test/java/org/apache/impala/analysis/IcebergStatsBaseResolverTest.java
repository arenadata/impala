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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.IcebergStatsPlan.MetricColumn;
import org.apache.impala.analysis.IcebergStatsPlan.Mode;
import org.apache.impala.analysis.IcebergStatsPlan.NdvBase;
import org.apache.impala.analysis.IcebergStatsPlan.ReasonKind;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.iceberg.IcebergTestTables;
import org.apache.impala.catalog.iceberg.PuffinFixtures;
import org.apache.impala.catalog.iceberg.PuffinFixtures.BlobSpec;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.service.BackendConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

/**
 * Mode, snapshot, source expressions and theta columns chosen by IcebergStatsBaseResolver
 * for Iceberg tables in the impalad cache, and the "Incremental stats" predicate.
 */
public class IcebergStatsBaseResolverTest extends FrontendTestBase {
  private static final String DB = "iceberg_stats_resolver";
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "s", Types.StringType.get()),
      optional(3, "b", Types.BinaryType.get()),
      optional(4, "d", Types.DecimalType.of(9, 2)),
      optional(5, "ts", Types.TimestampType.withoutZone()));
  private static final Schema ID_S_X = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "s", Types.StringType.get()),
      optional(3, "x", Types.StringType.get()));
  private static final List<Integer> THETA_FIELDS = Arrays.asList(1, 2, 4, 5);
  /** HMS NDV of every column with statistics and "ndv" of the Impala blobs. */
  private static final long NDV = 10;
  private static final String IDS = IcebergTable.COMPUTE_STATS_SNAPSHOT_IDS;
  private static final String WRITE = IcebergTable.ICEBERG_WRITE_PUFFIN_STATS;

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

  private IcebergTestTables create(String suffix, Schema schema) {
    return IcebergTestTables.create(tmp, DB,
        testName.getMethodName().toLowerCase() + suffix, schema,
        PartitionSpec.unpartitioned(), 2);
  }

  /** Table of SCHEMA with one snapshot, loaded with 'params' added to HMS parameters. */
  private IcebergTable table(String suffix, Map<String, String> params)
      throws Exception {
    IcebergTestTables t = create(suffix, SCHEMA);
    t.append(t.file(100, null));
    return ifx.addPathB(t, params);
  }

  private IcebergTable table(Map<String, String> params) throws Exception {
    return table("", params);
  }

  /** Loads 't' with every column given HMS statistics, as COMPUTE STATS leaves them. */
  private static IcebergTable loadWithStats(IcebergTestTables t,
      Map<String, String> params) throws Exception {
    IcebergTable tbl = ifx.addPathB(t, params);
    for (Column c : tbl.getColumns()) {
      c.getStats().setNumNulls(0);
      c.getStats().setNumDistinctValues(NDV);
    }
    return tbl;
  }

  /** Impala theta blobs of every theta column of SCHEMA at 'snapshotId'. */
  private static StatisticsFile registerImpalaBlobs(IcebergTestTables t,
      long snapshotId) {
    List<BlobSpec> blobs = new ArrayList<>();
    for (int f : THETA_FIELDS) {
      blobs.add(PuffinFixtures.blob(f, snapshotId, new byte[] {1, 2, 3})
          .ndvProperty(Long.toString(NDV)).impalaCreatedBy("Impala 4.5.0"));
    }
    StatisticsFile file = PuffinFixtures.write(t.table(), snapshotId, blobs);
    t.setStats(snapshotId, file);
    return file;
  }

  private static Map<String, String> param(String key, String value) {
    return Collections.singletonMap(key, value);
  }

  private static IcebergStatsPlan resolve(IcebergTable tbl, boolean incremental,
      boolean sampled) throws AnalysisException {
    return IcebergStatsBaseResolver.resolve(
        tbl, new ArrayList<>(tbl.getColumns()), incremental, sampled);
  }

  private static List<String> sourceSql(IcebergStatsPlan plan) {
    List<String> res = new ArrayList<>();
    for (MetricColumn c : plan.metricColumns()) res.add(c.sourceSql());
    return res;
  }

  private static Map<String, String> profile(Mode mode, long s1) {
    Map<String, String> res = new LinkedHashMap<>();
    res.put("Iceberg stats mode", mode.name());
    res.put("Iceberg stats snapshot", Long.toString(s1));
    return res;
  }

  /** Fields that only incremental bases fill. */
  private static void assertNoBases(IcebergStatsPlan plan) {
    assertNull(plan.bh());
    assertNull(plan.bn());
    assertTrue(plan.ndvBases().isEmpty());
    assertNull(plan.reason());
    assertTrue(plan.warnings().isEmpty());
    assertTrue(plan.profileCounters().isEmpty());
  }

  @Test
  public void computeStatsGivesFullOnCurrentSnapshot() throws Exception {
    IcebergTable tbl = table(Collections.emptyMap());
    long s1 = tbl.getIcebergApiTable().currentSnapshot().snapshotId();

    IcebergStatsPlan plan = resolve(tbl, false, false);

    assertEquals(Mode.FULL, plan.mode());
    assertEquals(s1, plan.s1());
    List<Column> cols = tbl.getColumns();
    assertEquals(cols.size(), plan.metricColumns().size());
    for (int i = 0; i < cols.size(); ++i) {
      assertSame(cols.get(i), plan.metricColumns().get(i).col());
      assertEquals(i + 1, plan.metricColumns().get(i).fieldId());
    }
    assertEquals(Arrays.asList("id", "s", "b", "d", "ts"), sourceSql(plan));
    assertEquals(Arrays.asList(1, 2, 4, 5), plan.thetaFieldIds());
    assertEquals(new ArrayList<>(profile(Mode.FULL, s1).entrySet()),
        new ArrayList<>(plan.profileInfo().entrySet()));
    assertNoBases(plan);
  }

  @Test
  public void columnListKeepsItsOrderAndThetaSkipsBinary() throws Exception {
    IcebergTable tbl = table(Collections.emptyMap());
    List<Column> cols = Arrays.asList(
        tbl.getColumn("b"), tbl.getColumn("ts"), tbl.getColumn("id"));

    IcebergStatsPlan plan = IcebergStatsBaseResolver.resolve(tbl, cols, false, false);

    assertEquals(Mode.FULL, plan.mode());
    assertEquals(Arrays.asList("b", "ts", "id"), sourceSql(plan));
    assertEquals(Arrays.asList(5, 1), plan.thetaFieldIds());
  }

  /**
   * Plan without bases whose reason is 'kind' with 'args': its only warning names
   * 'reasonText' and the profile ends with the reason.
   */
  private static void assertReason(IcebergStatsPlan plan, IcebergTable tbl,
      ReasonKind kind, List<String> args, String reasonText) {
    assertEquals(kind, plan.reason().kind());
    assertEquals(args, plan.reason().args());
    assertEquals(Collections.singletonList("COMPUTE INCREMENTAL STATS on Iceberg table "
        + tbl.getFullName() + " computes full statistics: " + reasonText + "."),
        plan.warnings());
    Map<String, String> profile = profile(plan.mode(), tbl.snapshotId());
    profile.put("Iceberg stats reason", kind.name());
    assertEquals(new ArrayList<>(profile.entrySet()),
        new ArrayList<>(plan.profileInfo().entrySet()));
    assertTrue(plan.profileCounters().isEmpty());
    assertNull(plan.bh());
    assertNull(plan.bn());
    assertTrue(plan.ndvBases().isEmpty());
    assertFalse(plan.deletesOnly());
  }

  /** LEGACY of a disabled Puffin write that COMPUTE INCREMENTAL STATS warns about. */
  private static void assertPuffinDisabled(IcebergStatsPlan plan, IcebergTable tbl) {
    assertEquals(Mode.LEGACY, plan.mode());
    assertTrue(plan.thetaFieldIds().isEmpty());
    assertReason(plan, tbl, ReasonKind.PUFFIN_DISABLED, Collections.emptyList(),
        "writing Puffin statistics is disabled ('enable_writing_puffin_stats' or"
        + " 'impala.iceberg_write_puffin_stats')");
  }

  /** FULL of all columns of SCHEMA with a reason. */
  private static void assertFullWithReason(IcebergStatsPlan plan, IcebergTable tbl,
      ReasonKind kind, List<String> args, String reasonText) {
    assertEquals(Mode.FULL, plan.mode());
    assertEquals(tbl.snapshotId(), plan.s1());
    assertEquals(Arrays.asList("id", "s", "b", "d", "ts"), sourceSql(plan));
    assertEquals(THETA_FIELDS, plan.thetaFieldIds());
    assertReason(plan, tbl, kind, args, reasonText);
  }

  @Test
  public void disabledWriteFlagGivesLegacyWithReasonOnIncremental() throws Exception {
    IcebergTable tbl = table(Collections.emptyMap());
    setWriteFlag(false);

    IcebergStatsPlan plan = resolve(tbl, false, false);
    assertEquals(Mode.LEGACY, plan.mode());
    assertEquals(tbl.snapshotId(), plan.s1());
    assertEquals(Arrays.asList("id", "s", "b", "d", "ts"), sourceSql(plan));
    assertTrue(plan.thetaFieldIds().isEmpty());
    assertEquals(profile(Mode.LEGACY, tbl.snapshotId()), plan.profileInfo());
    assertNoBases(plan);

    plan = resolve(tbl, true, false);
    assertEquals(tbl.snapshotId(), plan.s1());
    assertEquals(Arrays.asList("id", "s", "b", "d", "ts"), sourceSql(plan));
    assertPuffinDisabled(plan, tbl);
    // The disabled write is checked before the sample.
    assertPuffinDisabled(resolve(tbl, true, true), tbl);
  }

  @Test
  public void falsePropertyIgnoringCaseGivesLegacyWithReasonOnIncremental()
      throws Exception {
    String[] values = {"false", "FALSE", "FaLsE"};
    for (int i = 0; i < values.length; ++i) {
      String value = values[i];
      IcebergTable tbl = table("_" + i, param(WRITE, value));
      IcebergStatsPlan plan = resolve(tbl, false, false);
      assertEquals(value, Mode.LEGACY, plan.mode());
      assertTrue(value, plan.thetaFieldIds().isEmpty());
      assertTrue(value, plan.warnings().isEmpty());
      assertNull(value, plan.reason());
      assertPuffinDisabled(resolve(tbl, true, false), tbl);
    }
  }

  @Test
  public void otherPropertyValuesKeepFull() throws Exception {
    String[] values = {"true", "TRUE", "no", "0", ""};
    for (int i = 0; i < values.length; ++i) {
      String value = values[i];
      IcebergTable tbl = table("_" + i, param(WRITE, value));
      assertEquals("'" + value + "'", Mode.FULL, resolve(tbl, false, false).mode());
    }
  }

  @Test
  public void sampledGivesLegacy() throws Exception {
    IcebergTable tbl = table(Collections.emptyMap());

    for (boolean incremental : new boolean[] {false, true}) {
      IcebergStatsPlan plan = resolve(tbl, incremental, true);
      assertEquals(Mode.LEGACY, plan.mode());
      assertEquals(tbl.snapshotId(), plan.s1());
      assertTrue(plan.thetaFieldIds().isEmpty());
      assertEquals(Arrays.asList("id", "s", "b", "d", "ts"), sourceSql(plan));
      assertNoBases(plan);
    }
  }

  @Test
  public void incrementalWithoutBaseGivesFullWithReason() throws Exception {
    IcebergTable tbl = table(param(IDS, "1-5:" + 42));

    assertFullWithReason(resolve(tbl, true, false), tbl, ReasonKind.NO_BASE,
        Collections.emptyList(), "no statistics computed by Impala to use as a base");
  }

  @Test
  public void tableWithoutSnapshotsGivesLegacyWithColumnNames() throws Exception {
    IcebergTable tbl = ifx.addPathB(create("", SCHEMA), Collections.emptyMap());
    assertEquals(-1, tbl.snapshotId());

    for (boolean incremental : new boolean[] {false, true}) {
      IcebergStatsPlan plan = resolve(tbl, incremental, false);
      assertEquals(Mode.LEGACY, plan.mode());
      assertEquals(-1, plan.s1());
      assertEquals(Arrays.asList("id", "s", "b", "d", "ts"), sourceSql(plan));
      assertTrue(plan.thetaFieldIds().isEmpty());
      assertEquals(profile(Mode.LEGACY, -1), plan.profileInfo());
      assertNoBases(plan);
    }
  }

  /**
   * Schema changes after S1 do not make a new snapshot: the source expressions follow the
   * schema of S1, while field ids, types and theta columns follow the cached columns.
   */
  @Test
  public void sourceSqlFollowsSchemaOfS1() throws Exception {
    Schema v1 = new Schema(
        optional(1, "id", Types.IntegerType.get()),
        optional(2, "select", Types.StringType.get()),
        optional(3, "b", Types.BinaryType.get()),
        optional(4, "d", Types.DecimalType.of(9, 2)),
        optional(5, "x", Types.StringType.get()));
    IcebergTestTables t = create("", v1);
    long s1 = t.append(t.file(100, null));
    t.table().updateSchema()
        .updateColumn("id", Types.LongType.get())
        .renameColumn("select", "s_new")
        .updateColumn("d", Types.DecimalType.of(18, 2))
        .deleteColumn("x")
        .addColumn("n", Types.IntegerType.get())
        .commit();
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    assertEquals(s1, tbl.snapshotId());

    IcebergStatsPlan plan = resolve(tbl, false, false);

    assertEquals(Mode.FULL, plan.mode());
    assertEquals(s1, plan.s1());
    assertEquals(Arrays.asList("CAST(id AS BIGINT)", "`select`", "b",
        "CAST(d AS DECIMAL(18,2))", "CAST(NULL AS INT)"), sourceSql(plan));
    List<Integer> fieldIds = new ArrayList<>();
    for (MetricColumn c : plan.metricColumns()) fieldIds.add(c.fieldId());
    assertEquals(Arrays.asList(1, 2, 3, 4, 6), fieldIds);
    assertEquals(Arrays.asList(1, 2, 4, 6), plan.thetaFieldIds());

    setWriteFlag(false);
    assertEquals(sourceSql(plan), sourceSql(resolve(tbl, false, false)));
  }

  /**
   * Table of ID_S_X loaded at S1 whose Iceberg API table then sees 'change' committed
   * without a new snapshot, as a coordinator does when catalogd sent older columns.
   */
  private IcebergTable apiSchemaNewerThanColumns(
      UnaryOperator<UpdateSchema> change) throws Exception {
    IcebergTestTables t = create("", ID_S_X);
    long s1 = t.append(t.file(100, null));
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    change.apply(t.table().updateSchema()).commit();
    tbl.getIcebergApiTable().refresh();
    assertEquals(s1, tbl.snapshotId());
    return tbl;
  }

  @Test
  public void fieldDroppedAfterLoadKeepsItsNameInS1() throws Exception {
    IcebergTable tbl = apiSchemaNewerThanColumns(u -> u.deleteColumn("x"));
    assertNull(tbl.getIcebergSchema().findField(3));

    IcebergStatsPlan plan = resolve(tbl, false, false);

    assertEquals(Mode.FULL, plan.mode());
    assertEquals(Arrays.asList("id", "s", "x"), sourceSql(plan));
    assertEquals(Arrays.asList(1, 2, 3), plan.thetaFieldIds());
  }

  @Test
  public void typePromotedAfterLoadKeepsCachedColumnType() throws Exception {
    IcebergTable tbl = apiSchemaNewerThanColumns(
        u -> u.updateColumn("id", Types.LongType.get()));
    assertEquals(Types.LongType.get(), tbl.getIcebergSchema().findType(1));

    assertEquals(Arrays.asList("id", "s", "x"),
        sourceSql(resolve(tbl, false, false)));
  }

  @Test
  public void snapshotWithoutSchemaIdUsesCurrentSchema() throws Exception {
    IcebergTestTables t = create("", SCHEMA);
    long s1 = t.append(t.file(100, null));
    t.table().updateSchema().renameColumn("s", "s_new").commit();
    t.removeSnapshotSchemaIds();
    assertNull(t.table().snapshot(s1).schemaId());
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());

    IcebergStatsPlan plan = resolve(tbl, false, false);

    assertEquals(Mode.FULL, plan.mode());
    assertEquals(s1, plan.s1());
    assertEquals(Arrays.asList("id", "s_new", "b", "d", "ts"), sourceSql(plan));
  }

  @Test
  public void s1MissingFromMetadataFailsAnalysis() throws Exception {
    IcebergTestTables t = create("", SCHEMA);
    t.append(t.file(100, null));
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    long s1 = tbl.snapshotId();
    t.append(t.file(100, null));
    t.expire(s1);
    // The cached table sees metadata committed after its load only on refresh.
    tbl.getIcebergApiTable().refresh();
    assertEquals(s1, tbl.snapshotId());

    for (boolean writeEnabled : new boolean[] {true, false}) {
      setWriteFlag(writeEnabled);
      try {
        resolve(tbl, false, false);
        fail("S1 " + s1 + " is expired");
      } catch (AnalysisException e) {
        assertTrue(e.getCause() instanceof IllegalArgumentException);
        assertEquals("Failed to read metadata of Iceberg table " + tbl.getFullName()
            + ": " + e.getCause().getMessage(), e.getMessage());
      }
    }
  }

  @Test
  public void hasIncrementalBaseNeedsWriteEnabledAndBaseOfEveryColumn()
      throws Exception {
    IcebergTestTables t = create("", SCHEMA);
    long bh = t.append(t.file(100, null));
    registerImpalaBlobs(t, bh);
    Map<String, String> ids = param(IDS, "1-5:" + bh);
    IcebergTable withBase = loadWithStats(t, ids);
    assertTrue(IcebergStatsBaseResolver.hasIncrementalBase(withBase));

    assertTrue(IcebergStatsBaseResolver.hasIncrementalBase(
        loadWithStats(t, ImmutableMap.of(IDS, "1-5:" + bh, WRITE, "true"))));

    setWriteFlag(false);
    assertFalse(IcebergStatsBaseResolver.hasIncrementalBase(withBase));
    setWriteFlag(true);

    assertFalse(IcebergStatsBaseResolver.hasIncrementalBase(
        loadWithStats(t, ImmutableMap.of(IDS, "1-5:" + bh, WRITE, "False"))));

    // Parsed ids of snapshots the table does not have are no base.
    assertFalse(IcebergStatsBaseResolver.hasIncrementalBase(
        loadWithStats(t, param(IDS, "1-5:7"))));
    // Nor are ids of columns without HMS statistics.
    assertFalse(IcebergStatsBaseResolver.hasIncrementalBase(ifx.addPathB(t, ids)));
  }

  /** Plan with the gaps from 'bh' to 's1' and the profile of their bases and sizes. */
  private static void assertGapProfile(IcebergStatsPlan plan, Mode mode, long s1,
      long bh, long gapFiles, long gapBytes, long gapRecords) {
    assertEquals(mode, plan.mode());
    assertEquals(s1, plan.s1());
    Map<String, String> info = profile(mode, s1);
    info.put("Iceberg stats metrics base", Long.toString(bh));
    info.put("Iceberg stats NDV base", Long.toString(bh));
    assertEquals(new ArrayList<>(info.entrySet()),
        new ArrayList<>(plan.profileInfo().entrySet()));
    Map<String, Long> counters = ImmutableMap.of("IcebergStatsGapFiles", gapFiles,
        "IcebergStatsGapBytes", gapBytes, "IcebergStatsGapRecords", gapRecords,
        "IcebergStatsThetaGapFiles", gapFiles, "IcebergStatsThetaGapBytes", gapBytes);
    assertEquals(new ArrayList<>(counters.entrySet()),
        new ArrayList<>(plan.profileCounters().entrySet()));
  }

  @Test
  public void basesOfAllColumnsGiveIncrementalPlan() throws Exception {
    IcebergTestTables t = create("", SCHEMA);
    long bh = t.append(t.file(100, null));
    StatisticsFile file = registerImpalaBlobs(t, bh);
    DataFile added = t.file(300, null);
    long s1 = t.append(added);
    IcebergTable tbl = loadWithStats(t, ImmutableMap.of(IDS, "1-5:" + bh,
        IcebergIncrementalBaseRules.MAX_DELETED_RATIO, "x"));

    IcebergStatsPlan plan = resolve(tbl, true, false);

    assertGapProfile(plan, Mode.INCREMENTAL, s1, bh, 1, 300, added.recordCount());
    assertEquals(Arrays.asList("id", "s", "b", "d", "ts"), sourceSql(plan));
    assertEquals(THETA_FIELDS, plan.thetaFieldIds());
    assertEquals(Long.valueOf(bh), plan.bh());
    assertEquals(Long.valueOf(bh), plan.bn());
    assertEquals(THETA_FIELDS, new ArrayList<>(plan.ndvBases().keySet()));
    for (NdvBase base : plan.ndvBases().values()) {
      assertEquals(bh, base.blobSnapshotId());
      assertEquals(file.path(), base.statsFilePath());
    }
    assertNull(plan.reason());
    assertFalse(plan.deletesOnly());
    assertEquals(Collections.singletonList("Ignoring invalid value 'x' of table "
        + "property '" + IcebergIncrementalBaseRules.MAX_DELETED_RATIO + "'."),
        plan.warnings());

    // COMPUTE STATS ignores the bases.
    IcebergStatsPlan full = resolve(tbl, false, false);
    assertEquals(Mode.FULL, full.mode());
    assertEquals(THETA_FIELDS, full.thetaFieldIds());
    assertEquals(profile(Mode.FULL, s1), full.profileInfo());
    assertNoBases(full);
  }

  @Test
  public void emptyGapGivesNoopAndOnlyDeletesGiveFullWithoutColumns()
      throws Exception {
    IcebergTestTables t = create("", SCHEMA);
    DataFile f0 = t.file(100, null);
    long bh = t.append(f0, t.file(100, null));
    registerImpalaBlobs(t, bh);
    Map<String, String> ids = IcebergAnalysisFixture.computedAfterNow("1-5:" + bh);

    IcebergStatsPlan plan = resolve(loadWithStats(t, ids), true, false);
    assertGapProfile(plan, Mode.NOOP, bh, bh, 0, 0, 0);
    assertEquals(Arrays.asList("id", "s", "b", "d", "ts"), sourceSql(plan));
    assertEquals(THETA_FIELDS, plan.thetaFieldIds());
    assertFalse(plan.deletesOnly());
    assertWithoutBasesAndReason(plan);

    long s1 = t.delete(f0);
    plan = resolve(loadWithStats(t, ids), true, false);
    assertGapProfile(plan, Mode.FULL, s1, bh, 0, 0, 0);
    assertTrue(plan.deletesOnly());
    assertTrue(plan.metricColumns().isEmpty());
    assertTrue(plan.thetaFieldIds().isEmpty());
    assertWithoutBasesAndReason(plan);
  }

  private static void assertWithoutBasesAndReason(IcebergStatsPlan plan) {
    assertNull(plan.bh());
    assertNull(plan.bn());
    assertTrue(plan.ndvBases().isEmpty());
    assertNull(plan.reason());
    assertTrue(plan.warnings().isEmpty());
  }

  @Test
  public void unusableBaseGivesFullOfAllColumnsWithReason() throws Exception {
    IcebergTestTables t = create("", SCHEMA);
    long bh = t.append(t.file(100, null));
    registerImpalaBlobs(t, bh);
    t.append(t.file(100, null));
    IcebergTable tbl = loadWithStats(t, param(IDS, "1-3:" + bh + ",5:" + bh));

    assertFullWithReason(resolve(tbl, true, false), tbl,
        ReasonKind.COLUMNS_WITHOUT_BASE, Collections.singletonList("d"),
        "no base statistics for column(s) d");
  }

  @Test
  public void hasIncrementalBaseIsFalseForMissingEmptyOrBrokenIds() throws Exception {
    assertFalse(IcebergStatsBaseResolver.hasIncrementalBase(
        table("_missing", Collections.emptyMap())));
    assertFalse(IcebergStatsBaseResolver.hasIncrementalBase(
        table("_empty", param(IDS, ""))));
    String[] broken = {"garbage", "1:x", "1-2-3:4", "1:2:3", "0-100001:5"};
    for (int i = 0; i < broken.length; ++i) {
      assertFalse(broken[i], IcebergStatsBaseResolver.hasIncrementalBase(
          table("_broken" + i, param(IDS, broken[i]))));
    }
  }
}
