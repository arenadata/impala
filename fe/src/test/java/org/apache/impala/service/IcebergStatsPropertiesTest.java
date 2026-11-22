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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.ComputeStatsStmt;
import org.apache.impala.analysis.IcebergAnalysisFixture;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.iceberg.IcebergTestTables;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableType;
import org.apache.impala.thrift.TAlterTableUpdateStatsParams;
import org.apache.impala.thrift.TColumnStats;
import org.apache.impala.thrift.TComputeStatsParams;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlQueryOptions;
import org.apache.impala.thrift.TDdlType;
import org.apache.impala.thrift.TDropStatsParams;
import org.apache.impala.thrift.TIcebergPuffinStatsParams;
import org.apache.impala.thrift.TIcebergStatsMode;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.thrift.TTruncateParams;
import org.apache.impala.util.IcebergUtil.ComputeStatsSnapshotPropertyConverter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;


/**
 * Runs the catalogd side of COMPUTE STATS, DROP STATS and TRUNCATE on tables registered
 * in the embedded HMS and checks the stats properties left in HMS. COMPUTE STATS is
 * analyzed for real with Puffin writing disabled; the update request the backend would
 * send is built by the test from the analyzed snapshot id and Iceberg statistics
 * parameters, with column names in lower case as the backend reports them. Requests
 * without the Iceberg statistics parameters stand for clients that do not send them.
 */
public class IcebergStatsPropertiesTest extends FrontendTestBase {
  private static final String DB = "iceberg_stats_props";
  private static final int ID_FIELD = 1;
  private static final int S_FIELD = 2;
  private static final int USER_ID_FIELD = 3;
  private static final Schema SCHEMA = new Schema(
      required(ID_FIELD, "id", Types.IntegerType.get()),
      optional(S_FIELD, "s", Types.StringType.get()),
      optional(USER_ID_FIELD, "UserId", Types.StringType.get()));
  private static final long NUM_ROWS = 10;
  private static final long TOTAL_SIZE = 1234;
  private static final String IDS = IcebergTable.COMPUTE_STATS_SNAPSHOT_IDS;
  private static final String LAST_COMPUTE = HdfsTable.TBL_PROP_LAST_COMPUTE_STATS_TIME;

  @Rule public TemporaryFolder tmp = new TemporaryFolder();
  @Rule public TestName testName = new TestName();

  private final IcebergAnalysisFixture ifx = IcebergAnalysisFixture.create(feFixture_);
  private boolean savedWriteFlag_;

  @Before
  public void disablePuffinWriting() {
    savedWriteFlag_ =
        BackendConfig.INSTANCE.getBackendCfg().isEnable_writing_puffin_stats();
    setWriteFlag(false);
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

  /** Registers new table with one snapshot (none if 'withSnapshot' is false). */
  private IcebergTestTables newTable(boolean withSnapshot, String ids) throws Exception {
    IcebergTestTables t = IcebergTestTables.create(tmp, DB, tableName(), SCHEMA,
        PartitionSpec.unpartitioned(), 2);
    if (withSnapshot) t.append(t.file(100, null));
    Map<String, String> params = new HashMap<>();
    if (ids != null) params.put(IDS, ids);
    ifx.registerPathA(t, params);
    return t;
  }

  /**
   * Analyzes COMPUTE STATS on freshly loaded 't'; the snapshot id it passes on is
   * checked against the current snapshot of the table.
   */
  private TComputeStatsParams analyzed(IcebergTestTables t, String columnList)
      throws Exception {
    IcebergTable tbl = ifx.reloadPathA(t);
    TComputeStatsParams params = analyzeComputeStats(
        "compute stats " + t.db() + "." + t.name() + columnList);
    Snapshot current = t.table().currentSnapshot();
    long expected = current == null ? -1 : current.snapshotId();
    assertEquals(expected, tbl.snapshotId());
    assertEquals(expected, params.getIceberg_puffin_stats().getCalc_snapshot_id());
    return params;
  }

  private TComputeStatsParams analyzeComputeStats(String sql) {
    // Without it TABLESAMPLE reads every file of the small test tables.
    TQueryOptions opts = new TQueryOptions();
    opts.setCompute_stats_min_sample_size(0);
    return ((ComputeStatsStmt) AnalyzesOk(sql, feFixture_.createAnalysisCtx(opts)))
        .toThrift();
  }

  /**
   * COMPUTE STATS 't' 'columnList' whose update request carries no Iceberg statistics
   * parameters and the stats of 'columns'.
   */
  private long computeStats(IcebergTestTables t, String columnList, String... columns)
      throws Exception {
    long snapshotId =
        analyzed(t, columnList).getIceberg_puffin_stats().getCalc_snapshot_id();
    execDdl(updateStats(t.db(), t.name(), NUM_ROWS, columns));
    return snapshotId;
  }

  /** COMPUTE STATS 't' 'columnList' in LEGACY mode, as the backend would send it. */
  private long computeLegacyStats(IcebergTestTables t, String columnList)
      throws Exception {
    TDdlExecRequest req = legacyRequest(t, columnList);
    execDdl(req);
    return req.getAlter_table_params().getUpdate_stats_params()
        .getIceberg_puffin_stats().getCalc_snapshot_id();
  }

  private TDdlExecRequest legacyRequest(IcebergTestTables t, String columnList)
      throws Exception {
    TComputeStatsParams analyzed = analyzed(t, columnList);
    TIcebergPuffinStatsParams puffin = analyzed.getIceberg_puffin_stats();
    assertEquals(TIcebergStatsMode.LEGACY, puffin.getMode());
    TDdlExecRequest req = updateStats(t.db(), t.name(), NUM_ROWS,
        puffin.getColumn_field_ids().keySet().toArray(new String[0]));
    req.getAlter_table_params().getUpdate_stats_params().setIceberg_puffin_stats(puffin);
    return req;
  }

  private static TDdlExecRequest updateStats(String db, String name, long numRows,
      String... columns) {
    TAlterTableUpdateStatsParams stats = new TAlterTableUpdateStatsParams();
    stats.setTable_name(new TTableName(db, name));
    TTableStats tableStats = new TTableStats(numRows);
    tableStats.setTotal_file_bytes(TOTAL_SIZE);
    stats.setTable_stats(tableStats);
    if (columns.length > 0) {
      Map<String, TColumnStats> colStats = new HashMap<>();
      for (String col : columns) colStats.put(col, new TColumnStats(1, 1, 5, 0, -1, -1));
      stats.setColumn_stats(colStats);
    }
    TAlterTableParams alter = new TAlterTableParams();
    alter.setAlter_type(TAlterTableType.UPDATE_STATS);
    alter.setTable_name(new TTableName(db, name));
    alter.setUpdate_stats_params(stats);
    TDdlExecRequest req = ddlRequest(TDdlType.ALTER_TABLE);
    req.setAlter_table_params(alter);
    return req;
  }

  private static TDdlExecRequest dropStats(String db, String name) {
    TDdlExecRequest req = ddlRequest(TDdlType.DROP_STATS);
    req.setDrop_stats_params(new TDropStatsParams(new TTableName(db, name)));
    return req;
  }

  private static TDdlExecRequest truncate(String db, String name, boolean deleteStats) {
    TTruncateParams params = new TTruncateParams(new TTableName(db, name), false);
    params.setDelete_stats(deleteStats);
    TDdlExecRequest req = ddlRequest(TDdlType.TRUNCATE_TABLE);
    req.setTruncate_params(params);
    return req;
  }

  private static TDdlExecRequest ddlRequest(TDdlType type) {
    TDdlExecRequest req = new TDdlExecRequest();
    req.setQuery_options(new TDdlQueryOptions());
    req.setDdl_type(type);
    return req;
  }

  private void execDdl(TDdlExecRequest req) throws Exception {
    ifx.catalog().getCatalogOpExecutor().execDdlRequest(req);
  }

  private Map<String, String> hmsParams(String db, String name) throws Exception {
    try (MetaStoreClient client = ifx.catalog().getMetaStoreClient()) {
      return client.getHiveClient().getTable(db, name).getParameters();
    }
  }

  private Map<String, String> hmsParams(IcebergTestTables t) throws Exception {
    return hmsParams(t.db(), t.name());
  }

  private static TreeMap<Integer, Long> idsOf(long snapshotId, int... fieldIds) {
    TreeMap<Integer, Long> ids = new TreeMap<>();
    for (int f : fieldIds) ids.put(f, snapshotId);
    return ids;
  }

  /** The stored ids value must be non-empty and parse into 'expected'. */
  private static void assertIds(Map<Integer, Long> expected, Map<String, String> params) {
    String value = params.get(IDS);
    assertNotNull("no " + IDS + " in " + params, value);
    assertFalse(IDS + " is empty", value.isEmpty());
    assertEquals(value, expected, ComputeStatsSnapshotPropertyConverter.stringToMap(value));
  }

  private static void assertTableStatsWritten(Map<String, String> params) {
    assertEquals(String.valueOf(NUM_ROWS), params.get(StatsSetupConst.ROW_COUNT));
    assertEquals(String.valueOf(TOTAL_SIZE), params.get(StatsSetupConst.TOTAL_SIZE));
    assertNotNull(params.get(LAST_COMPUTE));
  }

  private static void assertStatsDropped(Map<String, String> params) {
    for (String key : Arrays.asList(StatsSetupConst.ROW_COUNT, StatsSetupConst.TOTAL_SIZE,
        LAST_COMPUTE, IDS)) {
      assertNull(key + " left in " + params, params.get(key));
    }
  }

  @Test
  public void fullComputeStatsWritesAllFieldIds() throws Exception {
    IcebergTestTables t = newTable(true, null);
    long snap = computeLegacyStats(t, "");
    Map<String, String> params = hmsParams(t);
    assertTableStatsWritten(params);
    assertIds(idsOf(snap, ID_FIELD, S_FIELD, USER_ID_FIELD), params);
  }

  @Test
  public void columnListUpdatesOnlyListedFieldIds() throws Exception {
    IcebergTestTables t = newTable(true, null);
    long first = computeLegacyStats(t, "");
    t.append(t.file(100, null));
    long second = computeLegacyStats(t, " (id, s)");
    TreeMap<Integer, Long> expected = idsOf(second, ID_FIELD, S_FIELD);
    expected.put(USER_ID_FIELD, first);
    assertIds(expected, hmsParams(t));
  }

  @Test
  public void tableSampleRemovesQueryFieldIds() throws Exception {
    IcebergTestTables t = newTable(true, null);
    long first = computeLegacyStats(t, "");
    // Four files, so that the sample does not cover the whole table.
    for (int i = 0; i < 3; ++i) t.append(t.file(100, null));
    // TABLESAMPLE is rejected at analysis unless stats extrapolation is enabled.
    ifx.setTableParams(t, Collections.singletonMap(
        HdfsTable.TBL_PROP_ENABLE_STATS_EXTRAPOLATION, "true"));
    TDdlExecRequest req =
        legacyRequest(t, " (id, s) tablesample system(10) repeatable(7)");
    assertTrue(req.getAlter_table_params().getUpdate_stats_params()
        .getIceberg_puffin_stats().isSampled());
    execDdl(req);
    Map<String, String> params = hmsParams(t);
    assertTableStatsWritten(params);
    assertIds(idsOf(first, USER_ID_FIELD), params);
  }

  @Test
  public void mixedCaseColumnUsesItsFieldId() throws Exception {
    IcebergTestTables t = newTable(true, null);
    long snap = computeLegacyStats(t, " (UserId)");
    assertIds(idsOf(snap, USER_ID_FIELD), hmsParams(t));
  }

  @Test
  public void emptyColumnListWritesNoIds() throws Exception {
    IcebergTestTables t = newTable(true, null);
    computeStats(t, " ()");
    Map<String, String> params = hmsParams(t);
    assertTableStatsWritten(params);
    assertFalse(IDS + " written: " + params, params.containsKey(IDS));
  }

  @Test
  public void emptyColumnListKeepsPreviousIds() throws Exception {
    IcebergTestTables t = newTable(true, "1-2:5");
    computeStats(t, " ()");
    Map<String, String> params = hmsParams(t);
    assertTableStatsWritten(params);
    assertEquals("1-2:5", params.get(IDS));
  }

  @Test
  public void invalidPreviousIdsReplacedByQueryColumns() throws Exception {
    IcebergTestTables t = newTable(true, null);
    List<String> invalid = Arrays.asList("abc", "1:x", "1-2-3:5", ",", "",
        "1-200000:5", "1-60000:5,70001-130000:5", "1-2147483647:5");
    for (String ids : invalid) {
      ifx.setTableParams(t, Collections.singletonMap(IDS, ids));
      long snap = computeLegacyStats(t, " (id)");
      assertIds(idsOf(snap, ID_FIELD), hmsParams(t));
    }
  }

  @Test
  public void previousIdsOverLimitAfterMergeReplacedByQueryColumns() throws Exception {
    // Valid alone (99 999 ids), but 100 002 once merged with the columns 1-3.
    IcebergTestTables t = newTable(true, "1000-100998:5");
    long snap = computeLegacyStats(t, "");
    assertIds(idsOf(snap, ID_FIELD, S_FIELD, USER_ID_FIELD), hmsParams(t));
  }

  @Test
  public void columnDroppedAfterAnalysisWritesNothing() throws Exception {
    IcebergTestTables t = newTable(true, null);
    TDdlExecRequest req = legacyRequest(t, " (id, s)");
    t.table().updateSchema().deleteColumn("s").commit();
    Map<String, String> before = hmsParams(t);
    try {
      execDdl(req);
      fail("stats of dropped column written");
    } catch (ImpalaRuntimeException e) {
      assertEquals("Column s of table " + t.db() + "." + t.name()
          + " was changed concurrently; run COMPUTE STATS again.", e.getMessage());
    }
    assertEquals(before, hmsParams(t));
  }

  @Test
  public void statsWithoutIcebergParamsDetachFieldIds() throws Exception {
    IcebergTestTables t = newTable(true, null);
    long snap = computeLegacyStats(t, "");
    computeStats(t, " (id)", "id");
    Map<String, String> params = hmsParams(t);
    assertTableStatsWritten(params);
    assertIds(idsOf(snap, S_FIELD, USER_ID_FIELD), params);
  }

  @Test
  public void unknownColumnOnlyWritesNoIds() throws Exception {
    IcebergTestTables t = newTable(true, null);
    computeStats(t, " (id)", "dropped_col");
    Map<String, String> params = hmsParams(t);
    assertTableStatsWritten(params);
    assertFalse(IDS + " written: " + params, params.containsKey(IDS));
  }

  @Test
  public void tableWithoutSnapshotsWritesMinusOne() throws Exception {
    IcebergTestTables t = newTable(false, null);
    long snap = computeLegacyStats(t, "");
    assertEquals(-1, snap);
    assertIds(idsOf(-1, ID_FIELD, S_FIELD, USER_ID_FIELD), hmsParams(t));
  }

  @Test
  public void dropStatsTwiceRemovesProperties() throws Exception {
    IcebergTestTables t = newTable(true, null);
    computeLegacyStats(t, "");
    assertNotNull(hmsParams(t).get(IDS));
    execDdl(dropStats(t.db(), t.name()));
    assertStatsDropped(hmsParams(t));
    execDdl(dropStats(t.db(), t.name()));
    assertStatsDropped(hmsParams(t));
  }

  @Test
  public void truncateWithDeleteStatsRemovesProperties() throws Exception {
    IcebergTestTables t = newTable(true, null);
    computeLegacyStats(t, "");
    assertNotNull(hmsParams(t).get(IDS));
    execDdl(truncate(t.db(), t.name(), true));
    assertStatsDropped(hmsParams(t));
  }

  @Test
  public void truncateWithoutDeleteStatsKeepsProperties() throws Exception {
    IcebergTestTables t = newTable(true, null);
    long snap = computeLegacyStats(t, "");
    Map<String, String> before = hmsParams(t);
    execDdl(truncate(t.db(), t.name(), false));
    Map<String, String> params = hmsParams(t);
    assertTableStatsWritten(params);
    assertEquals(before.get(LAST_COMPUTE), params.get(LAST_COMPUTE));
    assertIds(idsOf(snap, ID_FIELD, S_FIELD, USER_ID_FIELD), params);
  }

  @Test
  public void hdfsTableGetsNoIdsAndLosesComputeTime() throws Exception {
    String name = tableName();
    ifx.registerHdfsPathA(DB, name, tmp.newFolder());
    TComputeStatsParams analyzed = analyzeComputeStats("compute stats " + DB + "." + name);
    assertFalse(analyzed.isSetIceberg_puffin_stats());

    execDdl(updateStats(DB, name, NUM_ROWS, "id"));
    Map<String, String> params = hmsParams(DB, name);
    assertTableStatsWritten(params);
    assertFalse(IDS + " written: " + params, params.containsKey(IDS));
    execDdl(dropStats(DB, name));
    assertStatsDropped(hmsParams(DB, name));
    execDdl(dropStats(DB, name));
    assertStatsDropped(hmsParams(DB, name));

    execDdl(updateStats(DB, name, NUM_ROWS, "id"));
    assertTableStatsWritten(hmsParams(DB, name));
    execDdl(truncate(DB, name, true));
    assertStatsDropped(hmsParams(DB, name));
  }

  /**
   * Creates synchronized HiveCatalog table with one snapshot and sets both stats keys
   * the way COMPUTE STATS would: it cannot run here, as the catalog service id it needs
   * is not set in the test JVM.
   */
  private org.apache.iceberg.Table newHiveCatalogTable() throws Exception {
    org.apache.iceberg.Table iceTbl = ifx.createHiveCatalogTable(DB, tableName(), SCHEMA);
    ifx.appendDataFile(iceTbl);
    iceTbl.updateProperties()
        .set(LAST_COMPUTE, "1700000000")
        .set(IDS, ComputeStatsSnapshotPropertyConverter.mapToString(
            idsOf(iceTbl.currentSnapshot().snapshotId(), ID_FIELD, S_FIELD)))
        .commit();
    IcebergTable loaded = ifx.loadHiveCatalogTable(DB, tableName());
    assertTrue(IDS + " not loaded", loaded.getMetaStoreTable().getParameters()
        .containsKey(IDS));
    return iceTbl;
  }

  private static Map<String, String> refreshedProperties(org.apache.iceberg.Table t) {
    t.refresh();
    return t.properties();
  }

  /** The loader reads the keys from HMS parameters, not from Iceberg properties. */
  private void assertStatsKeysAbsentInHms() throws Exception {
    Map<String, String> params = hmsParams(DB, tableName());
    assertNull(params.toString(), params.get(IDS));
    assertNull(params.toString(), params.get(LAST_COMPUTE));
  }

  @Test
  public void hiveCatalogDropStatsRemovesBothKeys() throws Exception {
    org.apache.iceberg.Table iceTbl = newHiveCatalogTable();
    assertNotNull(refreshedProperties(iceTbl).get(IDS));
    assertNotNull(iceTbl.properties().get(LAST_COMPUTE));
    execDdl(dropStats(DB, tableName()));
    Map<String, String> props = refreshedProperties(iceTbl);
    assertNull(props.toString(), props.get(IDS));
    assertNull(props.toString(), props.get(LAST_COMPUTE));
    assertStatsKeysAbsentInHms();
    execDdl(dropStats(DB, tableName()));
    assertNull(refreshedProperties(iceTbl).get(IDS));
    assertStatsKeysAbsentInHms();
  }

  @Test
  public void hiveCatalogTruncateWithDeleteStatsRemovesBothKeys() throws Exception {
    org.apache.iceberg.Table iceTbl = newHiveCatalogTable();
    execDdl(truncate(DB, tableName(), true));
    Map<String, String> props = refreshedProperties(iceTbl);
    assertNull(props.toString(), props.get(IDS));
    assertNull(props.toString(), props.get(LAST_COMPUTE));
    assertStatsKeysAbsentInHms();
    assertEquals("0", iceTbl.currentSnapshot().summary().get("total-records"));
  }

  @Test
  public void hiveCatalogTruncateWithoutDeleteStatsKeepsBothKeys() throws Exception {
    org.apache.iceberg.Table iceTbl = newHiveCatalogTable();
    execDdl(truncate(DB, tableName(), false));
    Map<String, String> props = refreshedProperties(iceTbl);
    assertNotNull(props.toString(), props.get(IDS));
    assertNotNull(props.toString(), props.get(LAST_COMPUTE));
  }
}
