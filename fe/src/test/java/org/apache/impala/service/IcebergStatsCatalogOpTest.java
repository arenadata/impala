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

import static org.apache.impala.service.IcebergStatsOpFixture.COLUMNS;
import static org.apache.impala.service.IcebergStatsOpFixture.NUM_ROWS;
import static org.apache.impala.service.IcebergStatsOpFixture.SCHEMA;
import static org.apache.impala.service.IcebergStatsOpFixture.columnStats;
import static org.apache.impala.service.IcebergStatsOpFixture.fieldId;
import static org.apache.impala.service.IcebergStatsOpFixture.idsOf;
import static org.apache.impala.service.IcebergStatsOpFixture.values;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.util.Pair;
import org.apache.impala.analysis.IcebergAnalysisFixture;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.iceberg.IcebergPuffinStatsWriter;
import org.apache.impala.catalog.iceberg.IcebergTestTables;
import org.apache.impala.catalog.iceberg.PuffinFixtures;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableSetTblPropertiesParams;
import org.apache.impala.thrift.TAlterTableType;
import org.apache.impala.thrift.TAlterTableUpdateStatsParams;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlType;
import org.apache.impala.thrift.TDropStatsParams;
import org.apache.impala.thrift.TIcebergStatsMode;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TTablePropertyType;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.thrift.TTruncateParams;
import org.apache.impala.util.IcebergUtil.ComputeStatsSnapshotPropertyConverter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

/**
 * Statistics DDL of Iceberg tables through CatalogOpExecutor#execDdlRequest on tables
 * registered in the embedded HMS. The update requests are built as the coordinator
 * sends them; sketches come from PuffinFixtures. Oracles: HMS, Table#refresh() and the
 * IcebergTable reloaded from HMS.
 */
public class IcebergStatsCatalogOpTest extends FrontendTestBase {
  private static final String DB = "iceberg_stats_catalog_op";
  private static final String IDS = IcebergTable.COMPUTE_STATS_SNAPSHOT_IDS;
  private static final long LEGACY_NDV = 7;

  @Rule public TemporaryFolder tmp = new TemporaryFolder();
  @Rule public TestName testName = new TestName();

  private static final IcebergAnalysisFixture ifx =
      IcebergAnalysisFixture.create(feFixture_);

  private String tableName() { return testName.getMethodName().toLowerCase(); }

  /** Registered table with two snapshots. */
  private IcebergTestTables newTable() throws Exception {
    IcebergTestTables t = IcebergTestTables.create(tmp, DB, tableName(), SCHEMA,
        PartitionSpec.unpartitioned(), 2);
    t.append(t.file(100, null));
    t.append(t.file(100, null));
    ifx.registerPathA(t, Collections.emptyMap());
    return t;
  }

  private static TAlterTableUpdateStatsParams params(String table,
      TIcebergStatsMode mode, long s1, List<String> cols, long legacyNdv) {
    return IcebergStatsOpFixture.params(new TTableName(DB, table), mode, s1, cols,
        legacyNdv);
  }

  private static void updateStats(TAlterTableUpdateStatsParams params)
      throws Exception {
    ifx.updateStats(params);
  }

  private static void dropStats(IcebergTestTables t) throws Exception {
    TDdlExecRequest req = new TDdlExecRequest();
    req.setDdl_type(TDdlType.DROP_STATS);
    req.setDrop_stats_params(new TDropStatsParams(new TTableName(t.db(), t.name())));
    ifx.execDdl(req);
  }

  private static void truncateDeletingStats(IcebergTestTables t) throws Exception {
    TTruncateParams params = new TTruncateParams(new TTableName(t.db(), t.name()), false);
    params.setDelete_stats(true);
    TDdlExecRequest req = new TDdlExecRequest();
    req.setDdl_type(TDdlType.TRUNCATE_TABLE);
    req.setTruncate_params(params);
    ifx.execDdl(req);
  }

  /** "Incremental stats" of SHOW TABLE STATS with Puffin writing enabled. */
  private static String incrementalStats(IcebergTable tbl) {
    TBackendGflags cfg = BackendConfig.INSTANCE.getBackendCfg();
    boolean saved = cfg.isEnable_writing_puffin_stats();
    cfg.setEnable_writing_puffin_stats(true);
    try {
      TResultSet rs = FeIcebergTable.Utils.getTableStats(tbl);
      List<String> names = rs.getSchema().getColumns().stream()
          .map(org.apache.impala.thrift.TColumn::getColumnName)
          .collect(Collectors.toList());
      return rs.getRows().get(0).getColVals().get(names.indexOf("Incremental stats"))
          .getString_val();
    } finally {
      cfg.setEnable_writing_puffin_stats(saved);
    }
  }

  private static org.apache.hadoop.hive.metastore.api.Table hmsTable(
      IcebergTestTables t) throws Exception {
    try (MetaStoreClient client = ifx.catalog().getMetaStoreClient()) {
      return client.getHiveClient().getTable(t.db(), t.name());
    }
  }

  private static Map<String, ColumnStatisticsObj> hmsStats(IcebergTestTables t)
      throws Exception {
    try (MetaStoreClient client = ifx.catalog().getMetaStoreClient()) {
      return MetastoreShim.getTableColumnStatistics(client.getHiveClient(), t.db(),
          t.name(), COLUMNS).stream()
          .collect(Collectors.toMap(ColumnStatisticsObj::getColName, o -> o));
    }
  }

  private static long hmsNdv(ColumnStatisticsObj obj) {
    ColumnStatisticsData data = obj.getStatsData();
    if (data.isSetLongStats()) return data.getLongStats().getNumDVs();
    if (data.isSetStringStats()) return data.getStringStats().getNumDVs();
    if (data.isSetDecimalStats()) return data.getDecimalStats().getNumDVs();
    throw new IllegalArgumentException("no NDV in " + data);
  }

  private static Map<Integer, Long> hmsIds(IcebergTestTables t) throws Exception {
    String value = hmsTable(t).getParameters().get(IDS);
    assertTrue("no " + IDS + " in HMS", value != null && !value.isEmpty());
    return ComputeStatsSnapshotPropertyConverter.stringToMap(value);
  }

  /** The only statistics file registered for 's1'. */
  private static StatisticsFile statsFile(IcebergTestTables t, long s1) {
    List<StatisticsFile> files = t.table().statisticsFiles().stream()
        .filter(f -> f.snapshotId() == s1).collect(Collectors.toList());
    assertEquals("statistics files of " + s1, 1, files.size());
    return files.get(0);
  }

  /** Blob data of 'file' keyed by the type, fields, snapshot and properties. */
  private static Map<List<Object>, ByteBuffer> blobContent(IcebergTestTables t,
      StatisticsFile file) throws Exception {
    Map<List<Object>, ByteBuffer> res = new HashMap<>();
    try (PuffinReader reader =
        Puffin.read(t.table().io().newInputFile(file.path())).build()) {
      for (Pair<org.apache.iceberg.puffin.BlobMetadata, ByteBuffer> p
          : reader.readAll(reader.fileMetadata().blobs())) {
        org.apache.iceberg.puffin.BlobMetadata b = p.first();
        byte[] bytes = new byte[p.second().remaining()];
        p.second().duplicate().get(bytes);
        res.put(Arrays.asList(b.type(), b.inputFields(), b.snapshotId(), b.properties()),
            ByteBuffer.wrap(bytes));
      }
    }
    return res;
  }

  /** "ndv" of the Impala blobs registered for 's1', by field id. */
  private static Map<Integer, Long> blobNdvs(IcebergTestTables t, long s1) {
    Map<Integer, Long> res = new TreeMap<>();
    for (BlobMetadata b : statsFile(t, s1).blobMetadata()) {
      assertEquals(s1, b.sourceSnapshotId());
      assertTrue(b.properties().containsKey(IcebergPuffinStatsWriter.IMPALA_CREATED_BY));
      res.put(b.fields().get(0),
          Long.parseLong(b.properties().get(IcebergPuffinStatsWriter.NDV)));
    }
    return res;
  }

  @Test
  public void fullRegistersPuffinAndWritesBlobNdvToHms() throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.table().currentSnapshot().snapshotId();
    ifx.reloadPathA(t);

    updateStats(params(t.name(), TIcebergStatsMode.FULL, s1, COLUMNS, -1));

    Map<Integer, Long> blobNdv = blobNdvs(t, s1);
    assertEquals(COLUMNS.size(), blobNdv.size());
    Map<String, ColumnStatisticsObj> hms = hmsStats(t);
    Map<String, Long> expected = new TreeMap<>();
    expected.put("i", 10L);
    expected.put("s", NUM_ROWS);
    expected.put("d", 10L);
    expected.put("userid", 5L);
    for (Map.Entry<String, Long> e : expected.entrySet()) {
      String col = e.getKey();
      assertEquals(col, (long) e.getValue(), hmsNdv(hms.get(col)));
      assertEquals(col, (long) e.getValue(), (long) blobNdv.get(fieldId(col)));
    }
    assertEquals(2L, (long) blobNdv.get(fieldId("b")));
    assertTrue(hms.get("b").getStatsData().isSetBooleanStats());
    assertEquals(idsOf(COLUMNS, s1), hmsIds(t));
    assertEquals(String.valueOf(NUM_ROWS),
        hmsTable(t).getParameters().get(StatsSetupConst.ROW_COUNT));

    IcebergTable tbl = ifx.reloadPathA(t);
    for (Map.Entry<String, Long> e : expected.entrySet()) {
      assertEquals(e.getKey(), (long) e.getValue(),
          tbl.getColumn(e.getKey()).getStats().getNumDistinctValues());
    }
    assertEquals(idsOf(COLUMNS, s1), ComputeStatsSnapshotPropertyConverter.stringToMap(
        tbl.getMetaStoreTable().getParameters().get(IDS)));
  }

  @Test
  public void cacheReloadedAfterAnalysisKeepsStatsAtS1() throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.table().currentSnapshot().snapshotId();
    long s2 = t.append(t.file(100, null));
    assertEquals(s2, ifx.reloadPathA(t).snapshotId());

    updateStats(params(t.name(), TIcebergStatsMode.FULL, s1, COLUMNS, -1));

    assertEquals(1, t.table().statisticsFiles().size());
    assertEquals(COLUMNS.size(), blobNdvs(t, s1).size());
    assertEquals(idsOf(COLUMNS, s1), hmsIds(t));
  }

  @Test
  public void legacyWritesThriftStatsWithoutPuffin() throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.table().currentSnapshot().snapshotId();
    ifx.reloadPathA(t);

    updateStats(params(t.name(), TIcebergStatsMode.LEGACY, s1, COLUMNS, LEGACY_NDV));

    assertTrue(t.table().statisticsFiles().isEmpty());
    Map<String, ColumnStatisticsObj> hms = hmsStats(t);
    for (String col : Arrays.asList("i", "s", "d", "userid")) {
      assertEquals(col, LEGACY_NDV, hmsNdv(hms.get(col)));
    }
    assertEquals(idsOf(COLUMNS, s1), hmsIds(t));

    // A sample cannot be a base: its columns leave the ids.
    long s2 = t.append(t.file(100, null));
    ifx.reloadPathA(t);
    TAlterTableUpdateStatsParams sampled = params(t.name(), TIcebergStatsMode.LEGACY, s2,
        Arrays.asList("i", "s"), 3);
    sampled.getIceberg_puffin_stats().setSampled(true);
    updateStats(sampled);

    assertEquals(idsOf(Arrays.asList("b", "d", "userid"), s1), hmsIds(t));
    assertEquals(3L, hmsNdv(hmsStats(t).get("s")));
    assertTrue(t.table().statisticsFiles().isEmpty());
  }

  @Test
  public void repeatedFullEqualsSingle() throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.table().currentSnapshot().snapshotId();
    ifx.reloadPathA(t);
    updateStats(params(t.name(), TIcebergStatsMode.FULL, s1, COLUMNS, -1));
    StatisticsFile first = statsFile(t, s1);
    Map<List<Object>, ByteBuffer> firstBlobs = blobContent(t, first);
    Map<String, ColumnStatisticsObj> firstHms = hmsStats(t);
    Map<Integer, Long> firstIds = hmsIds(t);
    String firstRows = hmsTable(t).getParameters().get(StatsSetupConst.ROW_COUNT);

    // A retried RPC carries the same request.
    updateStats(params(t.name(), TIcebergStatsMode.FULL, s1, COLUMNS, -1));

    StatisticsFile second = statsFile(t, s1);
    assertNotEquals(first.path(), second.path());
    assertEquals(1, t.table().statisticsFiles().size());
    assertEquals(first.blobMetadata(), second.blobMetadata());
    assertEquals(COLUMNS.size(), firstBlobs.size());
    assertEquals(firstBlobs, blobContent(t, second));
    assertEquals(firstIds, hmsIds(t));
    assertEquals(firstHms, hmsStats(t));
    assertEquals(String.valueOf(NUM_ROWS), firstRows);
    assertEquals(firstRows,
        hmsTable(t).getParameters().get(StatsSetupConst.ROW_COUNT));
  }

  @Test
  public void statsWithoutPuffinParamsDetachTheirColumns() throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.table().currentSnapshot().snapshotId();
    ifx.reloadPathA(t);
    updateStats(params(t.name(), TIcebergStatsMode.FULL, s1, COLUMNS, -1));
    Map<String, ColumnStatisticsObj> before = hmsStats(t);

    // SET COLUMN STATS sends the stats of one column only.
    TAlterTableUpdateStatsParams setColumn = new TAlterTableUpdateStatsParams();
    setColumn.setTable_name(new TTableName(DB, t.name()));
    setColumn.putToColumn_stats("i", columnStats("i", 42));
    updateStats(setColumn);
    // COMPUTE STATS of a coordinator that sends no Iceberg parameters.
    TAlterTableUpdateStatsParams oldCoordinator = new TAlterTableUpdateStatsParams();
    oldCoordinator.setTable_name(new TTableName(DB, t.name()));
    oldCoordinator.setTable_stats(new TTableStats(99));
    oldCoordinator.putToColumn_stats("s", columnStats("s", 3));
    updateStats(oldCoordinator);

    List<String> untouched = Arrays.asList("b", "d", "userid");
    assertEquals(idsOf(untouched, s1), hmsIds(t));
    Map<String, ColumnStatisticsObj> after = hmsStats(t);
    assertEquals(42L, hmsNdv(after.get("i")));
    assertEquals(3L, hmsNdv(after.get("s")));
    for (String col : untouched) assertEquals(col, before.get(col), after.get(col));
    assertEquals("99", hmsTable(t).getParameters().get(StatsSetupConst.ROW_COUNT));
    assertEquals(COLUMNS.size(), blobNdvs(t, s1).size());
  }

  @Test
  public void computeStatsAfterFailedSetColumnStatsKeepsColumnDetached()
      throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.table().currentSnapshot().snapshotId();
    ifx.reloadPathA(t);
    updateStats(params(t.name(), TIcebergStatsMode.FULL, s1, COLUMNS, -1));
    // SET COLUMN STATS i detached the column and wrote its stats, then the final
    // alter_table failed: HMS moved on, the cached table still has 'i' in the ids.
    List<String> rest = Arrays.asList("s", "b", "d", "userid");
    ifx.setTableParams(t, Collections.singletonMap(IDS,
        ComputeStatsSnapshotPropertyConverter.mapToString(
            new TreeMap<>(idsOf(rest, s1)))));
    ifx.setHmsColumnNdv(t, "i", 42);

    updateStats(params(t.name(), TIcebergStatsMode.LEGACY, s1,
        Collections.singletonList("s"), LEGACY_NDV));

    assertEquals(idsOf(rest, s1), hmsIds(t));
    assertEquals(42L, hmsNdv(hmsStats(t).get("i")));
  }

  @Test
  public void setTblPropertiesAfterFailedSetColumnStatsKeepsColumnDetached()
      throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.table().currentSnapshot().snapshotId();
    ifx.reloadPathA(t);
    updateStats(params(t.name(), TIcebergStatsMode.FULL, s1, COLUMNS, -1));
    try (HmsFailureAfterColumnStats failure =
        HmsFailureAfterColumnStats.failTailAlterTable(ifx.catalog())) {
      setColumnStatsFails(t, failure);
    }
    assertSetTblPropertiesKeepsColumnDetached(t, s1);
  }

  /** The update reached HMS, the reload failed: the cache has the ids before it. */
  @Test
  public void setTblPropertiesAfterFailedReloadKeepsColumnDetached() throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.table().currentSnapshot().snapshotId();
    ifx.reloadPathA(t);
    updateStats(params(t.name(), TIcebergStatsMode.FULL, s1, COLUMNS, -1));
    try (HmsFailureAfterColumnStats failure =
        HmsFailureAfterColumnStats.failReload(ifx.catalog())) {
      setColumnStatsFails(t, failure);
    }
    assertSetTblPropertiesKeepsColumnDetached(t, s1);
  }

  private void setColumnStatsFails(IcebergTestTables t,
      HmsFailureAfterColumnStats failure) throws Exception {
    TAlterTableUpdateStatsParams setColumn = new TAlterTableUpdateStatsParams();
    setColumn.setTable_name(new TTableName(DB, t.name()));
    setColumn.putToColumn_stats("i", columnStats("i", 42));
    try {
      updateStats(setColumn);
      fail("SET COLUMN STATS succeeded");
    } catch (ImpalaException expected) {
      assertEquals(1, failure.failures());
    }
  }

  private void assertSetTblPropertiesKeepsColumnDetached(IcebergTestTables t, long s1)
      throws Exception {
    List<String> rest = Arrays.asList("s", "b", "d", "userid");
    assertEquals(idsOf(rest, s1), hmsIds(t));
    assertEquals(42L, hmsNdv(hmsStats(t).get("i")));

    TAlterTableSetTblPropertiesParams props = new TAlterTableSetTblPropertiesParams();
    props.setTarget(TTablePropertyType.TBL_PROPERTY);
    props.setProperties(Collections.singletonMap("x", "y"));
    TAlterTableParams alter = new TAlterTableParams();
    alter.setAlter_type(TAlterTableType.SET_TBL_PROPERTIES);
    alter.setTable_name(new TTableName(DB, t.name()));
    alter.setSet_tbl_properties_params(props);
    TDdlExecRequest req = new TDdlExecRequest();
    req.setDdl_type(TDdlType.ALTER_TABLE);
    req.setAlter_table_params(alter);
    ifx.execDdl(req);

    assertEquals("y", hmsTable(t).getParameters().get("x"));
    assertEquals(idsOf(rest, s1), hmsIds(t));
  }

  /** TRUNCATE removes the HMS stats but leaves the Puffin blobs of older snapshots. */
  @Test
  public void truncateDeletingStatsKeepsImpalaBlobs() throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.table().currentSnapshot().snapshotId();
    ifx.reloadPathA(t);
    updateStats(params(t.name(), TIcebergStatsMode.FULL, s1, COLUMNS, -1));
    Map<Integer, Long> blobNdv = blobNdvs(t, s1);
    assertEquals("true", incrementalStats(ifx.reloadPathA(t)));

    truncateDeletingStats(t);

    Snapshot current = t.table().currentSnapshot();
    assertNotEquals(s1, current.snapshotId());
    assertEquals("0", current.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
    Map<String, String> hmsParams = hmsTable(t).getParameters();
    assertFalse(hmsParams.containsKey(IDS));
    assertFalse(hmsParams.containsKey(StatsSetupConst.ROW_COUNT));
    assertTrue(hmsStats(t).isEmpty());
    assertEquals(blobNdv, blobNdvs(t, s1));
    assertEquals("false", incrementalStats(ifx.reloadPathA(t)));
  }

  @Test
  public void dropStatsKeepsOnlyForeignBlobs() throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.table().currentSnapshot().snapshotId();
    // Another type than the theta blobs of FULL, so that FULL copies it.
    StatisticsFile foreign = PuffinFixtures.write(t.table(), s1,
        Collections.singletonList(PuffinFixtures.blob(fieldId("i"), s1,
            PuffinFixtures.sketch(SCHEMA.findField("i").type(), values("i")))
            .type("other-engine-blob")));
    t.setStats(s1, foreign);
    ifx.reloadPathA(t);
    updateStats(params(t.name(), TIcebergStatsMode.FULL, s1, COLUMNS, -1));
    assertEquals(COLUMNS.size() + 1,
        t.table().statisticsFiles().get(0).blobMetadata().size());
    assertEquals("true", incrementalStats(ifx.reloadPathA(t)));

    dropStats(t);

    t.table().refresh();
    List<StatisticsFile> files = t.table().statisticsFiles();
    assertEquals(1, files.size());
    assertEquals(s1, files.get(0).snapshotId());
    assertFalse(foreign.path().equals(files.get(0).path()));
    assertEquals(foreign.blobMetadata(), files.get(0).blobMetadata());
    Map<String, String> hmsParams = hmsTable(t).getParameters();
    assertFalse(hmsParams.containsKey(IDS));
    assertFalse(hmsParams.containsKey(StatsSetupConst.ROW_COUNT));
    assertTrue(hmsStats(t).isEmpty());
    assertEquals("false", incrementalStats(ifx.reloadPathA(t)));
  }

  @Test
  public void puffinStatsForNonIcebergTableFail() throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.table().currentSnapshot().snapshotId();
    TAlterTableUpdateStatsParams params =
        params(t.name(), TIcebergStatsMode.FULL, s1, COLUMNS, -1);
    HdfsTable hdfs = new HdfsTable(
        IcebergAnalysisFixture.textMsTable(DB, t.name(), tmp.newFolder()),
        new Db(DB, new Database(DB, "", "", new HashMap<>())), t.name(), "owner");
    try {
      CatalogOpExecutor.checkIcebergPuffinStatsTarget(hdfs, params);
      fail("no error for HDFS table");
    } catch (ImpalaRuntimeException e) {
      assertEquals("Table " + DB + "." + t.name()
          + " was changed concurrently; run COMPUTE STATS again.", e.getMessage());
    }

    CatalogOpExecutor.checkIcebergPuffinStatsTarget(ifx.reloadPathA(t), params);
    params.unsetIceberg_puffin_stats();
    CatalogOpExecutor.checkIcebergPuffinStatsTarget(hdfs, params);
  }
}
