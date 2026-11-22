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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.IcebergAnalysisFixture;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventPropertyKey;
import org.apache.impala.catalog.iceberg.IcebergPuffinStatsWriter;
import org.apache.impala.catalog.iceberg.PuffinFixtures;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.thrift.TAlterTableUpdateStatsParams;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlType;
import org.apache.impala.thrift.TDropStatsParams;
import org.apache.impala.thrift.TIcebergStatsMode;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.util.IcebergUtil.ComputeStatsSnapshotPropertyConverter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Statistics DDL of HiveCatalog Iceberg tables through CatalogOpExecutor#execDdlRequest.
 * A synchronized table keeps the snapshot ids in the Iceberg properties, which its
 * commits copy to HMS. An external table without purge keeps them only in HMS, so every
 * Iceberg commit of the statistics must be followed by an HMS write that carries the new
 * metadata location: a stale one would roll the commit back.
 */
public class IcebergStatsHiveCatalogOpTest extends FrontendTestBase {
  private static final String DB = "iceberg_stats_hive_catalog_op";
  private static final String IDS = IcebergTable.COMPUTE_STATS_SNAPSHOT_IDS;
  private static final Set<String> COLUMNS =
      new HashSet<>(IcebergStatsOpFixture.COLUMNS);

  @Rule public TestName testName = new TestName();

  private static final IcebergAnalysisFixture ifx =
      IcebergAnalysisFixture.create(feFixture_);

  private String tableName() { return testName.getMethodName().toLowerCase(); }

  /** Table with two snapshots, loaded by the catalog. */
  private org.apache.iceberg.Table newTable(boolean purge) throws Exception {
    org.apache.iceberg.Table iceTbl = ifx.createHiveCatalogTable(DB, tableName(),
        IcebergStatsOpFixture.SCHEMA, purge);
    ifx.appendDataFile(iceTbl);
    ifx.appendDataFile(iceTbl);
    ifx.loadHiveCatalogTable(DB, tableName());
    return iceTbl;
  }

  private void full(long s1) throws Exception {
    ifx.updateStats(IcebergStatsOpFixture.params(new TTableName(DB, tableName()),
        TIcebergStatsMode.FULL, s1, IcebergStatsOpFixture.COLUMNS, -1));
  }

  private void dropStats() throws Exception {
    TDdlExecRequest req = new TDdlExecRequest();
    req.setDdl_type(TDdlType.DROP_STATS);
    req.setDrop_stats_params(new TDropStatsParams(new TTableName(DB, tableName())));
    ifx.execDdl(req);
  }

  private Map<String, String> hmsParams() throws Exception {
    try (MetaStoreClient client = ifx.catalog().getMetaStoreClient()) {
      return client.getHiveClient().getTable(DB, tableName()).getParameters();
    }
  }

  private Set<String> hmsStatsColumns() throws Exception {
    try (MetaStoreClient client = ifx.catalog().getMetaStoreClient()) {
      return MetastoreShim.getTableColumnStatistics(client.getHiveClient(), DB,
          tableName(), IcebergStatsOpFixture.COLUMNS).stream()
          .map(ColumnStatisticsObj::getColName).collect(Collectors.toSet());
    }
  }

  private Map<Integer, Long> hmsIds() throws Exception {
    String value = hmsParams().get(IDS);
    assertNotNull("no " + IDS + " in HMS", value);
    return ComputeStatsSnapshotPropertyConverter.stringToMap(value);
  }

  private static Map<Integer, Long> allIds(long s1) {
    return IcebergStatsOpFixture.idsOf(IcebergStatsOpFixture.COLUMNS, s1);
  }

  /** Snapshot of every Impala blob registered in the refreshed table, by field id. */
  private static Map<Integer, Long> impalaBlobs(org.apache.iceberg.Table iceTbl) {
    iceTbl.refresh();
    Map<Integer, Long> res = new TreeMap<>();
    for (StatisticsFile file : iceTbl.statisticsFiles()) {
      for (BlobMetadata b : file.blobMetadata()) {
        if (b.properties().containsKey(IcebergPuffinStatsWriter.IMPALA_CREATED_BY)) {
          assertEquals(b.sourceSnapshotId(), file.snapshotId());
          res.put(b.fields().get(0), b.sourceSnapshotId());
        }
      }
    }
    return res;
  }

  /**
   * Name of the newest metadata file in the table location. It is found on disk, as the
   * catalog takes the current metadata from HMS, which is what the test checks.
   */
  private static String newestMetadataFile(org.apache.iceberg.Table iceTbl) {
    File dir = new File(new Path(iceTbl.location(), "metadata").toUri().getPath());
    String[] names = dir.list((d, name) -> name.endsWith(".metadata.json"));
    assertTrue("no metadata files in " + dir, names != null && names.length > 0);
    // Commits of the HiveCatalog number their files with a zero-padded version.
    return Arrays.stream(names).max(String::compareTo).get();
  }

  /** The metadata location in HMS is the one of the last Iceberg commit. */
  private String assertHmsHasNewestMetadata(org.apache.iceberg.Table iceTbl)
      throws Exception {
    String newest = newestMetadataFile(iceTbl);
    assertEquals(newest,
        new Path(hmsParams().get(IcebergTable.METADATA_LOCATION)).getName());
    return newest;
  }

  @Test
  public void synchronizedTableFullThenDropStats() throws Exception {
    org.apache.iceberg.Table iceTbl = newTable(true);
    long s1 = iceTbl.currentSnapshot().snapshotId();
    assertFalse(hmsParams().containsKey(
        MetastoreEventPropertyKey.CATALOG_SERVICE_ID.getKey()));

    full(s1);

    Map<Integer, Long> ids = allIds(s1);
    assertEquals(ids, impalaBlobs(iceTbl));
    assertEquals(ids, ComputeStatsSnapshotPropertyConverter.stringToMap(
        iceTbl.properties().get(IDS)));
    assertEquals(ids, hmsIds());
    assertEquals(COLUMNS, hmsStatsColumns());
    IcebergTable tbl = ifx.loadHiveCatalogTable(DB, tableName());
    assertEquals(ids, ComputeStatsSnapshotPropertyConverter.stringToMap(
        tbl.getMetaStoreTable().getParameters().get(IDS)));
    assertEquals(10, tbl.getColumn("i").getStats().getNumDistinctValues());

    dropStats();

    assertTrue(impalaBlobs(iceTbl).isEmpty());
    assertTrue(iceTbl.statisticsFiles().isEmpty());
    assertFalse(iceTbl.properties().containsKey(IDS));
    assertFalse(hmsParams().containsKey(IDS));
    assertTrue(hmsStatsColumns().isEmpty());
    // numRows is not checked: a synchronized table keeps it after DROP STATS.
  }

  /**
   * SET COLUMN STATS removed the last column from the ids in Iceberg and HMS, then its
   * final Iceberg commit failed: the cache still has the column. COMPUTE STATS of
   * another column must not bring it back.
   */
  @Test
  public void synchronizedTableComputeStatsAfterFailedSetColumnStats() throws Exception {
    org.apache.iceberg.Table iceTbl = newTable(true);
    long s1 = iceTbl.currentSnapshot().snapshotId();
    ifx.updateStats(IcebergStatsOpFixture.params(new TTableName(DB, tableName()),
        TIcebergStatsMode.FULL, s1, Collections.singletonList("i"), -1));
    IcebergTable cached = ifx.loadHiveCatalogTable(DB, tableName());
    assertEquals(IcebergStatsOpFixture.idsOf(Collections.singletonList("i"), s1),
        ComputeStatsSnapshotPropertyConverter.stringToMap(
            cached.getMetaStoreTable().getParameters().get(IDS)));
    iceTbl.refresh();
    iceTbl.updateProperties().remove(IDS).commit();
    assertFalse(hmsParams().containsKey(IDS));

    ifx.updateStats(IcebergStatsOpFixture.params(new TTableName(DB, tableName()),
        TIcebergStatsMode.LEGACY, s1, Collections.singletonList("s"), 3));

    Map<Integer, Long> expected =
        IcebergStatsOpFixture.idsOf(Collections.singletonList("s"), s1);
    iceTbl.refresh();
    assertEquals(expected, ComputeStatsSnapshotPropertyConverter.stringToMap(
        iceTbl.properties().get(IDS)));
    assertEquals(expected, hmsIds());
  }

  @Test
  public void externalTableFullThenDropStats() throws Exception {
    org.apache.iceberg.Table iceTbl = newTable(false);
    long s1 = iceTbl.currentSnapshot().snapshotId();

    full(s1);

    Map<Integer, Long> ids = allIds(s1);
    String afterFull = assertHmsHasNewestMetadata(iceTbl);
    assertEquals(ids, impalaBlobs(iceTbl));
    assertFalse(iceTbl.properties().containsKey(IDS));
    assertEquals(ids, hmsIds());
    assertEquals(COLUMNS, hmsStatsColumns());

    dropStats();

    assertNotEquals(afterFull, assertHmsHasNewestMetadata(iceTbl));
    assertTrue(impalaBlobs(iceTbl).isEmpty());
    assertTrue(iceTbl.statisticsFiles().isEmpty());
    Map<String, String> hms = hmsParams();
    assertFalse(hms.containsKey(IDS));
    assertFalse(hms.containsKey(StatsSetupConst.ROW_COUNT));
    assertTrue(hmsStatsColumns().isEmpty());
  }

  /** DROP STATS commits the removal of the blobs while the ids are already gone. */
  @Test
  public void externalTableDropStatsAfterIdsDetached() throws Exception {
    org.apache.iceberg.Table iceTbl = newTable(false);
    long s1 = iceTbl.currentSnapshot().snapshotId();
    full(s1);
    // COMPUTE STATS of a coordinator that sends no Iceberg parameters.
    TAlterTableUpdateStatsParams oldCoordinator = new TAlterTableUpdateStatsParams();
    oldCoordinator.setTable_name(new TTableName(DB, tableName()));
    for (String col : IcebergStatsOpFixture.COLUMNS) {
      oldCoordinator.putToColumn_stats(col,
          IcebergStatsOpFixture.columnStats(col, 3));
    }
    ifx.updateStats(oldCoordinator);

    assertFalse(hmsParams().containsKey(IDS));
    String beforeDrop = assertHmsHasNewestMetadata(iceTbl);
    assertEquals(allIds(s1), impalaBlobs(iceTbl));

    dropStats();

    assertNotEquals(beforeDrop, assertHmsHasNewestMetadata(iceTbl));
    assertTrue(impalaBlobs(iceTbl).isEmpty());
    assertTrue(iceTbl.statisticsFiles().isEmpty());
    Map<String, String> hms = hmsParams();
    assertFalse(hms.containsKey(IDS));
    assertFalse(hms.containsKey(StatsSetupConst.ROW_COUNT));
    assertTrue(hmsStatsColumns().isEmpty());
  }

  /**
   * HMS has no ids and the cache misses the Iceberg commit that removed the last Impala
   * blob, as after a failed HMS write of an earlier DROP STATS. The repeated DROP STATS
   * has nothing to commit and must not point HMS back at the stale cached metadata.
   */
  @Test
  public void externalTableDropStatsAgainAfterHmsFailure() throws Exception {
    org.apache.iceberg.Table iceTbl = newTable(false);
    long s1 = iceTbl.currentSnapshot().snapshotId();
    ifx.loadHiveCatalogTable(DB, tableName());
    StatisticsFile file = PuffinFixtures.write(iceTbl, s1, Collections.singletonList(
        PuffinFixtures.blob(1, s1, PuffinFixtures.sketch(Types.IntegerType.get(),
            Arrays.asList(1, 2, 3))).impalaCreatedBy("Impala hive-catalog-op-test")));
    iceTbl.updateStatistics().setStatistics(file).commit();
    String afterRegistration = assertHmsHasNewestMetadata(iceTbl);
    iceTbl.updateStatistics().removeStatistics(s1).commit();
    String afterRemoval = assertHmsHasNewestMetadata(iceTbl);
    assertNotEquals(afterRegistration, afterRemoval);
    assertFalse(hmsParams().containsKey(IDS));

    dropStats();

    assertEquals(afterRemoval, assertHmsHasNewestMetadata(iceTbl));
    assertTrue(impalaBlobs(iceTbl).isEmpty());
    assertFalse(hmsParams().containsKey(IDS));
  }

  /**
   * The cache misses a registration of Impala blobs whose HMS write failed. SET COLUMN
   * STATS of a column outside the ids has nothing to commit and must not point HMS back
   * at the metadata without the registration.
   */
  @Test
  public void externalTableSetColumnStatsKeepsUncachedRegistration() throws Exception {
    org.apache.iceberg.Table iceTbl = newTable(false);
    long s1 = iceTbl.currentSnapshot().snapshotId();
    ifx.loadHiveCatalogTable(DB, tableName());
    StatisticsFile file = PuffinFixtures.write(iceTbl, s1, Collections.singletonList(
        PuffinFixtures.blob(1, s1, PuffinFixtures.sketch(Types.IntegerType.get(),
            Arrays.asList(1, 2, 3))).impalaCreatedBy("Impala hive-catalog-op-test")));
    iceTbl.updateStatistics().setStatistics(file).commit();
    assertHmsHasNewestMetadata(iceTbl);
    TAlterTableUpdateStatsParams setColumnStats = new TAlterTableUpdateStatsParams();
    setColumnStats.setTable_name(new TTableName(DB, tableName()));
    setColumnStats.putToColumn_stats("s", IcebergStatsOpFixture.columnStats("s", 3));

    ifx.updateStats(setColumnStats);

    assertHmsHasNewestMetadata(iceTbl);
    assertEquals(Collections.singletonMap(1, s1), impalaBlobs(iceTbl));
    assertEquals(Collections.singleton("s"), hmsStatsColumns());
  }

  @Test
  public void externalTableTableStatsOnlyKeepsUncachedRegistration() throws Exception {
    org.apache.iceberg.Table iceTbl = newTable(false);
    long s1 = iceTbl.currentSnapshot().snapshotId();
    ifx.loadHiveCatalogTable(DB, tableName());
    StatisticsFile file = PuffinFixtures.write(iceTbl, s1, Collections.singletonList(
        PuffinFixtures.blob(1, s1, PuffinFixtures.sketch(Types.IntegerType.get(),
            Arrays.asList(1, 2, 3))).impalaCreatedBy("Impala hive-catalog-op-test")));
    iceTbl.updateStatistics().setStatistics(file).commit();
    assertHmsHasNewestMetadata(iceTbl);
    TAlterTableUpdateStatsParams tableStats = new TAlterTableUpdateStatsParams();
    tableStats.setTable_name(new TTableName(DB, tableName()));
    tableStats.setTable_stats(new TTableStats(5));

    ifx.updateStats(tableStats);

    assertHmsHasNewestMetadata(iceTbl);
    assertEquals(Collections.singletonMap(1, s1), impalaBlobs(iceTbl));
  }
}
