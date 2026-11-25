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
import static org.apache.impala.service.IcebergStatsOpFixture.SCHEMA;
import static org.apache.impala.service.IcebergStatsOpFixture.idsOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StatisticsFile;
import org.apache.impala.analysis.IcebergAnalysisFixture;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.iceberg.IcebergTestTables;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableType;
import org.apache.impala.thrift.TAlterTableUpdateStatsParams;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlQueryOptions;
import org.apache.impala.thrift.TDdlType;
import org.apache.impala.thrift.TIcebergStatsMode;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.util.DebugUtils;
import org.apache.impala.util.IcebergUtil.ComputeStatsSnapshotPropertyConverter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import com.google.common.collect.ImmutableMap;

/**
 * An incremental update whose base does not match HMS, run through the catalogd DDL
 * path on a table registered in HMS: the cached table is reloaded after the snapshot
 * ids lose the columns of the request, and a failed reload keeps the original error.
 */
public class IcebergIncrementalCatalogOpTest extends FrontendTestBase {
  private static final String DB = "iceberg_incremental_catalog_op";
  private static final String IDS = IcebergTable.COMPUTE_STATS_SNAPSHOT_IDS;
  private static final List<String> REQUEST_COLS = Arrays.asList("i", "s");
  private static final List<String> OTHER_COLS = Arrays.asList("b", "d", "userid");
  /**
   * Table statistics in HMS before the failed request, whose own row count and
   * statistics time differ.
   */
  private static final Map<String, String> OLD_TABLE_STATS = ImmutableMap.of(
      StatsSetupConst.ROW_COUNT, "7", HdfsTable.TBL_PROP_LAST_COMPUTE_STATS_TIME, "1000");

  @Rule public TemporaryFolder tmp = new TemporaryFolder();
  @Rule public TestName testName = new TestName();

  private static final IcebergAnalysisFixture ifx =
      IcebergAnalysisFixture.create(feFixture_);

  @Test
  public void baseMismatchReloadsCachedTable() throws Exception {
    IcebergTestTables t = newTable();
    long b = fullBase(t);
    TDdlExecRequest req = mismatchRequest(t, b);
    long version = cached(t).getCatalogVersion();

    assertBaseMismatch(t, req);

    IcebergTable tbl = cached(t);
    assertTrue(tbl.getCatalogVersion() > version);
    assertEquals(idsOf(OTHER_COLS, b), cachedIds(tbl));
    assertEquals(idsOf(OTHER_COLS, b), hmsIds(t));
    assertEquals(OLD_TABLE_STATS, hmsTableStats(t));
  }

  @Test
  public void failedReloadKeepsVersionAndOriginalError() throws Exception {
    IcebergTestTables t = newTable();
    long b = fullBase(t);
    TDdlExecRequest req = mismatchRequest(t, b);
    req.getQuery_options().setDebug_action(DebugUtils.LOAD_METADATA_DELAY
        + ":EXCEPTION@validationexception@injected reload failure");
    long version = cached(t).getCatalogVersion();

    assertBaseMismatch(t, req);

    IcebergTable tbl = cached(t);
    assertEquals(version, tbl.getCatalogVersion());
    assertEquals(idsOf(OTHER_COLS, b), cachedIds(tbl));
    assertEquals(idsOf(OTHER_COLS, b), hmsIds(t));
  }

  private IcebergTestTables newTable() throws Exception {
    IcebergTestTables t = IcebergTestTables.create(tmp, DB,
        testName.getMethodName().toLowerCase(), SCHEMA, PartitionSpec.unpartitioned(), 2);
    t.append(t.file(100, null));
    ifx.registerPathA(t, Collections.emptyMap());
    return t;
  }

  /**
   * FULL statistics of all columns at the current snapshot, then a new snapshot and an
   * HMS NDV of "i" that differs from its blob. Returns the snapshot of the statistics.
   */
  private static long fullBase(IcebergTestTables t) throws Exception {
    long b = t.table().currentSnapshot().snapshotId();
    ifx.reloadPathA(t);
    ifx.updateStats(IcebergStatsOpFixture.params(tableName(t), TIcebergStatsMode.FULL,
        b, COLUMNS, -1));
    ifx.setTableParams(t, OLD_TABLE_STATS);
    t.append(t.file(100, null));
    ifx.setHmsColumnNdv(t, "i", 9);
    IcebergTable tbl = ifx.reloadPathA(t);
    assertEquals(idsOf(COLUMNS, b), cachedIds(tbl));
    return b;
  }

  private static TDdlExecRequest mismatchRequest(IcebergTestTables t, long b) {
    long s1 = t.table().currentSnapshot().snapshotId();
    StatisticsFile baseFile = t.table().statisticsFiles().stream()
        .filter(f -> f.snapshotId() == b).findFirst().get();
    TAlterTableUpdateStatsParams params = IcebergStatsOpFixture.incremental(
        tableName(t), s1, b, baseFile.path(), REQUEST_COLS);
    TAlterTableParams alter = new TAlterTableParams();
    alter.setAlter_type(TAlterTableType.UPDATE_STATS);
    alter.setTable_name(params.getTable_name());
    alter.setUpdate_stats_params(params);
    TDdlExecRequest req = new TDdlExecRequest();
    req.setDdl_type(TDdlType.ALTER_TABLE);
    req.setAlter_table_params(alter);
    req.setQuery_options(new TDdlQueryOptions());
    return req;
  }

  private static void assertBaseMismatch(IcebergTestTables t, TDdlExecRequest req)
      throws Exception {
    try {
      ifx.execDdl(req);
      fail("no base mismatch");
    } catch (ImpalaException e) {
      assertEquals(IcebergStatsBaseMismatchException.class, e.getClass());
      assertEquals("Base statistics of column(s) i of table " + t.db() + "." + t.name()
          + " do not match the Hive Metastore; run COMPUTE STATS.", e.getMessage());
    }
  }

  private static TTableName tableName(IcebergTestTables t) {
    return new TTableName(t.db(), t.name());
  }

  private static IcebergTable cached(IcebergTestTables t) {
    Table tbl = ifx.catalog().getTableNoThrow(t.db(), t.name());
    assertTrue(String.valueOf(tbl), tbl instanceof IcebergTable);
    return (IcebergTable) tbl;
  }

  private static Map<Integer, Long> cachedIds(IcebergTable tbl) {
    return ComputeStatsSnapshotPropertyConverter.stringToMap(
        tbl.getMetaStoreTable().getParameters().get(IDS));
  }

  /** The values in HMS of the keys of OLD_TABLE_STATS. */
  private static Map<String, String> hmsTableStats(IcebergTestTables t)
      throws Exception {
    try (MetaStoreClient client = ifx.catalog().getMetaStoreClient()) {
      Map<String, String> params =
          client.getHiveClient().getTable(t.db(), t.name()).getParameters();
      Map<String, String> res = new HashMap<>();
      for (String key : OLD_TABLE_STATS.keySet()) res.put(key, params.get(key));
      return res;
    }
  }

  private static Map<Integer, Long> hmsIds(IcebergTestTables t) throws Exception {
    try (MetaStoreClient client = ifx.catalog().getMetaStoreClient()) {
      return ComputeStatsSnapshotPropertyConverter.stringToMap(client.getHiveClient()
          .getTable(t.db(), t.name()).getParameters().get(IDS));
    }
  }
}
