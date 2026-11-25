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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.IcebergAnalysisFixture;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.iceberg.IcebergTestTables;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.thrift.TColumnStats;
import org.apache.impala.util.NoOpEventSequence;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

/**
 * CatalogHmsStatsWriter against the embedded HMS. The alter function stands in for
 * CatalogOpExecutor#applyAlterTable: it records its argument and calls alter_table.
 */
public class CatalogHmsStatsWriterTest extends FrontendTestBase {
  private static final String DB = "catalog_hms_stats_writer";
  private static final Schema SCHEMA = new Schema(
      optional(1, "i", Types.IntegerType.get()),
      optional(2, "UserId", Types.StringType.get()));

  @Rule public TemporaryFolder tmp = new TemporaryFolder();
  @Rule public TestName testName = new TestName();

  private static final IcebergAnalysisFixture ifx =
      IcebergAnalysisFixture.create(feFixture_);

  private final List<org.apache.hadoop.hive.metastore.api.Table> altered_ =
      new ArrayList<>();
  private final CatalogHmsStatsWriter writer_ = new CatalogHmsStatsWriter(
      ifx.catalog(), NoOpEventSequence.INSTANCE, this::alterTable);

  private void alterTable(org.apache.hadoop.hive.metastore.api.Table msTbl)
      throws ImpalaException {
    altered_.add(msTbl.deepCopy());
    try (MetaStoreClient client = ifx.catalog().getMetaStoreClient()) {
      client.getHiveClient().alter_table(msTbl.getDbName(), msTbl.getTableName(), msTbl);
    } catch (Exception e) {
      throw new ImpalaRuntimeException("alter_table failed", e);
    }
  }

  private String tableName() { return testName.getMethodName().toLowerCase(); }

  private static org.apache.hadoop.hive.metastore.api.Table hmsTable(String name)
      throws Exception {
    try (MetaStoreClient client = ifx.catalog().getMetaStoreClient()) {
      return client.getHiveClient().getTable(DB, name);
    }
  }

  private static String metadataLocation(org.apache.iceberg.Table table) {
    table.refresh();
    return ((HasTableOperations) table).operations().current().metadataFileLocation();
  }

  private static ColumnStatisticsObj statsObj(String col, String type, long ndv) {
    TColumnStats stats = new TColumnStats();
    stats.setNum_distinct_values(ndv);
    stats.setNum_nulls(0);
    stats.setAvg_size(4);
    stats.setMax_size(4);
    stats.setNum_trues(-1);
    stats.setNum_falses(-1);
    return new ColumnStatisticsObj(col, type,
        ColumnStats.createHiveColStatsData(-1, stats, Type.parseColumnType(type)));
  }

  private static ColumnStatistics colStats(String table, ColumnStatisticsObj... objs) {
    ColumnStatistics stats = MetastoreShim.createNewHiveColStats();
    stats.setStatsDesc(new ColumnStatisticsDesc(true, DB, table));
    for (ColumnStatisticsObj obj : objs) stats.addToStatsObj(obj);
    return stats;
  }

  @Test
  public void hiveCatalogTableGetsMetadataLocationFromHms() throws Exception {
    org.apache.iceberg.Table iceTbl =
        ifx.createHiveCatalogTable(DB, tableName(), SCHEMA, false);
    org.apache.hadoop.hive.metastore.api.Table stale = hmsTable(tableName());
    String staleLocation = stale.getParameters().get(IcebergTable.METADATA_LOCATION);
    ifx.appendDataFile(iceTbl);
    String current = metadataLocation(iceTbl);
    assertNotEquals(staleLocation, current);
    stale.putToParameters("impala.test.key", "v");

    writer_.alterTableProperties(stale);

    org.apache.hadoop.hive.metastore.api.Table hms = hmsTable(tableName());
    assertEquals(current, hms.getParameters().get(IcebergTable.METADATA_LOCATION));
    assertEquals(staleLocation,
        hms.getParameters().get(IcebergTable.PREVIOUS_METADATA_LOCATION));
    assertEquals("v", hms.getParameters().get("impala.test.key"));
    assertEquals(current, stale.getParameters().get(IcebergTable.METADATA_LOCATION));
    assertEquals(1, altered_.size());
    // The Iceberg commit survives the write.
    assertEquals(current, metadataLocation(iceTbl));
  }

  @Test
  public void previousLocationMissingInHmsIsRemoved() throws Exception {
    ifx.createHiveCatalogTable(DB, tableName(), SCHEMA, false);
    org.apache.hadoop.hive.metastore.api.Table msTbl = hmsTable(tableName());
    assertFalse(msTbl.getParameters()
        .containsKey(IcebergTable.PREVIOUS_METADATA_LOCATION));
    msTbl.putToParameters(IcebergTable.PREVIOUS_METADATA_LOCATION, "file:/stale.json");

    writer_.alterTableProperties(msTbl);

    assertFalse(msTbl.getParameters()
        .containsKey(IcebergTable.PREVIOUS_METADATA_LOCATION));
    assertFalse(hmsTable(tableName()).getParameters()
        .containsKey(IcebergTable.PREVIOUS_METADATA_LOCATION));
  }

  @Test
  public void readTableParametersReturnsHmsNotArgument() throws Exception {
    ifx.createHiveCatalogTable(DB, tableName(), SCHEMA, false);
    org.apache.hadoop.hive.metastore.api.Table cached = hmsTable(tableName());
    org.apache.hadoop.hive.metastore.api.Table stored = cached.deepCopy();
    stored.putToParameters("impala.test.key", "hms");
    alterTable(stored);
    cached.putToParameters("impala.test.key", "cache");

    assertEquals("hms", writer_.readTableParameters(cached).get("impala.test.key"));
    assertEquals("cache", cached.getParameters().get("impala.test.key"));
  }

  @Test
  public void getTableFailureSkipsAlterTable() throws Exception {
    ifx.createHiveCatalogTable(DB, tableName(), SCHEMA, false);
    org.apache.hadoop.hive.metastore.api.Table msTbl = hmsTable(tableName());
    msTbl.setTableName(tableName() + "_missing");
    try {
      writer_.alterTableProperties(msTbl);
      fail("no error for missing table");
    } catch (ImpalaRuntimeException e) {
      assertEquals("Error making 'getTable' RPC to Hive Metastore: ", e.getMessage());
      assertNotNull(e.getCause());
    }
    assertTrue(altered_.isEmpty());
  }

  @Test
  public void nonHiveCatalogTableIsAlteredAsIs() throws Exception {
    IcebergTestTables t = IcebergTestTables.create(tmp, DB, tableName(), SCHEMA,
        PartitionSpec.unpartitioned(), 2);
    org.apache.hadoop.hive.metastore.api.Table msTbl =
        t.msTable(Collections.singletonMap(IcebergTable.METADATA_LOCATION, "file:/x"));
    ImpalaRuntimeException alterFailure = new ImpalaRuntimeException("alter failed");
    CatalogHmsStatsWriter writer = new CatalogHmsStatsWriter(ifx.catalog(),
        NoOpEventSequence.INSTANCE, m -> {
          altered_.add(m.deepCopy());
          throw alterFailure;
        });
    try {
      writer.alterTableProperties(msTbl);
      fail("no error from alter function");
    } catch (ImpalaRuntimeException e) {
      assertSame(alterFailure, e);
    }
    assertEquals(1, altered_.size());
    assertEquals("file:/x",
        altered_.get(0).getParameters().get(IcebergTable.METADATA_LOCATION));
  }

  @Test
  public void columnStatsRoundTripByLowerCaseName() throws Exception {
    IcebergTestTables t = IcebergTestTables.create(tmp, DB, tableName(), SCHEMA,
        PartitionSpec.unpartitioned(), 2);
    ifx.registerPathA(t, Collections.emptyMap());
    org.apache.hadoop.hive.metastore.api.Table msTbl = hmsTable(tableName());

    writer_.updateColumnStats(colStats(tableName(), statsObj("userid", "string", 5)));

    Map<String, ColumnStatisticsObj> stats =
        writer_.readColumnStats(msTbl, Arrays.asList("UserId", "i"));
    assertEquals(Collections.singleton("userid"), stats.keySet());
    assertEquals(5, stats.get("userid").getStatsData().getStringStats().getNumDVs());
  }

  @Test
  public void updateColumnStatsOfMissingTableFails() throws Exception {
    try {
      writer_.updateColumnStats(
          colStats(tableName() + "_missing", statsObj("i", "int", 3)));
      fail("no error for missing table");
    } catch (ImpalaRuntimeException e) {
      assertEquals("Error making 'updateTableColumnStatistics' RPC to Hive Metastore: ",
          e.getMessage());
      assertNotNull(e.getCause());
    }
  }
}
