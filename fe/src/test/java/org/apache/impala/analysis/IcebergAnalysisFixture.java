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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.IncompleteTable;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.iceberg.IcebergHiveCatalog;
import org.apache.impala.catalog.iceberg.IcebergTestTables;
import org.apache.impala.common.FrontendFixture;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.testutil.CatalogServiceTestCatalog;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableType;
import org.apache.impala.thrift.TAlterTableUpdateStatsParams;
import org.apache.impala.thrift.TColumnStats;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlQueryOptions;
import org.apache.impala.thrift.TDdlType;
import org.apache.impala.util.IcebergSchemaConverter;
import org.apache.impala.util.NoOpEventSequence;

/**
 * Puts IcebergTestTables tables into the catalog of a FrontendFixture.
 * Path B: the table is loaded directly and added to the catalog cache, without HMS.
 * Path A: the table is registered in the embedded HMS of the test JVM and loaded by the
 * catalog; HMS changes made here become visible only after reloadPathA().
 * Catalog resets drop path B tables.
 */
public class IcebergAnalysisFixture {
  private final FrontendFixture fx_;
  /** Locations of the HMS databases this fixture creates; removed at JVM exit. */
  private final File dbRoot_;

  private IcebergAnalysisFixture(FrontendFixture fx, File dbRoot) {
    fx_ = fx;
    dbRoot_ = dbRoot;
  }

  public static IcebergAnalysisFixture create(FrontendFixture fx) {
    File dbRoot;
    try {
      dbRoot = Files.createTempDirectory("iceberg-analysis-fixture").toFile();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    Runtime.getRuntime().addShutdownHook(
        new Thread(() -> FileUtils.deleteQuietly(dbRoot)));
    return new IcebergAnalysisFixture(fx, dbRoot);
  }

  public CatalogServiceTestCatalog catalog() {
    return (CatalogServiceTestCatalog) fx_.catalog().getSrcCatalog();
  }

  public IcebergTable addPathB(IcebergTestTables t, Map<String, String> params)
      throws Exception {
    Db db = fx_.catalog().getDb(t.db());
    if (db == null) db = fx_.addTestDb(t.db(), null);
    org.apache.hadoop.hive.metastore.api.Table msTbl = t.msTable(params);
    // HMS sets the access type in getTable() responses; the analyzer rejects tables
    // without one.
    msTbl.setAccessType(Analyzer.ACCESSTYPE_READWRITE);
    IcebergTable tbl = (IcebergTable) Table.fromMetastoreTable(db, msTbl);
    // Table loading tolerates a table missing in HMS: column stats lookups fail silently.
    try (MetaStoreClient client = catalog().getMetaStoreClient()) {
      tbl.load(false, client.getHiveClient(), msTbl, "test", NoOpEventSequence.INSTANCE);
    }
    // The cache keeps an entry of the same catalog version, so a reload must replace it.
    db.removeTable(tbl.getName());
    db.addTable(tbl);
    return tbl;
  }

  /**
   * HMS parameters of statistics with snapshot ids 'ids' computed after every commit so
   * far: NOOP of COMPUTE INCREMENTAL STATS needs the current snapshot to be older.
   */
  public static Map<String, String> computedAfterNow(String ids) {
    Map<String, String> res = new HashMap<>();
    res.put(IcebergTable.COMPUTE_STATS_SNAPSHOT_IDS, ids);
    res.put(Table.TBL_PROP_LAST_COMPUTE_STATS_TIME,
        Long.toString(System.currentTimeMillis() / 1000 + 1));
    return res;
  }

  /**
   * Waits until the second of 'millis' is over, so that later commits and COMPUTE STATS,
   * whose time has only seconds, are not in that second.
   */
  public static void awaitSecondAfter(long millis) throws InterruptedException {
    long next = (millis / 1000 + 1) * 1000;
    for (long now = System.currentTimeMillis(); now < next;
         now = System.currentTimeMillis()) {
      Thread.sleep(next - now);
    }
  }

  public void registerPathA(IcebergTestTables t, Map<String, String> params)
      throws Exception {
    try (MetaStoreClient client = catalog().getMetaStoreClient()) {
      ensureHmsDb(client.getHiveClient(), t.db());
      client.getHiveClient().createTable(t.msTable(params));
    }
  }

  /** Keys mapped to null are removed, the rest are set. */
  public void setTableParams(IcebergTestTables t, Map<String, String> params)
      throws Exception {
    try (MetaStoreClient client = catalog().getMetaStoreClient()) {
      IMetaStoreClient hms = client.getHiveClient();
      org.apache.hadoop.hive.metastore.api.Table msTbl = hms.getTable(t.db(), t.name());
      for (Map.Entry<String, String> e : params.entrySet()) {
        if (e.getValue() == null) {
          msTbl.getParameters().remove(e.getKey());
        } else {
          msTbl.getParameters().put(e.getKey(), e.getValue());
        }
      }
      hms.alter_table(t.db(), t.name(), msTbl);
    }
  }

  public void setHmsColumnNdv(IcebergTestTables t, String col, long ndv)
      throws Exception {
    FieldSchema field = null;
    for (FieldSchema f : IcebergSchemaConverter.convertToHiveSchema(t.table().schema())) {
      if (f.getName().equalsIgnoreCase(col)) field = f;
    }
    assertNotNull("no column " + col + " in " + t.db() + "." + t.name(), field);
    Type type = Type.parseColumnType(field.getType());
    TColumnStats stats = new TColumnStats();
    stats.setNum_distinct_values(ndv);
    stats.setNum_nulls(0);
    stats.setAvg_size(1);
    stats.setMax_size(1);
    stats.setNum_trues(-1);
    stats.setNum_falses(-1);
    ColumnStatisticsData data = ColumnStats.createHiveColStatsData(-1, stats, type);
    assertNotNull("HMS has no stats for type " + type, data);
    ColumnStatistics colStats = MetastoreShim.createNewHiveColStats();
    colStats.setStatsDesc(new ColumnStatisticsDesc(true, t.db(), t.name()));
    colStats.addToStatsObj(
        new ColumnStatisticsObj(field.getName(), field.getType(), data));
    try (MetaStoreClient client = catalog().getMetaStoreClient()) {
      client.getHiveClient().updateTableColumnStatistics(colStats);
    }
  }

  /** Runs 'req' on the catalogd of the fixture, with default DDL query options. */
  public void execDdl(TDdlExecRequest req) throws Exception {
    if (!req.isSetQuery_options()) req.setQuery_options(new TDdlQueryOptions());
    catalog().getCatalogOpExecutor().execDdlRequest(req);
  }

  /** Sends 'params' as the ALTER TABLE UPDATE_STATS request of a coordinator. */
  public void updateStats(TAlterTableUpdateStatsParams params) throws Exception {
    TAlterTableParams alter = new TAlterTableParams();
    alter.setAlter_type(TAlterTableType.UPDATE_STATS);
    alter.setTable_name(params.getTable_name());
    alter.setUpdate_stats_params(params);
    TDdlExecRequest req = new TDdlExecRequest();
    req.setDdl_type(TDdlType.ALTER_TABLE);
    req.setAlter_table_params(alter);
    execDdl(req);
  }

  /** Every call loads the table anew from the current HMS state. */
  public IcebergTable reloadPathA(IcebergTestTables t) throws Exception {
    Table tbl = resetAndLoad(t.db(), t.name());
    assertTrue("not an Iceberg table: " + tbl.getClass().getName(),
        tbl instanceof IcebergTable);
    return (IcebergTable) tbl;
  }

  /** Unpartitioned external TEXTFILE table with column 'id INT' over 'dir'. */
  public HdfsTable registerHdfsPathA(String db, String name, File dir) throws Exception {
    org.apache.hadoop.hive.metastore.api.Table msTbl = textMsTable(db, name, dir);
    try (MetaStoreClient client = catalog().getMetaStoreClient()) {
      ensureHmsDb(client.getHiveClient(), db);
      client.getHiveClient().createTable(msTbl);
    }
    Table tbl = resetAndLoad(db, name);
    assertTrue("not an HDFS table: " + tbl.getClass().getName(), tbl instanceof HdfsTable);
    return (HdfsTable) tbl;
  }

  /** HMS table of registerHdfsPathA(), not registered anywhere. */
  public static org.apache.hadoop.hive.metastore.api.Table textMsTable(String db,
      String name, File dir) {
    SerDeInfo serde = new SerDeInfo();
    serde.setSerializationLib(HdfsFileFormat.TEXT.serializationLib());
    serde.setParameters(new HashMap<>());
    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(Collections.singletonList(new FieldSchema("id", "int", null)));
    sd.setLocation(dir.toURI().toString());
    sd.setInputFormat(HdfsFileFormat.TEXT.inputFormat());
    sd.setOutputFormat(HdfsFileFormat.TEXT.outputFormat());
    sd.setSerdeInfo(serde);
    sd.setCompressed(false);
    sd.setNumBuckets(-1);
    sd.setBucketCols(new ArrayList<>());
    sd.setSortCols(new ArrayList<>());
    sd.setParameters(new HashMap<>());
    org.apache.hadoop.hive.metastore.api.Table msTbl =
        new org.apache.hadoop.hive.metastore.api.Table();
    msTbl.setDbName(db);
    msTbl.setTableName(name);
    msTbl.setOwner(System.getProperty("user.name"));
    msTbl.setTableType(TableType.EXTERNAL_TABLE.toString());
    msTbl.setPartitionKeys(new ArrayList<>());
    Map<String, String> params = new HashMap<>();
    params.put("EXTERNAL", "TRUE");
    msTbl.setParameters(params);
    msTbl.setSd(sd);
    return msTbl;
  }

  /**
   * Synchronized (purge on drop) Iceberg table of the HiveCatalog.
   */
  public org.apache.iceberg.Table createHiveCatalogTable(String db, String name,
      Schema schema) throws Exception {
    return createHiveCatalogTable(db, name, schema, true);
  }

  /** External Iceberg table of the HiveCatalog; 'purge' sets external.table.purge. */
  public org.apache.iceberg.Table createHiveCatalogTable(String db, String name,
      Schema schema, boolean purge) throws Exception {
    try (MetaStoreClient client = catalog().getMetaStoreClient()) {
      ensureHmsDb(client.getHiveClient(), db);
    }
    Map<String, String> props = new HashMap<>();
    // IcebergHiveCatalog sets purge by default.
    if (!purge) props.put(Table.TBL_PROP_EXTERNAL_TABLE_PURGE, "false");
    return IcebergHiveCatalog.getInstance().createTable(TableIdentifier.of(db, name),
        schema, PartitionSpec.unpartitioned(), null, props);
  }

  /**
   * Synchronized Iceberg table of the HiveCatalog with 'spec' and 'formatVersion',
   * wrapped for the builders of IcebergTestTables.
   */
  public IcebergTestTables createHiveCatalogTestTable(String db, String name,
      Schema schema, PartitionSpec spec, int formatVersion) throws Exception {
    try (MetaStoreClient client = catalog().getMetaStoreClient()) {
      ensureHmsDb(client.getHiveClient(), db);
    }
    Map<String, String> props = new HashMap<>();
    props.put(TableProperties.FORMAT_VERSION, Integer.toString(formatVersion));
    org.apache.iceberg.Table table = IcebergHiveCatalog.getInstance().createTable(
        TableIdentifier.of(db, name), schema, spec, null, props);
    return IcebergTestTables.of(db, name, table);
  }

  /**
   * Appends one 100-byte Parquet data file under the location of 'iceTbl' as a new
   * snapshot and returns its id.
   */
  public long appendDataFile(org.apache.iceberg.Table iceTbl) {
    return IcebergTestTables.append(iceTbl,
        IcebergTestTables.placeholderFile(iceTbl, 100, null, FileFormat.PARQUET));
  }

  public ComputeStatsStmt analyzeComputeStats(String sql) {
    return analyzeComputeStats(sql, fx_.createAnalysisCtx());
  }

  public ComputeStatsStmt analyzeComputeStats(String sql, AnalysisContext ctx) {
    ParseNode stmt = fx_.analyzeStmt(sql, ctx, null, false);
    assertTrue("not COMPUTE STATS: " + sql, stmt instanceof ComputeStatsStmt);
    return (ComputeStatsStmt) stmt;
  }

  /** Loads a table made by createHiveCatalogTable() anew from the current HMS state. */
  public IcebergTable loadHiveCatalogTable(String db, String name) throws Exception {
    Table tbl = resetAndLoad(db, name);
    assertTrue("not an Iceberg table: " + tbl.getClass().getName(),
        tbl instanceof IcebergTable);
    return (IcebergTable) tbl;
  }

  private Table resetAndLoad(String db, String name) throws Exception {
    fx_.catalog().reset();
    Table tbl = fx_.catalog().getOrLoadTable(db, name);
    assertNotNull("no table " + db + "." + name + " in HMS", tbl);
    if (tbl instanceof IncompleteTable) {
      throw new AssertionError("failed to load " + db + "." + name,
          ((IncompleteTable) tbl).getCause());
    }
    return tbl;
  }

  private void ensureHmsDb(IMetaStoreClient hms, String db) throws Exception {
    try {
      hms.getDatabase(db);
    } catch (NoSuchObjectException e) {
      String location = new Path(new File(dbRoot_, db + ".db").toURI()).toString();
      hms.createDatabase(new Database(db, "", location, new HashMap<>()));
    }
  }
}
