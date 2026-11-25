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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.iceberg.IcebergPuffinBlobReader;
import org.apache.impala.catalog.iceberg.IcebergPuffinStatsWriter;
import org.apache.impala.catalog.iceberg.IcebergTestTables;
import org.apache.impala.catalog.iceberg.PuffinFixtures;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.FakeHmsStatsWriter.Method;
import org.apache.impala.thrift.TAlterTableUpdateStatsParams;
import org.apache.impala.thrift.TColumnStats;
import org.apache.impala.thrift.TIcebergNdvBase;
import org.apache.impala.thrift.TIcebergPuffinStatsParams;
import org.apache.impala.thrift.TIcebergStatsMode;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.util.IcebergUtil;
import org.apache.impala.util.IcebergUtil.ComputeStatsSnapshotPropertyConverter;
import org.apache.impala.util.NoOpEventSequence;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

/**
 * Fixture of the IcebergStatsUpdater#apply tests: tables, parameters as ComputeStatsStmt
 * and the backend build them, and the oracles over the table metadata and the calls
 * recorded by FakeHmsStatsWriter.
 */
public abstract class IcebergStatsUpdaterTestBase extends FrontendTestBase {
  protected static final String DB = "iceberg_stats_updater_write";
  protected static final String IDS = IcebergTable.COMPUTE_STATS_SNAPSHOT_IDS;
  protected static final String VERSION = "4.5.0-updater-test";
  protected static final Schema SCHEMA = new Schema(
      optional(1, "i", Types.IntegerType.get()),
      optional(2, "s", Types.StringType.get()),
      optional(3, "b", Types.BooleanType.get()),
      optional(4, "bin", Types.BinaryType.get()),
      optional(5, "d", Types.DecimalType.of(1, 0)));
  protected static final List<String> COLUMNS = Arrays.asList("i", "s", "b", "bin", "d");
  protected static final long LEGACY_NDV = 7;
  /** Values whose sketches incremental requests send as the gap. */
  protected static final Map<String, List<?>> GAP = new HashMap<>();
  static {
    GAP.put("i", Arrays.asList(6, 7, 8, 9, 10, 11, 12, 13, 14, 15));
    GAP.put("s", Arrays.asList("a", "x", "y"));
    GAP.put("b", Collections.singletonList(true));
    GAP.put("d", Collections.singletonList(BigDecimal.valueOf(5)));
  }

  @Rule public TemporaryFolder tmp = new TemporaryFolder();
  @Rule public TestName testName = new TestName();

  private String savedVersion_;
  protected int loads_;
  /** Runs after each load, so the loaded table does not see its changes. */
  protected Runnable afterLoad_;
  private IcebergStatsUpdater updater_;
  protected final FakeHmsStatsWriter hms_ = new FakeHmsStatsWriter();

  @Before
  public void setBuildVersion() {
    savedVersion_ = BackendConfig.INSTANCE.getBackendCfg().getImpala_build_version();
    BackendConfig.INSTANCE.getBackendCfg().setImpala_build_version(VERSION);
    updater_ = new IcebergStatsUpdater(t -> {
      ++loads_;
      org.apache.iceberg.Table loaded = IcebergUtil.loadTable(t);
      if (afterLoad_ != null) afterLoad_.run();
      return loaded;
    });
  }

  @After
  public void restoreBuildVersion() {
    BackendConfig.INSTANCE.getBackendCfg().setImpala_build_version(savedVersion_);
  }

  protected IcebergTestTables create() {
    return IcebergTestTables.create(tmp, DB, testName.getMethodName().toLowerCase(),
        SCHEMA, PartitionSpec.unpartitioned(), 2);
  }

  protected static List<?> values(String col) {
    switch (col) {
      case "i": return Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
      case "s": return Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "j", "k",
          "l", "m");
      case "b": return Arrays.asList(true, false);
      // More distinct values than DECIMAL(1,0) allows: HMS caps the NDV at 10.
      case "d": {
        List<BigDecimal> res = new ArrayList<>();
        for (int v = 10; v <= 30; ++v) res.add(BigDecimal.valueOf(v));
        return res;
      }
      default: throw new IllegalArgumentException(col);
    }
  }

  protected static byte[] sketchBytes(String col) {
    Type type = SCHEMA.findField(col).type();
    return PuffinFixtures.sketch(type, values(col)).toByteArray();
  }

  protected static byte[] sketch(String col, List<?> values) {
    return PuffinFixtures.sketch(SCHEMA.findField(col).type(), values).toByteArray();
  }

  protected static int fieldId(String col) { return SCHEMA.findField(col).fieldId(); }

  /**
   * Parameters as ComputeStatsStmt and the backend build them for 'cols'. FULL sends a
   * sketch for every column but BINARY.
   */
  protected static TAlterTableUpdateStatsParams params(TIcebergStatsMode mode, long s1,
      long numRows, List<String> cols) {
    TAlterTableUpdateStatsParams params = new TAlterTableUpdateStatsParams();
    params.setTable_name(new TTableName(DB, "t"));
    params.setTable_stats(new TTableStats(numRows));
    TIcebergPuffinStatsParams p =
        new TIcebergPuffinStatsParams(mode, s1, new HashMap<>());
    for (String col : cols) {
      TColumnStats stats = new TColumnStats();
      stats.setNum_distinct_values(mode == TIcebergStatsMode.LEGACY ? LEGACY_NDV : -1);
      stats.setNum_nulls(1);
      stats.setMax_size(col.equals("s") || col.equals("bin") ? 3 : 4);
      stats.setAvg_size(2.5);
      stats.setNum_trues(col.equals("b") ? 4 : -1);
      stats.setNum_falses(col.equals("b") ? 3 : -1);
      params.putToColumn_stats(col, stats);
      p.putToColumn_field_ids(col, fieldId(col));
      if (mode == TIcebergStatsMode.FULL && !col.equals("bin")) {
        p.addToTheta_field_ids(fieldId(col));
        p.putToTheta_sketches(fieldId(col), ByteBuffer.wrap(sketchBytes(col)));
      }
    }
    params.setIceberg_puffin_stats(p);
    return params;
  }

  protected static TAlterTableUpdateStatsParams params(TIcebergStatsMode mode, long s1,
      long numRows) {
    return params(mode, s1, numRows, COLUMNS);
  }

  protected long apply(IcebergTable tbl, org.apache.hadoop.hive.metastore.api.Table msTbl,
      TAlterTableUpdateStatsParams params) throws ImpalaException {
    return apply(updater_, tbl, msTbl, params);
  }

  protected long apply(IcebergStatsUpdater updater, IcebergTable tbl,
      org.apache.hadoop.hive.metastore.api.Table msTbl,
      TAlterTableUpdateStatsParams params) throws ImpalaException {
    tbl.writeLock().lock();
    try {
      return updater.apply(tbl, msTbl, params, hms_, NoOpEventSequence.INSTANCE);
    } finally {
      tbl.releaseWriteLock();
    }
  }

  protected void detach(IcebergTable tbl,
      org.apache.hadoop.hive.metastore.api.Table msTbl, List<String> cols)
      throws ImpalaException {
    tbl.writeLock().lock();
    try {
      updater_.detachColumns(tbl, msTbl, cols, hms_);
    } finally {
      tbl.releaseWriteLock();
    }
  }

  protected void drop(IcebergTable tbl, org.apache.hadoop.hive.metastore.api.Table msTbl)
      throws ImpalaException {
    tbl.writeLock().lock();
    try {
      updater_.dropImpalaStatistics(tbl, msTbl, hms_);
    } finally {
      tbl.releaseWriteLock();
    }
  }

  protected long apply(IcebergTable tbl, TAlterTableUpdateStatsParams params)
      throws ImpalaException {
    return apply(tbl, tbl.getMetaStoreTable().deepCopy(), params);
  }

  protected static org.apache.hadoop.hive.metastore.api.Table integrated(
      IcebergTable tbl) {
    org.apache.hadoop.hive.metastore.api.Table msTbl = tbl.getMetaStoreTable().deepCopy();
    msTbl.setTableType(TableType.EXTERNAL_TABLE.toString());
    msTbl.putToParameters("EXTERNAL", "TRUE");
    msTbl.putToParameters(IcebergTable.ICEBERG_CATALOG, "hive.catalog");
    msTbl.putToParameters("external.table.purge", "TRUE");
    return msTbl;
  }

  /** ids written by the last alterTableProperties call. */
  protected Map<Integer, Long> hmsIds() {
    FakeHmsStatsWriter.Call call = hms_.last(Method.ALTER_TABLE_PROPERTIES);
    assertTrue("no alterTableProperties call", call != null);
    return ComputeStatsSnapshotPropertyConverter.stringToMap(
        call.msTbl.getParameters().get(IDS));
  }

  protected static Map<Integer, Long> idsOf(List<String> cols, long snapshotId) {
    Map<Integer, Long> res = new TreeMap<>();
    for (String col : cols) res.put(fieldId(col), snapshotId);
    return res;
  }

  protected Map<String, ColumnStatisticsObj> hmsStats() {
    FakeHmsStatsWriter.Call call = hms_.last(Method.UPDATE_COLUMN_STATS);
    assertTrue("no updateColumnStats call", call != null);
    assertTrue(call.stats.getStatsDesc().isIsTblLevel());
    return call.stats.getStatsObj().stream()
        .collect(Collectors.toMap(ColumnStatisticsObj::getColName, o -> o));
  }

  protected static long hmsNdv(ColumnStatisticsObj obj) {
    ColumnStatisticsData data = obj.getStatsData();
    if (data.isSetLongStats()) return data.getLongStats().getNumDVs();
    if (data.isSetStringStats()) return data.getStringStats().getNumDVs();
    if (data.isSetDecimalStats()) return data.getDecimalStats().getNumDVs();
    throw new IllegalArgumentException("no NDV in " + data);
  }

  protected static StatisticsFile statsFile(Table table, long snapshotId) {
    List<StatisticsFile> files = table.statisticsFiles().stream()
        .filter(f -> f.snapshotId() == snapshotId).collect(Collectors.toList());
    assertEquals("statistics files of " + snapshotId, 1, files.size());
    return files.get(0);
  }

  protected static Map<Integer, BlobMetadata> blobsByField(StatisticsFile file) {
    Map<Integer, BlobMetadata> res = new TreeMap<>();
    for (BlobMetadata b : file.blobMetadata()) {
      assertEquals(1, b.fields().size());
      assertTrue(res.put(b.fields().get(0), b) == null);
    }
    return res;
  }

  /** Checks the Impala blobs of 's1' and returns their "ndv" values by field id. */
  protected static Map<Integer, Long> blobNdvs(Table table, long s1, List<String> cols)
      throws ImpalaException {
    StatisticsFile file = statsFile(table, s1);
    Map<Integer, BlobMetadata> blobs = blobsByField(file);
    Map<Integer, Long> snapshots = new HashMap<>();
    for (String col : cols) snapshots.put(fieldId(col), s1);
    Map<Integer, ByteBuffer> bytes =
        IcebergPuffinBlobReader.readThetaBlobs(table.io(), file.path(), snapshots);
    Map<Integer, Long> ndvs = new TreeMap<>();
    for (String col : cols) {
      BlobMetadata b = blobs.get(fieldId(col));
      assertEquals(StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1, b.type());
      assertEquals(s1, b.sourceSnapshotId());
      assertEquals("Impala " + VERSION,
          b.properties().get(IcebergPuffinStatsWriter.IMPALA_CREATED_BY));
      ByteBuffer blob = bytes.get(fieldId(col));
      byte[] actual = new byte[blob.remaining()];
      blob.duplicate().get(actual);
      assertArrayEquals(col, sketchBytes(col), actual);
      ndvs.put(fieldId(col),
          Long.parseLong(b.properties().get(IcebergPuffinStatsWriter.NDV)));
    }
    assertEquals(cols.size(), blobs.size());
    return ndvs;
  }

  /**
   * Computes statistics of 'mode' at a new snapshot through IcebergStatsUpdater#apply
   * and makes HMS and the cached columns hold what it wrote.
   */
  protected long base(IcebergTestTables t, IcebergTable tbl, TIcebergStatsMode mode,
      List<String> cols) throws Exception {
    long snapshot = t.append(t.file(100, null));
    apply(tbl, params(mode, snapshot, 100, cols));
    syncFromHms(tbl);
    return snapshot;
  }

  protected void syncFromHms(IcebergTable tbl) {
    Map<String, ColumnStatisticsObj> stats = hmsStats();
    hms_.setStoredStats(stats);
    for (ColumnStatisticsObj obj : stats.values()) {
      Column col = tbl.getColumn(obj.getColName());
      assertTrue(col.getStats().update(obj.getColName(), col.getType(),
          obj.getStatsData()));
    }
  }

  protected static void register(IcebergTestTables t, long snapshotId,
      PuffinFixtures.BlobSpec... blobs) {
    t.setStats(snapshotId, PuffinFixtures.write(t.table(), snapshotId,
        Arrays.asList(blobs)));
  }

  protected static TIcebergNdvBase ndvBase(IcebergTestTables t, String col, long x) {
    return new TIcebergNdvBase(fieldId(col), x, statsFile(t.table(), x).path());
  }

  protected static Map<String, TIcebergNdvBase> bases(IcebergTestTables t, long x,
      List<String> cols) {
    Map<String, TIcebergNdvBase> res = new LinkedHashMap<>();
    for (String col : cols) res.put(col, ndvBase(t, col, x));
    return res;
  }

  /**
   * Request as ComputeStatsStmt and the backend build it: gap metrics of 'cols' and gap
   * sketches of GAP for the columns of 'bases', in their order.
   */
  protected static TAlterTableUpdateStatsParams incremental(long s1, long bh,
      long numRows, List<String> cols, Map<String, TIcebergNdvBase> bases) {
    TAlterTableUpdateStatsParams params =
        params(TIcebergStatsMode.INCREMENTAL, s1, numRows, cols);
    TIcebergPuffinStatsParams p = params.getIceberg_puffin_stats();
    p.setMetrics_base_snapshot_id(bh);
    p.setNdv_bases(new ArrayList<>());
    for (String col : cols) {
      boolean fixed = !col.equals("s") && !col.equals("bin");
      TColumnStats gap = params.getColumn_stats().get(col);
      gap.setNum_nulls(2);
      gap.setNum_distinct_values(8);
      gap.setMax_size(fixed ? 4 : 5);
      gap.setAvg_size(4);
      gap.setNum_trues(col.equals("b") ? 6 : -1);
      gap.setNum_falses(col.equals("b") ? 1 : -1);
      TIcebergNdvBase base = bases.get(col);
      if (base == null) continue;
      p.addToNdv_bases(base);
      p.addToTheta_field_ids(fieldId(col));
      p.putToTheta_sketches(fieldId(col), ByteBuffer.wrap(sketch(col, GAP.get(col))));
    }
    return params;
  }
}
