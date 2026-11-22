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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.analysis.ComputeStatsStmt;
import org.apache.impala.analysis.IcebergAnalysisFixture;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.iceberg.IcebergTestTables;
import org.apache.impala.catalog.iceberg.PuffinFixtures;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.thrift.TColumnStats;
import org.apache.impala.thrift.TComputeStatsParams;
import org.apache.impala.thrift.TCounter;
import org.apache.impala.thrift.TIcebergNdvBase;
import org.apache.impala.thrift.TIcebergPuffinStatsParams;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.util.IcebergUtil.ComputeStatsSnapshotPropertyConverter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

/**
 * COMPUTE INCREMENTAL STATS end to end on tables registered in HMS: analysis, the child
 * queries answered by IncrementalResults from the declared values of the scanned files,
 * the catalogd update and a reload. The base statistics come from COMPUTE STATS run the
 * same way, and the merged statistics are compared with COMPUTE STATS on the same data.
 */
public class IcebergIncrementalStatsScenarioTest extends FrontendTestBase {
  private static final String DB = "iceberg_incremental_scenario";
  private static final String IDS = IcebergTable.COMPUTE_STATS_SNAPSHOT_IDS;
  private static final String NO_NEW_FILES =
      "No new data files since the last COMPUTE STATS; statistics are up to date.";
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "p", Types.IntegerType.get()),
      optional(3, "s", Types.StringType.get()));
  /** Iceberg has no TINYINT or SMALLINT: DECIMAL(1,0) stands for a narrow domain. */
  private static final Schema TYPES_SCHEMA = new Schema(
      optional(1, "i", Types.IntegerType.get()),
      optional(2, "d", Types.DecimalType.of(1, 0)),
      optional(3, "m", Types.DecimalType.of(9, 2)),
      optional(4, "b", Types.BooleanType.get()),
      optional(5, "s", Types.StringType.get()),
      optional(6, "ts", Types.TimestampType.withoutZone()));
  private static final long TS_MICROS = 1_700_000_000_000_000L;

  @Rule public TemporaryFolder tmp = new TemporaryFolder();
  @Rule public TestName testName = new TestName();

  private static final IcebergAnalysisFixture ifx =
      IcebergAnalysisFixture.create(feFixture_);

  private boolean savedWriteFlag_;

  @Before
  public void enablePuffinWriting() {
    savedWriteFlag_ =
        BackendConfig.INSTANCE.getBackendCfg().isEnable_writing_puffin_stats();
    BackendConfig.INSTANCE.getBackendCfg().setEnable_writing_puffin_stats(true);
  }

  @After
  public void restorePuffinWriting() {
    BackendConfig.INSTANCE.getBackendCfg().setEnable_writing_puffin_stats(
        savedWriteFlag_);
  }

  /** One analyzed COMPUTE STATS and the answers of its child queries. */
  private static final class Command {
    final TComputeStatsParams params;
    final List<String> warnings;
    final String mode;
    final Map<String, Long> counters;
    /** Null if the statement has no child queries. */
    final IncrementalResults results;

    Command(TComputeStatsParams params, List<String> warnings, String mode,
        Map<String, Long> counters, IncrementalResults results) {
      this.params = params;
      this.warnings = warnings;
      this.mode = mode;
      this.counters = counters;
      this.results = results;
    }

    TIcebergPuffinStatsParams puffin() {
      assertTrue("no iceberg_puffin_stats", params.isSetIceberg_puffin_stats());
      return params.getIceberg_puffin_stats();
    }
  }

  private static Command analyze(IcebergTestTables t, String command) throws Exception {
    TQueryOptions options = new TQueryOptions();
    options.setCompute_column_minmax_stats(true);
    String sql = command + " " + t.db() + "." + t.name();
    ComputeStatsStmt stmt;
    List<String> warnings;
    String mode;
    Map<String, Long> counters = new TreeMap<>();
    try (FrontendProfile.Scope scope = FrontendProfile.createNewWithScope()) {
      AnalysisResult res =
          feFixture_.parseAndAnalyze(sql, feFixture_.createAnalysisCtx(options));
      stmt = (ComputeStatsStmt) res.getStmt();
      warnings = new ArrayList<>(res.getAnalyzer().getWarnings());
      FrontendProfile profile = FrontendProfile.getCurrent();
      mode = profile.getInfoString("Iceberg stats mode");
      List<TCounter> all = profile.emitAsThrift().getCounters();
      if (all != null) {
        for (TCounter c : all) {
          if (c.getName().startsWith("IcebergStats")) {
            counters.put(c.getName(), c.getValue());
          }
        }
      }
    }
    TComputeStatsParams params = stmt.toThrift();
    IncrementalResults results = params.isSetNo_child_queries_message() ? null
        : IncrementalResults.run(feFixture_, stmt, t);
    return new Command(params, warnings, mode, counters, results);
  }

  private static IcebergTable apply(IcebergTestTables t, Command c) throws Exception {
    assertNotNull("no child queries: " + c.params.getNo_child_queries_message(),
        c.results);
    ifx.updateStats(c.results.params());
    return ifx.reloadPathA(t);
  }

  private static IcebergTable compute(IcebergTestTables t, String command,
      String expectedMode) throws Exception {
    Command c = analyze(t, command);
    assertEquals(command, expectedMode, c.mode);
    return apply(t, c);
  }

  private IcebergTestTables hiveTable(String suffix, PartitionSpec spec, Schema schema)
      throws Exception {
    return ifx.createHiveCatalogTestTable(DB, tableName(suffix), schema, spec, 2);
  }

  private String tableName(String suffix) {
    return testName.getMethodName().toLowerCase() + suffix;
  }

  /**
   * Rows of SCHEMA with ids [from, to), partition column 'p' and strings of several
   * lengths; every fifth string is NULL.
   */
  private static Map<String, List<?>> rows(int from, int to, int p) {
    List<Object> ids = new ArrayList<>();
    List<Object> parts = new ArrayList<>();
    List<Object> strings = new ArrayList<>();
    for (int id = from; id < to; ++id) {
      ids.add(id);
      parts.add(p);
      strings.add(id % 5 == 0 ? null : "s" + (id % 9) + "xx".substring(0, id % 3));
    }
    Map<String, List<?>> res = new LinkedHashMap<>();
    res.put("id", ids);
    res.put("p", parts);
    res.put("s", strings);
    return res;
  }

  /** Rows of TYPES_SCHEMA with ids [from, to); every column has NULLs but "i" and "d". */
  private static Map<String, List<?>> typedRows(int from, int to) {
    Map<String, List<Object>> cols = new LinkedHashMap<>();
    for (String c : Arrays.asList("i", "d", "m", "b", "s", "ts")) {
      cols.put(c, new ArrayList<>());
    }
    for (int id = from; id < to; ++id) {
      cols.get("i").add(id);
      cols.get("d").add(BigDecimal.valueOf(id % 19 - 9));
      cols.get("m").add(id % 6 == 0 ? null : BigDecimal.valueOf(id * 125L, 2));
      cols.get("b").add(id % 3 == 0 ? null : id % 2 == 0);
      cols.get("s").add(id % 4 == 0 ? null : "t" + (id % 5) + "yy".substring(0, id % 3));
      cols.get("ts").add(id % 7 == 0 ? null : TS_MICROS + (id % 10) * 3_600_000_000L);
    }
    return new LinkedHashMap<>(cols);
  }

  @Test
  public void incrementalScansGapAndMergesWithBase() throws Exception {
    checkGapAndMerge(hiveTable("_flat", PartitionSpec.unpartitioned(), SCHEMA),
        null, null);
    checkGapAndMerge(hiveTable("_part",
        PartitionSpec.builderFor(SCHEMA).identity("p").build(), SCHEMA), "p=1", "p=2");
  }

  /**
   * COMPUTE STATS, two appends and COMPUTE INCREMENTAL STATS: the column stats query
   * scans just the appended files and carries the sketch tail, the profile counts the
   * same files, and the result equals COMPUTE STATS of the same snapshot.
   */
  private void checkGapAndMerge(IcebergTestTables t, String part1, String part2)
      throws Exception {
    t.append(t.file(100, part1, rows(0, 20, 1)));
    ifx.reloadPathA(t);
    compute(t, "compute stats", "FULL");
    DataFile f2 = t.file(200, part2, rows(15, 41, 2));
    DataFile f3 = t.file(300, part1, rows(35, 60, 1));
    t.append(f2);
    long s1 = t.append(f3);
    ifx.reloadPathA(t);

    Command inc = analyze(t, "compute incremental stats");

    assertEquals("INCREMENTAL", inc.mode);
    assertEquals(Collections.emptyList(), inc.warnings);
    assertEquals(new ArrayList<>(new TreeSet<>(Arrays.asList(f2.location(),
        f3.location()))), inc.results.metricFiles());
    assertEquals(Collections.emptyList(), inc.results.thetaFiles());
    Map<String, Long> gap = new TreeMap<>();
    gap.put("IcebergStatsGapBytes", 500L);
    gap.put("IcebergStatsGapFiles", 2L);
    gap.put("IcebergStatsGapRecords", 51L);
    gap.put("IcebergStatsThetaGapBytes", 500L);
    gap.put("IcebergStatsThetaGapFiles", 2L);
    assertEquals(gap, inc.counters);
    IcebergTable tbl = apply(t, inc);

    assertEquals(idsAt(SCHEMA, s1), hmsIds(t));
    assertEquals(71L, tbl.getNumRows());
    Map<Integer, byte[]> blobs =
        PuffinFixtures.impalaThetaPayloads(t.table(), statsFile(t.table(), s1));
    assertEquals(Arrays.asList(1, 2, 3), new ArrayList<>(blobs.keySet()));
    for (Types.NestedField f : SCHEMA.columns()) {
      double estimate = estimate(blobs.get(f.fieldId()));
      assertEquals(f.name(),
          PuffinFixtures.sketch(f.type(), liveValues(t, f.name())).getEstimate(),
          estimate, 0);
      assertTrue(f.name(), estimate <= tbl.getNumRows());
    }
    assertSameAsFull(t, tbl);
  }

  @Test
  public void secondIncrementalUsesOwnBaseOnHiveCatalog() throws Exception {
    checkOwnBase(hiveTable("", PartitionSpec.unpartitioned(), TYPES_SCHEMA));
  }

  @Test
  public void secondIncrementalUsesOwnBaseOnHadoopCatalog() throws Exception {
    IcebergTestTables t = IcebergTestTables.create(tmp, DB, tableName(""),
        TYPES_SCHEMA, PartitionSpec.unpartitioned(), 2);
    ifx.registerPathA(t, Collections.emptyMap());
    checkOwnBase(t);
  }

  /**
   * COMPUTE STATS, then COMPUTE INCREMENTAL STATS twice with an append before each: the
   * second one takes the result of the first as its base, and repeating its update
   * changes nothing.
   */
  private void checkOwnBase(IcebergTestTables t) throws Exception {
    t.append(t.file(100, null, typedRows(0, 12)));
    ifx.reloadPathA(t);
    compute(t, "compute stats", "FULL");
    long s1a = t.append(t.file(100, null, typedRows(12, 24)));
    ifx.reloadPathA(t);
    IcebergTable tbl = compute(t, "compute incremental stats", "INCREMENTAL");
    assertOwnBase(t, tbl, s1a);
    // 19 distinct values are capped to the 10 a DECIMAL(1,0) column can hold.
    assertEquals(19, estimate(PuffinFixtures.impalaThetaPayloads(t.table(),
        statsFile(t.table(), s1a)).get(2)), 0);
    assertEquals(10, impalaBlobNdv(t.table(), 2, s1a));

    long s1b = t.append(t.file(100, null, typedRows(24, 36)));
    ifx.reloadPathA(t);
    Command inc = analyze(t, "compute incremental stats");

    assertEquals("INCREMENTAL", inc.mode);
    assertEquals(Collections.emptyList(), inc.warnings);
    TIcebergPuffinStatsParams puffin = inc.puffin();
    assertEquals(s1b, puffin.getCalc_snapshot_id());
    assertEquals(s1a, puffin.getMetrics_base_snapshot_id());
    assertEquals(Integer.valueOf(6), puffin.getColumn_field_ids().get("ts"));
    Map<Integer, Long> ndvBases = new TreeMap<>();
    for (TIcebergNdvBase b : puffin.getNdv_bases()) {
      ndvBases.put(b.getField_id(), b.getSnapshot_id());
    }
    assertEquals(idsAt(TYPES_SCHEMA, s1a), ndvBases);
    tbl = apply(t, inc);
    assertOwnBase(t, tbl, s1b);

    State written = State.of(t);
    ifx.updateStats(inc.results.params());
    tbl = ifx.reloadPathA(t);
    assertEquals(written, State.of(t));
    assertOwnBase(t, tbl, s1b);
    assertSameAsFull(t, tbl);
  }

  /**
   * Every column has the snapshot 's1' in the snapshot ids, and its NDV in HMS is the
   * "ndv" of its Impala blob at 's1'; BOOLEAN keeps no NDV in HMS.
   */
  private static void assertOwnBase(IcebergTestTables t, IcebergTable tbl, long s1)
      throws Exception {
    Table api = t.table();
    assertEquals(idsAt(api.schema(), s1), hmsIds(t));
    for (Types.NestedField f : api.schema().columns()) {
      if (f.type().typeId() == org.apache.iceberg.types.Type.TypeID.BOOLEAN) continue;
      assertEquals(f.name(), impalaBlobNdv(api, f.fieldId(), s1),
          tbl.getColumn(f.name()).getStats().getNumDistinctValues());
    }
  }

  @Test
  public void noNewFilesIsNoopAndDeletesUpdateRowCountOnly() throws Exception {
    // The HiveCatalog keeps the row count of its tables in HMS itself, without deletes.
    checkNoopAndDeletes(hiveTable("_hive", PartitionSpec.unpartitioned(), SCHEMA),
        false);
    IcebergTestTables t = IcebergTestTables.create(tmp, DB, tableName("_hadoop"),
        SCHEMA, PartitionSpec.unpartitioned(), 2);
    ifx.registerPathA(t, Collections.emptyMap());
    checkNoopAndDeletes(t, true);
  }

  /**
   * After COMPUTE STATS, COMPUTE INCREMENTAL STATS without new snapshots changes nothing,
   * and after each of two position deletes it sends just the live row count.
   */
  private void checkNoopAndDeletes(IcebergTestTables t, boolean storesRowCount)
      throws Exception {
    t.append(t.file(100, null, rows(0, 20, 1)));
    // Without the HiveCatalog, NOOP needs the snapshot to be current before COMPUTE
    // STATS, whose time has only seconds.
    IcebergAnalysisFixture.awaitSecondAfter(System.currentTimeMillis());
    ifx.reloadPathA(t);
    compute(t, "compute stats", "FULL");
    State base = State.of(t);

    Command noop = analyze(t, "compute incremental stats");

    assertEquals("NOOP", noop.mode);
    assertEquals(NO_NEW_FILES, noop.params.getNo_child_queries_message());
    assertNull(noop.results);
    assertFalse(noop.params.isSetCol_stats_query());
    assertFalse(noop.params.isSetIceberg_theta_query());
    ifx.reloadPathA(t);
    assertEquals(base, State.of(t));

    for (long deleted = 2; deleted <= 4; deleted += 2) {
      t.rowDelta(t.positionDeletes(10, 2, null));
      ifx.reloadPathA(t);
      Command delete = analyze(t, "compute incremental stats");
      assertEquals("FULL", delete.mode);
      assertFalse(delete.params.isSetCol_stats_query());
      assertFalse(delete.params.isSetIceberg_theta_query());
      assertFalse(delete.results.params().isSetColumn_stats());
      assertEquals(20 - deleted,
          delete.results.params().getTable_stats().getNum_rows());
      IcebergTable tbl = apply(t, delete);
      if (storesRowCount) assertEquals(20 - deleted, tbl.getNumRows());
      State after = State.of(t);
      assertEquals(base.ids, after.ids);
      assertEquals(base.statsFiles, after.statsFiles);
      assertEquals(base.hmsColumnStats, after.hmsColumnStats);
    }
  }

  /**
   * Runs COMPUTE STATS on the current snapshot and checks that it gives the statistics
   * 'incremental' has.
   */
  private static void assertSameAsFull(IcebergTestTables t, IcebergTable incremental)
      throws Exception {
    Map<String, TColumnStats> merged = columnStats(incremental);
    long mergedRows = incremental.getNumRows();
    IcebergTable full = compute(t, "compute stats", "FULL");
    assertEquals(full.getNumRows(), mergedRows);
    Map<String, TColumnStats> expected = columnStats(full);
    assertEquals(expected.keySet(), merged.keySet());
    for (String c : expected.keySet()) {
      TColumnStats e = expected.get(c).deepCopy();
      TColumnStats m = merged.get(c).deepCopy();
      assertEquals(c, e.getAvg_size(), m.getAvg_size(), 1e-9);
      e.setAvg_size(0);
      m.setAvg_size(0);
      assertEquals(c, e, m);
    }
  }

  private static Map<String, TColumnStats> columnStats(IcebergTable tbl) {
    Map<String, TColumnStats> res = new TreeMap<>();
    for (Column c : tbl.getColumns()) res.put(c.getName(), c.getStats().toThrift());
    return res;
  }

  /** Table metadata and HMS statistics that an update may change. */
  private static final class State {
    final String metadataLocation;
    final List<String> statsFiles;
    final Map<Integer, Long> ids;
    final Map<String, String> hmsParams;
    final List<ColumnStatisticsObj> hmsColumnStats;

    private State(String metadataLocation, List<String> statsFiles,
        Map<Integer, Long> ids, Map<String, String> hmsParams,
        List<ColumnStatisticsObj> hmsColumnStats) {
      this.metadataLocation = metadataLocation;
      this.statsFiles = statsFiles;
      this.ids = ids;
      this.hmsParams = hmsParams;
      this.hmsColumnStats = hmsColumnStats;
    }

    static State of(IcebergTestTables t) throws Exception {
      Table api = t.table();
      List<String> files = new ArrayList<>();
      for (StatisticsFile f : api.statisticsFiles()) {
        files.add(f.snapshotId() + " " + f.path());
      }
      Collections.sort(files);
      List<String> cols = new ArrayList<>();
      for (Types.NestedField f : api.schema().columns()) cols.add(f.name());
      try (MetaStoreClient client = ifx.catalog().getMetaStoreClient()) {
        Map<String, String> params = new TreeMap<>(
            client.getHiveClient().getTable(t.db(), t.name()).getParameters());
        List<ColumnStatisticsObj> colStats = new ArrayList<>(
            MetastoreShim.getTableColumnStatistics(client.getHiveClient(), t.db(),
                t.name(), cols));
        colStats.sort((a, b) -> a.getColName().compareTo(b.getColName()));
        return new State(
            ((HasTableOperations) api).operations().current().metadataFileLocation(),
            files, ComputeStatsSnapshotPropertyConverter.stringToMap(params.get(IDS)),
            params, colStats);
      }
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof State)) return false;
      State s = (State) o;
      return metadataLocation.equals(s.metadataLocation)
          && statsFiles.equals(s.statsFiles) && ids.equals(s.ids)
          && hmsParams.equals(s.hmsParams) && hmsColumnStats.equals(s.hmsColumnStats);
    }

    @Override
    public int hashCode() { return metadataLocation.hashCode(); }

    @Override
    public String toString() {
      return metadataLocation + " " + statsFiles + " " + ids + " " + hmsParams + " "
          + hmsColumnStats;
    }
  }

  private static Map<Integer, Long> hmsIds(IcebergTestTables t) throws Exception {
    return State.of(t).ids;
  }

  private static Map<Integer, Long> idsAt(Schema schema, long snapshotId) {
    Map<Integer, Long> res = new HashMap<>();
    for (Types.NestedField f : schema.columns()) res.put(f.fieldId(), snapshotId);
    return res;
  }

  private static StatisticsFile statsFile(Table api, long snapshotId) {
    for (StatisticsFile f : api.statisticsFiles()) {
      if (f.snapshotId() == snapshotId) return f;
    }
    throw new AssertionError("no statistics file of snapshot " + snapshotId);
  }

  /** The "ndv" property of the Impala theta blob of 'fieldId' at 'snapshotId'. */
  private static long impalaBlobNdv(Table api, int fieldId, long snapshotId) {
    for (BlobMetadata b : statsFile(api, snapshotId).blobMetadata()) {
      if (b.fields().equals(Collections.singletonList(fieldId))
          && b.sourceSnapshotId() == snapshotId
          && b.properties().containsKey(PuffinFixtures.IMPALA_CREATED_BY)) {
        return Long.parseLong(b.properties().get("ndv"));
      }
    }
    throw new AssertionError("no Impala blob of field " + fieldId + " at " + snapshotId);
  }

  private static double estimate(byte[] sketch) {
    return CompactSketch.heapify(Memory.wrap(sketch), 9001).getEstimate();
  }

  /** Declared values of 'column' in the live data files of the current snapshot. */
  private static List<Object> liveValues(IcebergTestTables t, String column) {
    List<Object> res = new ArrayList<>();
    try (CloseableIterable<FileScanTask> tasks = t.table().newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        res.addAll(t.columnValues(task.file().location(), column));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return res;
  }
}
