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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.analysis.ComputeStatsStmt;
import org.apache.impala.analysis.IcebergAnalysisFixture;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.iceberg.IcebergTestTables;
import org.apache.impala.catalog.iceberg.PuffinFixtures;
import org.apache.impala.catalog.iceberg.PuffinFixtures.BlobSpec;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableType;
import org.apache.impala.thrift.TComputeStatsParams;
import org.apache.impala.thrift.TCounter;
import org.apache.impala.thrift.TDdlExecRequest;
import org.apache.impala.thrift.TDdlQueryOptions;
import org.apache.impala.thrift.TDdlType;
import org.apache.impala.thrift.TDropStatsParams;
import org.apache.impala.thrift.TIcebergNdvBase;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.util.DebugUtils;
import org.apache.impala.util.IcebergUtil.ComputeStatsSnapshotPropertyConverter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * COMPUTE INCREMENTAL STATS on tables registered in HMS whose base is not the plain
 * result of the previous Impala command: sketches of other engines, table events that
 * force full statistics, and a base that HMS stopped matching before the update.
 * The child queries are answered by IncrementalResults.
 */
public class IcebergIncrementalStatsBaseScenarioTest extends FrontendTestBase {
  private static final String DB = "iceberg_incremental_base_scenario";
  private static final String IDS = IcebergTable.COMPUTE_STATS_SNAPSHOT_IDS;
  private static final String MAX_DELETED_RATIO =
      "impala.iceberg_incremental_stats_max_deleted_ratio";
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "p", Types.IntegerType.get()),
      optional(3, "s", Types.StringType.get()));
  /** Values only the sketches of another engine have, by column. */
  private static final Map<String, List<?>> FOREIGN_EXTRA = new LinkedHashMap<>();
  static {
    FOREIGN_EXTRA.put("id", Arrays.asList(1000, 1001, 1002));
    FOREIGN_EXTRA.put("p", Arrays.asList(7));
    FOREIGN_EXTRA.put("s", Arrays.asList("foreign1", "foreign2"));
  }

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
    TComputeStatsParams params;
    List<String> warnings;
    String mode;
    String reason;
    Map<String, Long> counters = new TreeMap<>();
    IncrementalResults results;

    Map<Integer, Long> ndvBases() {
      Map<Integer, Long> res = new TreeMap<>();
      for (TIcebergNdvBase b : params.getIceberg_puffin_stats().getNdv_bases()) {
        res.put(b.getField_id(), b.getSnapshot_id());
      }
      return res;
    }
  }

  private static Command analyze(IcebergTestTables t, String command) throws Exception {
    TQueryOptions options = new TQueryOptions();
    options.setCompute_column_minmax_stats(true);
    String sql = command + " " + t.db() + "." + t.name();
    Command c = new Command();
    ComputeStatsStmt stmt;
    try (FrontendProfile.Scope scope = FrontendProfile.createNewWithScope()) {
      AnalysisResult res =
          feFixture_.parseAndAnalyze(sql, feFixture_.createAnalysisCtx(options));
      stmt = (ComputeStatsStmt) res.getStmt();
      c.warnings = new ArrayList<>(res.getAnalyzer().getWarnings());
      c.mode = FrontendProfile.getCurrent().getInfoString("Iceberg stats mode");
      c.reason = FrontendProfile.getCurrent().getInfoString("Iceberg stats reason");
      List<TCounter> all = FrontendProfile.getCurrent().emitAsThrift().getCounters();
      if (all != null) {
        for (TCounter counter : all) {
          if (counter.getName().startsWith("IcebergStats")) {
            c.counters.put(counter.getName(), counter.getValue());
          }
        }
      }
    }
    c.params = stmt.toThrift();
    if (!c.params.isSetNo_child_queries_message()) {
      c.results = IncrementalResults.run(feFixture_, stmt, t);
    }
    return c;
  }

  private static IcebergTable apply(IcebergTestTables t, Command c) throws Exception {
    ifx.updateStats(c.results.params());
    return ifx.reloadPathA(t);
  }

  private IcebergTestTables newTable(String suffix) throws Exception {
    return ifx.createHiveCatalogTestTable(DB,
        testName.getMethodName().toLowerCase() + suffix, SCHEMA,
        PartitionSpec.unpartitioned(), 2);
  }

  /** New table with 20 rows in one file and FULL statistics. */
  private IcebergTestTables tableWithBase(String suffix) throws Exception {
    IcebergTestTables t = newTable(suffix);
    t.append(t.file(100, null, rows(0, 20)));
    computeFull(t);
    return t;
  }

  private static void computeFull(IcebergTestTables t) throws Exception {
    ifx.reloadPathA(t);
    Command full = analyze(t, "compute stats");
    assertEquals("FULL", full.mode);
    apply(t, full);
  }

  /** Appends ids [from, to) with strings in the third column, whatever its name. */
  private static long appendRows(IcebergTestTables t, int from, int to)
      throws Exception {
    Map<String, List<?>> values = rows(from, to);
    values.put(t.table().schema().columns().get(2).name(), values.remove("s"));
    long s = t.append(t.file(100, null, values));
    ifx.reloadPathA(t);
    return s;
  }

  /** Rows of SCHEMA with ids [from, to); every fifth string is NULL. */
  private static Map<String, List<?>> rows(int from, int to) {
    List<Object> ids = new ArrayList<>();
    List<Object> parts = new ArrayList<>();
    List<Object> strings = new ArrayList<>();
    for (int id = from; id < to; ++id) {
      ids.add(id);
      parts.add(id % 3);
      strings.add(id % 5 == 0 ? null : "s" + (id % 9) + "xx".substring(0, id % 3));
    }
    Map<String, List<?>> res = new LinkedHashMap<>();
    res.put("id", ids);
    res.put("p", parts);
    res.put("s", strings);
    return res;
  }

  @Test
  public void foreignSketchAtAncestorIsNdvBaseAndStaysUntouched() throws Exception {
    IcebergTestTables t = tableWithBase("");
    long s0 = t.table().currentSnapshot().snapshotId();
    DataFile f2 = t.file(100, null, rows(20, 40));
    long p = t.append(f2);
    StatisticsFile foreign = registerForeignSketches(t, p, Collections.emptyList());
    byte[] foreignBytes = Files.readAllBytes(Paths.get(localPath(foreign)));
    DataFile f3 = t.file(100, null, rows(40, 60));
    long s1 = t.append(f3);
    ifx.reloadPathA(t);

    Command inc = analyze(t, "compute incremental stats");

    assertEquals("INCREMENTAL", inc.mode);
    assertEquals(s0, inc.params.getIceberg_puffin_stats().getMetrics_base_snapshot_id());
    assertEquals(idsAt(SCHEMA, p), inc.ndvBases());
    assertEquals(sorted(f2.location(), f3.location()), inc.results.metricFiles());
    assertEquals(sorted(f3.location()), inc.results.thetaFiles());
    Map<String, Long> gap = new TreeMap<>();
    gap.put("IcebergStatsGapBytes", 200L);
    gap.put("IcebergStatsGapFiles", 2L);
    gap.put("IcebergStatsGapRecords", 40L);
    gap.put("IcebergStatsThetaGapBytes", 100L);
    gap.put("IcebergStatsThetaGapFiles", 1L);
    assertEquals(gap, inc.counters);
    IcebergTable tbl = apply(t, inc);

    assertEquals(idsAt(SCHEMA, s1), hmsIds(t));
    assertUnionWithForeign(t, tbl, s1);
    StatisticsFile after = statsFile(t.table(), p);
    assertEquals(foreign.path(), after.path());
    assertEquals(foreign.fileSizeInBytes(), after.fileSizeInBytes());
    assertEquals(foreign.blobMetadata(), after.blobMetadata());
    assertArrayEquals(foreignBytes, Files.readAllBytes(Paths.get(localPath(after))));
  }

  @Test
  public void foreignSketchAtS1IsReplacedByImpalaBlobInNewFile() throws Exception {
    IcebergTestTables t = tableWithBase("");
    long s0 = t.table().currentSnapshot().snapshotId();
    DataFile f2 = t.file(100, null, rows(20, 40));
    long s1 = t.append(f2);
    byte[] otherPayload = {5, 4, 3, 2, 1};
    BlobSpec other = PuffinFixtures.blob(1, s1, otherPayload)
        .type("connector-bloom-filter-v1").property("fpp", "0.01");
    StatisticsFile connector =
        registerForeignSketches(t, s1, Collections.singletonList(other));
    ifx.reloadPathA(t);

    Command inc = analyze(t, "compute incremental stats");

    assertEquals("INCREMENTAL", inc.mode);
    assertEquals(s0, inc.params.getIceberg_puffin_stats().getMetrics_base_snapshot_id());
    assertEquals(idsAt(SCHEMA, s1), inc.ndvBases());
    assertEquals(sorted(f2.location()), inc.results.metricFiles());
    assertEquals(Collections.emptyList(), inc.results.thetaFiles());
    IcebergTable tbl = apply(t, inc);

    assertEquals(idsAt(SCHEMA, s1), hmsIds(t));
    StatisticsFile written = statsFile(t.table(), s1);
    assertNotEquals(connector.path(), written.path());
    List<String> theta = new ArrayList<>();
    BlobMetadata copied = null;
    for (BlobMetadata b : written.blobMetadata()) {
      if (b.type().equals("connector-bloom-filter-v1")) {
        copied = b;
      } else {
        assertTrue(b.toString(),
            b.properties().containsKey(PuffinFixtures.IMPALA_CREATED_BY));
        theta.add(b.type() + " " + b.fields());
      }
    }
    assertEquals(3, theta.size());
    assertEquals(Collections.singletonList(1), copied.fields());
    assertEquals(Collections.singletonMap("fpp", "0.01"), copied.properties());
    assertArrayEquals(otherPayload,
        PuffinFixtures.payloadOfType(t.table(), written, copied.type()));
    assertUnionWithForeign(t, tbl, s1);
  }

  /**
   * Registers for 'snapshotId' a file of another engine with a theta blob per column of
   * SCHEMA, the sketch of its live values and FOREIGN_EXTRA, and the 'others' blobs.
   */
  private static StatisticsFile registerForeignSketches(IcebergTestTables t,
      long snapshotId, List<BlobSpec> others) {
    List<BlobSpec> blobs = new ArrayList<>();
    for (Types.NestedField f : SCHEMA.columns()) {
      blobs.add(PuffinFixtures.blob(f.fieldId(), snapshotId,
          PuffinFixtures.sketch(f.type(), foreignValues(t, snapshotId, f.name()))));
    }
    blobs.addAll(others);
    StatisticsFile file = PuffinFixtures.write(t.table(), snapshotId, blobs);
    t.setStats(snapshotId, file);
    return file;
  }

  private static List<Object> foreignValues(IcebergTestTables t, long snapshotId,
      String column) {
    List<Object> res = liveValues(t, snapshotId, column);
    res.addAll(FOREIGN_EXTRA.get(column));
    return res;
  }

  /**
   * The Impala blob of every column at 's1' estimates its live values together with
   * FOREIGN_EXTRA, and HMS holds the "ndv" of that blob.
   */
  private static void assertUnionWithForeign(IcebergTestTables t, IcebergTable tbl,
      long s1) {
    Map<Integer, byte[]> blobs =
        PuffinFixtures.impalaThetaPayloads(t.table(), statsFile(t.table(), s1));
    assertEquals(Arrays.asList(1, 2, 3), new ArrayList<>(blobs.keySet()));
    for (Types.NestedField f : SCHEMA.columns()) {
      double estimate =
          CompactSketch.heapify(Memory.wrap(blobs.get(f.fieldId())), 9001).getEstimate();
      assertEquals(f.name(), PuffinFixtures.sketch(f.type(),
          foreignValues(t, s1, f.name())).getEstimate(), estimate, 0);
      assertNotEquals(f.name(), PuffinFixtures.sketch(f.type(),
          liveValues(t, s1, f.name())).getEstimate(), estimate, 0);
      assertEquals(f.name(), impalaBlobNdv(t.table(), f.fieldId(), s1),
          tbl.getColumn(f.name()).getStats().getNumDistinctValues());
    }
  }

  @Test
  public void dropStatsGivesNoBase() throws Exception {
    IcebergTestTables t = tableWithBase("");
    TDdlExecRequest req = new TDdlExecRequest();
    req.setDdl_type(TDdlType.DROP_STATS);
    req.setDrop_stats_params(new TDropStatsParams(new TTableName(t.db(), t.name())));
    ifx.execDdl(req);
    long s1 = appendRows(t, 20, 30);

    checkFull(t, s1, "NO_BASE",
        "no statistics computed by Impala to use as a base");
  }

  @Test
  public void rollbackGivesBaseNotAncestor() throws Exception {
    IcebergTestTables t = newTable("");
    long s0 = t.append(t.file(100, null, rows(0, 20)));
    long b = t.append(t.file(100, null, rows(20, 30)));
    computeFull(t);
    t.rollbackTo(s0);
    long s1 = appendRows(t, 30, 40);

    checkFull(t, s1, "BASE_NOT_ANCESTOR",
        "base snapshot " + b + " is not an ancestor of snapshot " + s1);
  }

  @Test
  public void schemaChangesGiveColumnsWithoutBase() throws Exception {
    IcebergTestTables added = tableWithBase("_add");
    added.table().updateSchema().addColumn("n", Types.IntegerType.get()).commit();
    checkFull(added, appendRows(added, 20, 30),
        "COLUMNS_WITHOUT_BASE", "no base statistics for column(s) n");

    // HMS keeps column statistics by name, so the renamed column has none.
    IcebergTestTables renamed = tableWithBase("_rename");
    renamed.table().updateSchema().renameColumn("s", "s2").commit();
    checkFull(renamed, appendRows(renamed, 20, 30),
        "COLUMNS_WITHOUT_BASE", "no base statistics for column(s) s2");

    // The new field id has no key in the base ids.
    IcebergTestTables readded = tableWithBase("_readd");
    readded.table().updateSchema().deleteColumn("s").commit();
    readded.table().updateSchema().addColumn("s", Types.StringType.get()).commit();
    checkFull(readded, appendRows(readded, 20, 30),
        "COLUMNS_WITHOUT_BASE", "no base statistics for column(s) s");
  }

  @Test
  public void deletedRatioAboveThresholdIsFull() throws Exception {
    IcebergTestTables t = newTable("");
    DataFile half = t.file(100, null, rows(0, 10));
    t.append(half);
    t.append(t.file(100, null, rows(10, 20)));
    computeFull(t);
    ifx.setTableParams(t, Collections.singletonMap(MAX_DELETED_RATIO, "0.1"));
    t.delete(half);
    long s1 = appendRows(t, 20, 30);

    checkFull(t, s1, "DELETED_RATIO", "estimated ratio of deleted rows 0.5000 exceeds '"
        + MAX_DELETED_RATIO + "'=0.1");
  }

  @Test
  public void replaceOfGapFilesIsFull() throws Exception {
    IcebergTestTables t = tableWithBase("");
    long b = t.table().currentSnapshot().snapshotId();
    DataFile f2 = t.file(100, null, rows(20, 30));
    t.append(f2);
    long s1 = t.replace(Collections.singletonList(f2),
        Collections.singletonList(t.file(100, null, rows(20, 30))));
    ifx.reloadPathA(t);

    checkFull(t, s1, "REPLACE_REWROTE_NEW_FILES", "data files added after snapshot " + b
        + " were rewritten by a 'replace' snapshot");
  }

  /**
   * COMPUTE INCREMENTAL STATS of 't' at snapshot 's1' computes full statistics with
   * 'reason' and its 'text', and writes them as COMPUTE STATS does.
   */
  private static void checkFull(IcebergTestTables t, long s1, String reason,
      String text) throws Exception {
    Command c = analyze(t, "compute incremental stats");

    assertEquals("FULL", c.mode);
    assertEquals(reason, c.reason);
    assertEquals(Collections.singletonList("COMPUTE INCREMENTAL STATS on Iceberg table "
        + t.db() + "." + t.name() + " computes full statistics: " + text + "."),
        c.warnings);
    assertEquals(s1, c.params.getIceberg_puffin_stats().getCalc_snapshot_id());
    // Ids of fields that are no longer in the schema stay as they were.
    Map<Integer, Long> ids = hmsIds(t);
    IcebergTable tbl = apply(t, c);

    Table api = t.table();
    Map<Integer, Long> all = idsAt(api.schema(), s1);
    ids.putAll(all);
    assertEquals(ids, hmsIds(t));
    assertEquals(all.keySet(),
        PuffinFixtures.impalaThetaPayloads(api, statsFile(api, s1)).keySet());
    for (Types.NestedField f : api.schema().columns()) {
      assertEquals(f.name(), impalaBlobNdv(api, f.fieldId(), s1),
          tbl.getColumn(f.name()).getStats().getNumDistinctValues());
    }
  }

  @Test
  public void baseMismatchReloadsTableAndNextAnalysisIsFull() throws Exception {
    IcebergTestTables t = tableWithBase("");
    long version = checkBaseMismatch(t, null);

    IcebergTable cached = cached(t);
    assertTrue(cached.getCatalogVersion() > version);
    assertEquals(Collections.emptyMap(), cachedIds(cached));
    assertEquals(Collections.emptyMap(), hmsIds(t));
    Command next = analyze(t, "compute incremental stats");
    assertEquals("FULL", next.mode);
    assertEquals("NO_BASE", next.reason);
  }

  @Test
  public void baseMismatchWithFailedReloadKeepsVersion() throws Exception {
    IcebergTestTables t = tableWithBase("");
    long version = checkBaseMismatch(t, DebugUtils.LOAD_METADATA_DELAY
        + ":EXCEPTION@validationexception@injected reload failure");

    IcebergTable cached = cached(t);
    assertEquals(version, cached.getCatalogVersion());
    assertEquals(Collections.emptyMap(), cachedIds(cached));
    assertEquals(Collections.emptyMap(), hmsIds(t));
  }

  /**
   * COMPUTE INCREMENTAL STATS of 't' after an append, whose update runs with
   * 'debugAction' after another writer changed the NDV of "id" in HMS. The update fails
   * with advice to compute full statistics, and its repeat sees a conflict. Returns the
   * catalog version of the cached table before the update.
   */
  private static long checkBaseMismatch(IcebergTestTables t, String debugAction)
      throws Exception {
    appendRows(t, 20, 40);
    Command inc = analyze(t, "compute incremental stats");
    assertEquals("INCREMENTAL", inc.mode);
    ifx.setHmsColumnNdv(t, "id", 99);
    long version = cached(t).getCatalogVersion();
    TDdlExecRequest req = updateRequest(inc, debugAction);

    try {
      ifx.execDdl(req);
      fail("no base mismatch");
    } catch (ImpalaException e) {
      assertEquals(IcebergStatsBaseMismatchException.class, e.getClass());
      assertEquals("Base statistics of column(s) id of table " + t.db() + "."
          + t.name() + " do not match the Hive Metastore; run COMPUTE STATS.",
          e.getMessage());
    }
    long afterMismatch = cached(t).getCatalogVersion();
    try {
      ifx.execDdl(updateRequest(inc, debugAction));
      fail("no conflict");
    } catch (ImpalaRuntimeException e) {
      assertEquals("Statistics of table " + t.db() + "." + t.name() + " were changed "
          + "concurrently; run COMPUTE INCREMENTAL STATS again.", e.getMessage());
    }
    assertEquals(afterMismatch, cached(t).getCatalogVersion());
    return version;
  }

  private static TDdlExecRequest updateRequest(Command c, String debugAction) {
    TAlterTableParams alter = new TAlterTableParams();
    alter.setAlter_type(TAlterTableType.UPDATE_STATS);
    alter.setTable_name(c.results.params().getTable_name());
    alter.setUpdate_stats_params(c.results.params().deepCopy());
    TDdlExecRequest req = new TDdlExecRequest();
    req.setDdl_type(TDdlType.ALTER_TABLE);
    req.setAlter_table_params(alter);
    req.setQuery_options(new TDdlQueryOptions());
    if (debugAction != null) req.getQuery_options().setDebug_action(debugAction);
    return req;
  }

  private static IcebergTable cached(IcebergTestTables t) throws Exception {
    org.apache.impala.catalog.Table tbl =
        ifx.catalog().getOrLoadTable(t.db(), t.name(), "test", null);
    assertTrue(String.valueOf(tbl), tbl instanceof IcebergTable);
    return (IcebergTable) tbl;
  }

  private static Map<Integer, Long> cachedIds(IcebergTable tbl) {
    return ComputeStatsSnapshotPropertyConverter.stringToMap(
        tbl.getMetaStoreTable().getParameters().get(IDS));
  }

  private static Map<Integer, Long> hmsIds(IcebergTestTables t) throws Exception {
    try (MetaStoreClient client = ifx.catalog().getMetaStoreClient()) {
      return ComputeStatsSnapshotPropertyConverter.stringToMap(client.getHiveClient()
          .getTable(t.db(), t.name()).getParameters().get(IDS));
    }
  }

  private static Map<Integer, Long> idsAt(Schema schema, long snapshotId) {
    Map<Integer, Long> res = new TreeMap<>();
    for (Types.NestedField f : schema.columns()) res.put(f.fieldId(), snapshotId);
    return res;
  }

  private static List<String> sorted(String... paths) {
    List<String> res = new ArrayList<>(Arrays.asList(paths));
    Collections.sort(res);
    return res;
  }

  private static StatisticsFile statsFile(Table api, long snapshotId) {
    for (StatisticsFile f : api.statisticsFiles()) {
      if (f.snapshotId() == snapshotId) return f;
    }
    throw new AssertionError("no statistics file of snapshot " + snapshotId);
  }

  private static String localPath(StatisticsFile f) {
    return f.path().replaceFirst("^file:", "");
  }

  /** The "ndv" property of the Impala theta blob of 'fieldId' at 'snapshotId'. */
  private static long impalaBlobNdv(Table api, int fieldId, long snapshotId) {
    for (BlobMetadata b : statsFile(api, snapshotId).blobMetadata()) {
      if (b.fields().equals(Collections.singletonList(fieldId))
          && b.properties().containsKey(PuffinFixtures.IMPALA_CREATED_BY)) {
        return Long.parseLong(b.properties().get("ndv"));
      }
    }
    throw new AssertionError("no Impala blob of field " + fieldId + " at " + snapshotId);
  }

  /** Declared values of 'column' in the live data files of 'snapshotId'. */
  private static List<Object> liveValues(IcebergTestTables t, long snapshotId,
      String column) {
    List<Object> res = new ArrayList<>();
    try (CloseableIterable<FileScanTask> tasks =
        t.table().newScan().useSnapshot(snapshotId).planFiles()) {
      for (FileScanTask task : tasks) {
        res.addAll(t.columnValues(task.file().location(), column));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return res;
  }
}
