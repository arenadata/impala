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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.analysis.ComputeStatsStmt;
import org.apache.impala.analysis.IcebergAnalysisFixture;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.iceberg.IcebergTestTables;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.thrift.TComputeStatsParams;
import org.apache.impala.thrift.TQueryOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

/**
 * COMPUTE INCREMENTAL STATS on a HadoopCatalog table whose current snapshot goes back
 * to the base after the row count of a later snapshot reached HMS. The child queries
 * are answered by IncrementalResults.
 */
public class IcebergIncrementalStatsRollbackScenarioTest extends FrontendTestBase {
  private static final String DB = "iceberg_incremental_rollback_scenario";
  private static final String NO_NEW_FILES =
      "No new data files since the last COMPUTE STATS; statistics are up to date.";
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "s", Types.StringType.get()));

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
    TComputeStatsParams params;
    List<String> warnings;
    String mode;
    String reason;
    /** Null if the statement has no child queries. */
    IncrementalResults results;
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

  /** Rows with ids [from, to); every fifth string is NULL. */
  private static Map<String, List<?>> rows(int from, int to) {
    List<Object> ids = new ArrayList<>();
    List<Object> strings = new ArrayList<>();
    for (int id = from; id < to; ++id) {
      ids.add(id);
      strings.add(id % 5 == 0 ? null : "s" + (id % 9));
    }
    Map<String, List<?>> res = new LinkedHashMap<>();
    res.put("id", ids);
    res.put("s", strings);
    return res;
  }

  /**
   * Snapshot ids of the statistics, their Puffin files and the column statistics in HMS:
   * what a row count update must keep.
   */
  private static String columnState(IcebergTestTables t) throws Exception {
    Table api = t.table();
    List<String> files = new ArrayList<>();
    for (StatisticsFile f : api.statisticsFiles()) {
      files.add(f.snapshotId() + " " + f.path());
    }
    Collections.sort(files);
    List<String> cols = new ArrayList<>();
    for (Types.NestedField f : api.schema().columns()) cols.add(f.name());
    try (MetaStoreClient client = ifx.catalog().getMetaStoreClient()) {
      String ids = client.getHiveClient().getTable(t.db(), t.name()).getParameters()
          .get(IcebergTable.COMPUTE_STATS_SNAPSHOT_IDS);
      List<ColumnStatisticsObj> colStats = new ArrayList<>(
          MetastoreShim.getTableColumnStatistics(client.getHiveClient(), t.db(),
              t.name(), cols));
      colStats.sort((a, b) -> a.getColName().compareTo(b.getColName()));
      return ids + " " + files + " " + colStats;
    }
  }

  /** FULL of the row count only: no column queries, no reason, columns kept. */
  private static IcebergTable computeRowCount(IcebergTestTables t, long numRows,
      String columns) throws Exception {
    Command c = analyze(t, "compute incremental stats");
    assertEquals("FULL", c.mode);
    assertEquals("", c.reason);
    assertEquals(Collections.emptyList(), c.warnings);
    assertFalse(c.params.isSetCol_stats_query());
    assertFalse(c.params.isSetIceberg_theta_query());
    assertFalse(c.results.params().isSetColumn_stats());
    assertEquals(numRows, c.results.params().getTable_stats().getNum_rows());
    IcebergTable tbl = apply(t, c);
    assertEquals(numRows, tbl.getNumRows());
    assertEquals(columns, columnState(t));
    return tbl;
  }

  @Test
  public void rollbackToBaseAfterDeletesRecomputesRowCount() throws Exception {
    IcebergTestTables t = IcebergTestTables.create(tmp, DB,
        testName.getMethodName().toLowerCase(), SCHEMA, PartitionSpec.unpartitioned(),
        2);
    ifx.registerPathA(t, Collections.emptyMap());
    long bh = t.append(t.file(100, null, rows(0, 20)));
    IcebergAnalysisFixture.awaitSecondAfter(System.currentTimeMillis());
    ifx.reloadPathA(t);
    Command full = analyze(t, "compute stats");
    assertEquals("FULL", full.mode);
    assertEquals(20, apply(t, full).getNumRows());
    String columns = columnState(t);
    assertTrue(columns, columns.contains(":" + bh));

    t.rowDelta(t.positionDeletes(10, 2, null));
    ifx.reloadPathA(t);
    computeRowCount(t, 18, columns);

    // The snapshot is the base again, but HMS has the row count of the deletes.
    t.rollbackTo(bh);
    IcebergAnalysisFixture.awaitSecondAfter(System.currentTimeMillis());
    assertEquals(18, ifx.reloadPathA(t).getNumRows());
    computeRowCount(t, 20, columns);

    Command noop = analyze(t, "compute incremental stats");
    assertEquals("NOOP", noop.mode);
    assertEquals(NO_NEW_FILES, noop.params.getNo_child_queries_message());
    assertNull(noop.results);
    assertEquals(columns, columnState(t));
  }
}
