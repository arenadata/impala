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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.IcebergIncrementalBaseRules.Decision;
import org.apache.impala.analysis.IcebergStatsPlan.Mode;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.iceberg.IcebergTestTables;
import org.apache.impala.catalog.iceberg.PuffinFixtures;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.service.BackendConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

/**
 * NOOP of COMPUTE INCREMENTAL STATS when no files were added since the bases: it needs
 * the row count in HMS to be that of the current snapshot, so either the HiveCatalog
 * keeps the row count or the snapshot became current before the last COMPUTE STATS.
 * Otherwise only the row count is recomputed. Covers IcebergIncrementalBaseRules#decide
 * and the plan of IcebergStatsBaseResolver.
 */
public class IcebergIncrementalNoopRulesTest extends FrontendTestBase {
  private static final String DB = "iceberg_incremental_noop_rules";
  private static final String IDS = IcebergTable.COMPUTE_STATS_SNAPSHOT_IDS;
  private static final String TIME = Table.TBL_PROP_LAST_COMPUTE_STATS_TIME;
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "s", Types.StringType.get()));
  private static final List<Integer> THETA_FIELDS = Arrays.asList(1, 2);
  private static final long NDV = 10;

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

  private String tableName() { return testName.getMethodName().toLowerCase(); }

  /** HadoopCatalog table with one file at 'bh' and Impala sketches at 'bh'. */
  private IcebergTestTables tableWithBase() {
    IcebergTestTables t = IcebergTestTables.create(tmp, DB, tableName(), SCHEMA,
        PartitionSpec.unpartitioned(), 2);
    registerImpalaBlobs(t, t.append(file(t)));
    return t;
  }

  private static DataFile file(IcebergTestTables t) {
    return t.file(100, null, Collections.singletonMap("id", Arrays.asList(1, 2)));
  }

  private static void registerImpalaBlobs(IcebergTestTables t, long snapshotId) {
    List<PuffinFixtures.BlobSpec> blobs = new ArrayList<>();
    for (int f : THETA_FIELDS) {
      blobs.add(PuffinFixtures.blob(f, snapshotId, new byte[] {1, 2, 3})
          .ndvProperty(Long.toString(NDV)).impalaCreatedBy("Impala 4.5.0"));
    }
    t.setStats(snapshotId, PuffinFixtures.write(t.table(), snapshotId, blobs));
  }

  /**
   * Loads 't' with base 'bh' of every column and HMS statistics matching the sketches;
   * 'computeStatsTime' is the last COMPUTE STATS time in seconds, not set if null.
   */
  private static IcebergTable load(IcebergTestTables t, long bh, String computeStatsTime)
      throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put(IDS, "1-2:" + bh);
    if (computeStatsTime != null) params.put(TIME, computeStatsTime);
    return withHmsStats(ifx.addPathB(t, params));
  }

  private static IcebergTable withHmsStats(IcebergTable tbl) {
    for (Column c : tbl.getColumns()) {
      c.getStats().setNumNulls(0);
      c.getStats().setNumDistinctValues(NDV);
    }
    return tbl;
  }

  /** Time of the last entry of the snapshot log for 'snapshotId'. */
  private static long currentSince(IcebergTestTables t, long snapshotId) {
    Long res = null;
    for (HistoryEntry e : t.table().history()) {
      if (e.snapshotId() == snapshotId) res = e.timestampMillis();
    }
    assertTrue("no history entry of " + snapshotId, res != null);
    return res;
  }

  private static String seconds(long millis, long plusSeconds) {
    return Long.toString(millis / 1000 + plusSeconds);
  }

  private static Decision decide(IcebergTable tbl) throws AnalysisException {
    IcebergStatsPlan plan = IcebergStatsBaseResolver.resolve(
        tbl, new ArrayList<>(tbl.getColumns()), false, false);
    return IcebergIncrementalBaseRules.decide(
        tbl, plan.metricColumns(), plan.thetaFieldIds(), tbl.snapshotId());
  }

  private static IcebergStatsPlan resolveIncremental(IcebergTable tbl)
      throws AnalysisException {
    return IcebergStatsBaseResolver.resolve(
        tbl, new ArrayList<>(tbl.getColumns()), true, false);
  }

  private static void assertNoop(Decision d, long bh) {
    assertSame(Mode.NOOP, d.mode);
    assertFalse(d.deletesOnly);
    assertNull(d.reason);
    assertEquals(Long.valueOf(bh), d.bh);
    assertEquals(THETA_FIELDS, new ArrayList<>(d.ndvBases.keySet()));
    assertEquals(Collections.emptyList(), d.warnings);
  }

  /** FULL of the row count only, without a reason, as for a gap of only deletes. */
  private static void assertRowCountOnly(Decision d, long bh) {
    assertSame(Mode.FULL, d.mode);
    assertTrue(d.deletesOnly);
    assertNull(d.reason);
    assertEquals(Long.valueOf(bh), d.bh);
    assertTrue(d.ndvBases.isEmpty());
    assertEquals(Collections.emptyList(), d.warnings);
    assertEquals(Long.valueOf(0),
        d.profileCounters.get(IcebergIncrementalBaseRules.PROFILE_GAP_FILES));
  }

  private static void assertRowCountOnly(IcebergStatsPlan plan, long s1) {
    assertEquals(Mode.FULL, plan.mode());
    assertEquals(s1, plan.s1());
    assertTrue(plan.deletesOnly());
    assertTrue(plan.metricColumns().isEmpty());
    assertTrue(plan.thetaFieldIds().isEmpty());
    assertNull(plan.reason());
    assertTrue(plan.warnings().isEmpty());
  }

  @Test
  public void currentBeforeLastComputeStatsIsNoop() throws Exception {
    IcebergTestTables t = tableWithBase();
    long bh = t.table().currentSnapshot().snapshotId();
    long since = currentSince(t, bh);

    assertNoop(decide(load(t, bh, seconds(since, 1))), bh);
    assertNoop(decide(load(t, bh, " " + seconds(since, 100) + " ")), bh);

    IcebergStatsPlan plan = resolveIncremental(load(t, bh, seconds(since, 1)));
    assertEquals(Mode.NOOP, plan.mode());
    assertEquals(bh, plan.s1());
    assertFalse(plan.deletesOnly());
    assertEquals(THETA_FIELDS, plan.thetaFieldIds());
  }

  @Test
  public void currentInSameSecondOrLaterRecomputesRowCount() throws Exception {
    IcebergTestTables t = tableWithBase();
    long bh = t.table().currentSnapshot().snapshotId();
    long since = currentSince(t, bh);

    assertRowCountOnly(decide(load(t, bh, seconds(since, 0))), bh);
    assertRowCountOnly(decide(load(t, bh, seconds(since, -1))), bh);
    assertRowCountOnly(resolveIncremental(load(t, bh, seconds(since, 0))), bh);
  }

  @Test
  public void missingOrInvalidComputeStatsTimeRecomputesRowCount() throws Exception {
    IcebergTestTables t = tableWithBase();
    long bh = t.table().currentSnapshot().snapshotId();
    String later = seconds(currentSince(t, bh), 1);

    assertRowCountOnly(decide(load(t, bh, null)), bh);
    for (String invalid : new String[] {"", "x", later + ".5", "1e12"}) {
      assertRowCountOnly(decide(load(t, bh, invalid)), bh);
    }
    assertRowCountOnly(resolveIncremental(load(t, bh, null)), bh);
  }

  /** The snapshot log entry of the rollback counts, not the first one of the base. */
  @Test
  public void rollbackToBaseAfterComputeStatsRecomputesRowCount() throws Exception {
    IcebergTestTables t = tableWithBase();
    long bh = t.table().currentSnapshot().snapshotId();
    long firstSince = currentSince(t, bh);
    String computed = seconds(firstSince, 1);
    t.append(file(t));
    IcebergAnalysisFixture.awaitSecondAfter(firstSince);
    t.rollbackTo(bh);
    assertTrue(currentSince(t, bh) / 1000 >= Long.parseLong(computed));

    assertRowCountOnly(decide(load(t, bh, computed)), bh);
    assertRowCountOnly(resolveIncremental(load(t, bh, computed)), bh);
    assertNoop(decide(load(t, bh, seconds(currentSince(t, bh), 1))), bh);
  }

  /** The HiveCatalog sets the row count in HMS on each commit, rollbacks included. */
  @Test
  public void integratedTableIsNoopWhateverComputeStatsTimeIs() throws Exception {
    IcebergTestTables t = ifx.createHiveCatalogTestTable(DB, tableName(), SCHEMA,
        PartitionSpec.unpartitioned(), 2);
    long bh = t.append(file(t));
    registerImpalaBlobs(t, bh);
    t.table().updateProperties().set(IDS, "1-2:" + bh).commit();
    IcebergTable tbl = withHmsStats(ifx.loadHiveCatalogTable(DB, tableName()));
    assertNull(tbl.getMetaStoreTable().getParameters().get(TIME));

    assertNoop(decide(tbl), bh);
    IcebergStatsPlan plan = resolveIncremental(tbl);
    assertEquals(Mode.NOOP, plan.mode());
    assertFalse(plan.deletesOnly());
  }
}
