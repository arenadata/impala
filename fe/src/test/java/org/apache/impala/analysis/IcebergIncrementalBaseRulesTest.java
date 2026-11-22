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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.IcebergIncrementalBaseRules.Decision;
import org.apache.impala.analysis.IcebergStatsPlan.MetricColumn;
import org.apache.impala.analysis.IcebergStatsPlan.Mode;
import org.apache.impala.analysis.IcebergStatsPlan.NdvBase;
import org.apache.impala.analysis.IcebergStatsPlan.Reason;
import org.apache.impala.analysis.IcebergStatsPlan.ReasonKind;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.iceberg.IcebergTestTables;
import org.apache.impala.catalog.iceberg.PuffinFixtures;
import org.apache.impala.catalog.iceberg.PuffinFixtures.BlobSpec;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

/**
 * Metric and NDV bases that IcebergIncrementalBaseRules#decide chooses from the
 * metadata of Iceberg tables in the impalad cache, the reasons it falls back to full
 * statistics with and their warnings.
 */
public class IcebergIncrementalBaseRulesTest extends FrontendTestBase {
  private static final String DB = "iceberg_incremental_base_rules";
  /** Theta columns: id, s, flag, ts; HMS NDV: id, s, ts. */
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "s", Types.StringType.get()),
      optional(3, "flag", Types.BooleanType.get()),
      optional(4, "ts", Types.TimestampType.withoutZone()),
      optional(5, "bin", Types.BinaryType.get()));
  private static final List<Integer> THETA_FIELDS = Arrays.asList(1, 2, 3, 4);
  private static final String IDS = IcebergTable.COMPUTE_STATS_SNAPSHOT_IDS;
  /** HMS NDV of every column with statistics, and "ndv" of blobs by default. */
  private static final long NDV = 10;
  private static final String WARNING_PREFIX =
      "COMPUTE INCREMENTAL STATS on Iceberg table " + DB + ".";

  @Rule public TemporaryFolder tmp = new TemporaryFolder();
  @Rule public TestName testName = new TestName();

  private static final IcebergAnalysisFixture ifx =
      IcebergAnalysisFixture.create(feFixture_);

  private IcebergTestTables create(String suffix, Schema schema, PartitionSpec spec,
      int formatVersion) {
    return IcebergTestTables.create(tmp, DB,
        testName.getMethodName().toLowerCase() + suffix, schema, spec, formatVersion);
  }

  private IcebergTestTables create() {
    return create("", SCHEMA, PartitionSpec.unpartitioned(), 2);
  }

  /**
   * Loads 't' with 'ids' as the property of base snapshots (none if null); every column
   * gets HMS statistics with NDV.
   */
  private static IcebergTable load(IcebergTestTables t, String ids) throws Exception {
    Map<String, String> params = new HashMap<>();
    if (ids != null) params.put(IDS, ids);
    IcebergTable tbl = ifx.addPathB(t, params);
    for (Column c : tbl.getColumns()) {
      c.getStats().setNumNulls(0);
      c.getStats().setNumDistinctValues(NDV);
    }
    return tbl;
  }

  private static void dropStats(IcebergTable tbl, String col) {
    tbl.getColumn(col).getStats().setNumNulls(-1);
    tbl.getColumn(col).getStats().setNumDistinctValues(-1);
  }

  private static StatisticsFile register(IcebergTestTables t, long snapshotId,
      BlobSpec... blobs) {
    StatisticsFile file =
        PuffinFixtures.write(t.table(), snapshotId, Arrays.asList(blobs));
    t.setStats(snapshotId, file);
    return file;
  }

  private static BlobSpec impalaBlob(int fieldId, long snapshotId, long ndv) {
    return foreignBlob(fieldId, snapshotId, ndv).impalaCreatedBy("Impala 4.5.0");
  }

  private static BlobSpec foreignBlob(int fieldId, long snapshotId, long ndv) {
    return PuffinFixtures.blob(fieldId, snapshotId, new byte[] {1, 2, 3})
        .ndvProperty(Long.toString(ndv));
  }

  /** Impala blobs of every theta column at 'snapshotId', as COMPUTE STATS writes them. */
  private static StatisticsFile registerImpalaBlobs(IcebergTestTables t,
      long snapshotId) {
    List<BlobSpec> blobs = new ArrayList<>();
    for (int f : THETA_FIELDS) blobs.add(impalaBlob(f, snapshotId, NDV));
    return register(t, snapshotId, blobs.toArray(new BlobSpec[0]));
  }

  /** Columns and theta field ids as the resolver plans them. */
  private static Decision decide(IcebergTable tbl) throws AnalysisException {
    IcebergStatsPlan plan = IcebergStatsBaseResolver.resolve(
        tbl, new ArrayList<>(tbl.getColumns()), false, false);
    return IcebergIncrementalBaseRules.decide(
        tbl, plan.metricColumns(), plan.thetaFieldIds(), tbl.snapshotId());
  }

  private static Decision decide(IcebergTable tbl, String... cols)
      throws AnalysisException {
    List<Column> metricCols = new ArrayList<>();
    for (String c : cols) metricCols.add(tbl.getColumn(c));
    IcebergStatsPlan plan =
        IcebergStatsBaseResolver.resolve(tbl, metricCols, false, false);
    return IcebergIncrementalBaseRules.decide(
        tbl, plan.metricColumns(), plan.thetaFieldIds(), tbl.snapshotId());
  }

  private static Map<Integer, NdvBase> sameFile(StatisticsFile file, int... fieldIds) {
    Map<Integer, NdvBase> res = new LinkedHashMap<>();
    for (int f : fieldIds) res.put(f, new NdvBase(file.snapshotId(), file.path()));
    return res;
  }

  private static void assertIncremental(Decision d, long bh, Long bn,
      Map<Integer, NdvBase> ndvBases) {
    assertSame(Mode.INCREMENTAL, d.mode);
    assertNull(d.reason);
    assertFalse(d.deletesOnly);
    assertEquals(Long.valueOf(bh), d.bh);
    assertEquals(bn, d.bn);
    assertEquals(
        new ArrayList<>(ndvBases.keySet()), new ArrayList<>(d.ndvBases.keySet()));
    for (Map.Entry<Integer, NdvBase> e : ndvBases.entrySet()) {
      NdvBase actual = d.ndvBases.get(e.getKey());
      assertEquals("blob snapshot of field " + e.getKey(),
          e.getValue().blobSnapshotId(), actual.blobSnapshotId());
      assertEquals("stats file of field " + e.getKey(),
          e.getValue().statsFilePath(), actual.statsFilePath());
    }
    assertEquals(Collections.emptyList(), d.warnings);
  }

  /** FULL with 'kind' and 'args', whose only warning ends with 'reasonText'. */
  private static void assertFull(Decision d, IcebergTable tbl, ReasonKind kind,
      List<String> args, String reasonText) {
    assertSame(Mode.FULL, d.mode);
    assertSame(kind, d.reason.kind());
    assertEquals(args, d.reason.args());
    assertFalse(d.deletesOnly);
    assertNull(d.bh);
    assertNull(d.bn);
    assertTrue(d.ndvBases.isEmpty());
    assertEquals(Collections.singletonList(WARNING_PREFIX + tbl.getName()
        + " computes full statistics: " + reasonText + "."), d.warnings);
  }

  private static List<String> args(Object... values) {
    List<String> res = new ArrayList<>();
    for (Object v : values) res.add(String.valueOf(v));
    return res;
  }

  @Test
  public void emptyMetricColumnsGiveFullWithoutReason() throws Exception {
    IcebergTestTables t = create();
    long s1 = t.append(t.file(100, null));
    IcebergTable tbl = load(t, "1-5:" + s1);
    Decision d = IcebergIncrementalBaseRules.decide(
        tbl, Collections.emptyList(), Collections.emptyList(), s1);
    assertSame(Mode.FULL, d.mode);
    assertNull(d.reason);
    assertNull(d.bh);
    assertTrue(d.ndvBases.isEmpty());
    assertEquals(Collections.emptyList(), d.warnings);
  }

  @Test
  public void basesOfAllColumnsGiveIncrementalForEveryLayout() throws Exception {
    PartitionSpec byId = PartitionSpec.builderFor(SCHEMA).identity("id").build();
    for (int version : new int[] {1, 2}) {
      for (PartitionSpec spec : Arrays.asList(PartitionSpec.unpartitioned(), byId)) {
        String partition = spec.isPartitioned() ? "id=1" : null;
        IcebergTestTables t = create("_v" + version + "_" + spec.fields().size(),
            SCHEMA, spec, version);
        t.append(t.file(100, partition));
        long bh = t.append(t.file(100, partition));
        StatisticsFile file = registerImpalaBlobs(t, bh);
        t.append(t.file(100, partition));
        IcebergTable tbl = load(t, "1-5:" + bh);
        assertIncremental(decide(tbl), bh, bh, sameFile(file, 1, 2, 3, 4));
      }
    }
  }

  @Test
  public void foreignNdvBaseNewerThanImpalaBlobIsChosen() throws Exception {
    IcebergTestTables t = create();
    long bh = t.append(t.file(100, null));
    StatisticsFile impala = registerImpalaBlobs(t, bh);
    long s2 = t.append(t.file(100, null));
    StatisticsFile trino = register(t, s2, foreignBlob(2, s2, 15));
    long s3 = t.append(t.file(100, null));
    // Table loading may put the newer foreign NDV into HMS statistics of the column.
    IcebergTable tbl = load(t, "1-5:" + bh);
    tbl.getColumn("s").getStats().setNumDistinctValues(15);
    Map<Integer, NdvBase> bases = sameFile(impala, 1, 2, 3, 4);
    bases.put(2, new NdvBase(s2, trino.path()));
    assertIncremental(decide(tbl), bh, bh, bases);

    // At S1 itself.
    StatisticsFile atS1 =
        register(t, s3, foreignBlob(1, s3, NDV), foreignBlob(2, s3, 15));
    tbl = load(t, "1-5:" + bh);
    bases.put(1, new NdvBase(s3, atS1.path()));
    bases.put(2, new NdvBase(s3, atS1.path()));
    assertIncremental(decide(tbl), bh, bh, bases);
    assertIncremental(decide(tbl, "s"), bh, s3, sameFile(atS1, 2));
  }

  @Test
  public void blobOffChainOfS1IsIgnored() throws Exception {
    IcebergTestTables t = create();
    long bh = t.append(t.file(100, null));
    StatisticsFile impala = registerImpalaBlobs(t, bh);
    long dropped = t.append(t.file(100, null));
    register(t, dropped, foreignBlob(2, dropped, NDV));
    t.rollbackTo(bh);
    long s1 = t.append(t.file(100, null));
    long branch = t.appendToBranch("b", t.file(100, null));
    register(t, branch, foreignBlob(2, branch, NDV));
    IcebergTable tbl = load(t, "1-5:" + bh);
    assertEquals(s1, tbl.snapshotId());
    assertIncremental(decide(tbl), bh, bh, sameFile(impala, 1, 2, 3, 4));
  }

  @Test
  public void hmsNdvOfImpalaBlobOlderThanBaseIsMismatch() throws Exception {
    IcebergTestTables t = create();
    long s0 = t.append(t.file(100, null));
    register(t, s0, impalaBlob(1, s0, 5), impalaBlob(2, s0, 5));
    long bh = t.append(t.file(100, null));
    registerImpalaBlobs(t, bh);
    t.append(t.file(100, null));
    IcebergTable tbl = load(t, "1-5:" + bh);
    tbl.getColumn("id").getStats().setNumDistinctValues(5);
    tbl.getColumn("s").getStats().setNumDistinctValues(5);
    // HMS has no NDV of BOOLEAN columns to compare.
    tbl.getColumn("flag").getStats().setNumDistinctValues(2);
    assertFull(decide(tbl), tbl, ReasonKind.BASE_NDV_MISMATCH, args("id", "s", bh),
        "NDV of column(s) id, s in the Hive Metastore differs from the theta sketch of"
            + " snapshot " + bh);

    // Without an Impala blob of the field at the base there is nothing to compare with.
    t.setStats(bh, PuffinFixtures.write(t.table(), bh, Arrays.asList(
        foreignBlob(1, bh, NDV), impalaBlob(2, bh, NDV), impalaBlob(3, bh, NDV),
        impalaBlob(4, bh, NDV))));
    tbl = load(t, "1-5:" + bh);
    tbl.getColumn("id").getStats().setNumDistinctValues(5);
    assertEquals(Mode.INCREMENTAL, decide(tbl).mode);
  }

  @Test
  public void baseAfterLegacyKeepsOlderImpalaSketch() throws Exception {
    IcebergTestTables t = create();
    long s1 = t.append(t.file(100, null));
    StatisticsFile impala = registerImpalaBlobs(t, s1);
    // LEGACY statistics at 'bh' updated HMS and the property, not Puffin.
    long bh = t.append(t.file(100, null));
    t.append(t.file(100, null));
    IcebergTable tbl = load(t, "1-5:" + bh);
    tbl.getColumn("s").getStats().setNumDistinctValues(NDV + 7);
    assertIncremental(decide(tbl), bh, s1, sameFile(impala, 1, 2, 3, 4));
  }

  @Test
  public void twoBlobsOfFieldAtSnapshotInFileHaveNoNdvBase() throws Exception {
    IcebergTestTables t = create();
    long bh = t.append(t.file(100, null));
    register(t, bh, impalaBlob(1, bh, NDV), impalaBlob(2, bh, NDV),
        foreignBlob(2, bh, NDV), impalaBlob(3, bh, NDV), impalaBlob(4, bh, NDV));
    IcebergTable tbl = load(t, "1-5:" + bh);
    assertFull(decide(tbl), tbl, ReasonKind.COLUMNS_WITHOUT_NDV_BASE, args("s"),
        "no theta sketch for column(s) s");
  }

  @Test
  public void oldIdsWithoutBlobsHaveNoNdvBase() throws Exception {
    IcebergTestTables t = create();
    long bh = t.append(t.file(100, null));
    register(t, bh, impalaBlob(2, bh, NDV));
    t.append(t.file(100, null));
    IcebergTable tbl = load(t, "1-5:" + bh);
    assertFull(decide(tbl), tbl, ReasonKind.COLUMNS_WITHOUT_NDV_BASE,
        args("id", "flag", "ts"), "no theta sketch for column(s) id, flag, ts");
    // A column without theta sketches needs no NDV base.
    assertIncremental(decide(tbl, "bin"), bh, null, Collections.emptyMap());
  }

  @Test
  public void missingIdsOrHmsStatsGiveNoBase() throws Exception {
    IcebergTestTables t = create();
    long bh = t.append(t.file(100, null));
    registerImpalaBlobs(t, bh);
    String noBase = "no statistics computed by Impala to use as a base";
    IcebergTable tbl = load(t, null);
    assertFull(decide(tbl), tbl, ReasonKind.NO_BASE, args(), noBase);
    tbl = load(t, "1-5:-1");
    assertFull(decide(tbl), tbl, ReasonKind.NO_BASE, args(), noBase);
    tbl = load(t, "1-5:" + bh);
    for (Column c : tbl.getColumns()) dropStats(tbl, c.getName());
    assertFull(decide(tbl), tbl, ReasonKind.NO_BASE, args(), noBase);
  }

  @Test
  public void columnsWithoutIdsKeyOrHmsStatsHaveNoBase() throws Exception {
    IcebergTestTables t = create();
    long bh = t.append(t.file(100, null));
    registerImpalaBlobs(t, bh);
    IcebergTable tbl = load(t, "1-3:" + bh + ",5:-1");
    dropStats(tbl, "flag");
    assertFull(decide(tbl), tbl, ReasonKind.COLUMNS_WITHOUT_BASE,
        args("flag", "ts", "bin"), "no base statistics for column(s) flag, ts, bin");
  }

  @Test
  public void droppedAndAddedColumnOfSameNameHasNoBase() throws Exception {
    IcebergTestTables t = create();
    long bh = t.append(t.file(100, null));
    registerImpalaBlobs(t, bh);
    t.table().updateSchema().deleteColumn("s").commit();
    t.table().updateSchema().addColumn("s", Types.StringType.get()).commit();
    t.append(t.file(100, null));
    // HMS statistics are kept by name, so the new column has them.
    IcebergTable tbl = load(t, "1-5:" + bh);
    assertFull(decide(tbl), tbl, ReasonKind.COLUMNS_WITHOUT_BASE, args("s"),
        "no base statistics for column(s) s");
  }

  @Test
  public void renamedColumnHasBaseOnlyWithStatsUnderNewName() throws Exception {
    IcebergTestTables t = create();
    long bh = t.append(t.file(100, null));
    StatisticsFile file = registerImpalaBlobs(t, bh);
    t.table().updateSchema().renameColumn("s", "s2").commit();
    t.append(t.file(100, null));
    IcebergTable tbl = load(t, "1-5:" + bh);
    assertIncremental(decide(tbl), bh, bh, sameFile(file, 1, 2, 3, 4));
    dropStats(tbl, "s2");
    assertFull(decide(tbl), tbl, ReasonKind.COLUMNS_WITHOUT_BASE, args("s2"),
        "no base statistics for column(s) s2");
  }

  @Test
  public void basesOfDifferentSnapshotsDiffer() throws Exception {
    IcebergTestTables t = create();
    long s1 = t.append(t.file(100, null));
    registerImpalaBlobs(t, s1);
    long s2 = t.append(t.file(100, null));
    StatisticsFile idFile = register(t, s2, impalaBlob(1, s2, NDV));
    t.append(t.file(100, null));
    // After COMPUTE INCREMENTAL STATS t (id).
    IcebergTable tbl = load(t, "1:" + s2 + ",2-5:" + s1);
    assertFull(decide(tbl), tbl, ReasonKind.BASES_DIFFER, args(),
        "columns have base statistics from different snapshots");
    assertIncremental(decide(tbl, "id"), s2, s2, sameFile(idFile, 1));
  }

  @Test
  public void baseOffChainOfS1IsNotAncestor() throws Exception {
    IcebergTestTables t = create("_rollback", SCHEMA, PartitionSpec.unpartitioned(), 2);
    long s1 = t.append(t.file(100, null));
    long bh = t.append(t.file(100, null));
    registerImpalaBlobs(t, bh);
    t.rollbackTo(s1);
    long s3 = t.append(t.file(100, null));
    IcebergTable tbl = load(t, "1-5:" + bh);
    assertFull(decide(tbl), tbl, ReasonKind.BASE_NOT_ANCESTOR, args(bh, s3),
        "base snapshot " + bh + " is not an ancestor of snapshot " + s3);

    t = create("_expired", SCHEMA, PartitionSpec.unpartitioned(), 2);
    bh = t.append(t.file(100, null));
    registerImpalaBlobs(t, bh);
    t.append(t.file(100, null));
    long s1Expired = t.append(t.file(100, null));
    t.expire(bh);
    tbl = load(t, "1-5:" + bh);
    assertFull(decide(tbl), tbl, ReasonKind.BASE_NOT_ANCESTOR, args(bh, s1Expired),
        "base snapshot " + bh + " is not an ancestor of snapshot " + s1Expired);
  }

  @Test
  public void timestampColumnWithHmsStatsHasBase() throws Exception {
    Class<?> timestampStatsClass;
    try {
      timestampStatsClass = Class.forName(
          "org.apache.hadoop.hive.metastore.api.TimestampColumnStatsData");
    } catch (ClassNotFoundException e) {
      // Only Hive 4 sends TIMESTAMP statistics as timestampStats.
      Assume.assumeNoException(e);
      return;
    }
    IcebergTestTables t = create();
    long bh = t.append(t.file(100, null));
    StatisticsFile file = register(t, bh, impalaBlob(4, bh, NDV));
    t.append(t.file(100, null));
    IcebergTable tbl = load(t, "1-5:" + bh);
    dropStats(tbl, "ts");
    Object tsStats = timestampStatsClass.getConstructor().newInstance();
    timestampStatsClass.getMethod("setNumNulls", long.class).invoke(tsStats, 0L);
    timestampStatsClass.getMethod("setNumDVs", long.class).invoke(tsStats, NDV);
    ColumnStatisticsData data = new ColumnStatisticsData();
    ColumnStatisticsData.class.getMethod("setTimestampStats", timestampStatsClass)
        .invoke(data, tsStats);
    assertTrue(tbl.getColumn("ts").updateStats(data));
    assertIncremental(decide(tbl, "ts"), bh, bh, sameFile(file, 4));
  }

  /** Decides on 'tbl' whose Iceberg table is a spy that 'stub' prepares. */
  private static Decision decideWithApi(IcebergTable tbl, Consumer<Table> stub)
      throws AnalysisException {
    IcebergStatsPlan plan = IcebergStatsBaseResolver.resolve(
        tbl, new ArrayList<>(tbl.getColumns()), false, false);
    IcebergTable spyTbl = spy(tbl);
    Table api = spy(tbl.getIcebergApiTable());
    stub.accept(api);
    doReturn(api).when(spyTbl).getIcebergApiTable();
    return IcebergIncrementalBaseRules.decide(
        spyTbl, plan.metricColumns(), plan.thetaFieldIds(), tbl.snapshotId());
  }

  @Test
  public void icebergMetadataFailureIsAnalysisError() throws Exception {
    IcebergTestTables t = create();
    long bh = t.append(t.file(100, null));
    registerImpalaBlobs(t, bh);
    IcebergTable tbl = load(t, "1-5:" + bh);
    for (RuntimeException failure : Arrays.asList(
        new NotFoundException("File does not exist: x.stats"),
        new RuntimeIOException("File does not exist: x.stats"),
        new ValidationException("File does not exist: x.stats"))) {
      try {
        decideWithApi(tbl, api -> doThrow(failure).when(api).statisticsFiles());
        fail("expected AnalysisException");
      } catch (AnalysisException e) {
        assertEquals("Failed to read Iceberg metadata of table " + DB + "."
            + tbl.getName() + ": " + failure.getMessage(), e.getMessage());
        assertSame(failure, e.getCause());
      }
    }
  }

  @Test
  public void s1MissingFromIcebergTableIsNotAncestor() throws Exception {
    IcebergTestTables t = create();
    long bh = t.append(t.file(100, null));
    registerImpalaBlobs(t, bh);
    long s1 = t.append(t.file(100, null));
    IcebergTable tbl = load(t, "1-5:" + bh);
    // The cached Iceberg table is older than the snapshot of the table.
    Decision d = decideWithApi(tbl, api -> doReturn(null).when(api).snapshot(s1));
    assertFull(d, tbl, ReasonKind.BASE_NOT_ANCESTOR, args(bh, s1),
        "base snapshot " + bh + " is not an ancestor of snapshot " + s1);
  }

  @Test
  public void firstStatsFileWithBlobOfSnapshotIsNdvBase() throws Exception {
    IcebergTestTables t = create();
    long bh = t.append(t.file(100, null));
    long s2 = t.append(t.file(100, null));
    long s3 = t.append(t.file(100, null));
    register(t, s2, impalaBlob(1, bh, NDV));
    register(t, s3, impalaBlob(1, bh, NDV));
    IcebergTable tbl = load(t, "1-5:" + bh);
    // The table metadata orders the files, not the order of registration.
    String first = tbl.getIcebergApiTable().statisticsFiles().get(0).path();
    assertIncremental(decide(tbl, "id"), bh, bh,
        Collections.singletonMap(1, new NdvBase(bh, first)));
  }

  @Test
  public void foreignBlobAtBaseWithHmsNdvIsNotMismatch() throws Exception {
    IcebergTestTables t = create();
    long bh = t.append(t.file(100, null));
    register(t, bh, impalaBlob(1, bh, NDV));
    long s2 = t.append(t.file(100, null));
    register(t, s2, foreignBlob(1, bh, 15));
    IcebergTable tbl = load(t, "1-5:" + bh);
    tbl.getColumn("id").getStats().setNumDistinctValues(15);
    assertEquals(Mode.INCREMENTAL, decide(tbl, "id").mode);
  }

  @Test
  public void fullStatsWarningCoversEveryReasonKind() throws Exception {
    IcebergTestTables t = create();
    t.append(t.file(100, null));
    IcebergTable tbl = load(t, null);
    List<String> eleven = args("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9",
        "c10", "c11");
    List<String> ndvArgs = new ArrayList<>(eleven);
    ndvArgs.add("7");
    Map<ReasonKind, String> texts = new LinkedHashMap<>();
    Map<ReasonKind, List<String>> reasonArgs = new HashMap<>();
    texts.put(ReasonKind.NO_BASE, "no statistics computed by Impala to use as a base");
    texts.put(ReasonKind.COLUMNS_WITHOUT_BASE,
        "no base statistics for column(s) c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, …");
    reasonArgs.put(ReasonKind.COLUMNS_WITHOUT_BASE, eleven);
    texts.put(ReasonKind.BASES_DIFFER,
        "columns have base statistics from different snapshots");
    texts.put(ReasonKind.BASE_NOT_ANCESTOR, "base snapshot 3 is not an ancestor of"
        + " snapshot 5");
    reasonArgs.put(ReasonKind.BASE_NOT_ANCESTOR, args(3, 5));
    texts.put(ReasonKind.BASE_NDV_MISMATCH, "NDV of column(s) c1, c2, c3, c4, c5, c6, c7,"
        + " c8, c9, c10, … in the Hive Metastore differs from the theta sketch of"
        + " snapshot 7");
    reasonArgs.put(ReasonKind.BASE_NDV_MISMATCH, ndvArgs);
    texts.put(ReasonKind.COLUMNS_WITHOUT_NDV_BASE, "no theta sketch for column(s) c1");
    reasonArgs.put(ReasonKind.COLUMNS_WITHOUT_NDV_BASE, args("c1"));
    texts.put(ReasonKind.UNSUPPORTED_SNAPSHOT,
        "snapshot 9 has no manifest list or operation");
    reasonArgs.put(ReasonKind.UNSUPPORTED_SNAPSHOT, args(9));
    texts.put(ReasonKind.NO_TOTAL_RECORDS, "snapshot 4 has no usable 'total-records' to"
        + " evaluate 'impala.iceberg_incremental_stats_max_deleted_ratio'");
    reasonArgs.put(ReasonKind.NO_TOTAL_RECORDS, args(4));
    texts.put(ReasonKind.DELETED_RATIO, "estimated ratio of deleted rows 0.3333 exceeds"
        + " 'impala.iceberg_incremental_stats_max_deleted_ratio'=0.250");
    reasonArgs.put(ReasonKind.DELETED_RATIO, args(1.0 / 3, "0.250"));
    texts.put(ReasonKind.REPLACE_REWROTE_NEW_FILES, "data files added after snapshot 4"
        + " were rewritten by a 'replace' snapshot");
    reasonArgs.put(ReasonKind.REPLACE_REWROTE_NEW_FILES, args(4));
    texts.put(ReasonKind.SNAPSHOT_WITHOUT_SCHEMA, "snapshot 6 has no schema id");
    reasonArgs.put(ReasonKind.SNAPSHOT_WITHOUT_SCHEMA, args(6));
    texts.put(ReasonKind.COLUMNS_TYPE_CHANGED,
        "type of column(s) c1, c2 changed since their base statistics");
    reasonArgs.put(ReasonKind.COLUMNS_TYPE_CHANGED, args("c1", "c2"));
    texts.put(ReasonKind.PUFFIN_DISABLED, "writing Puffin statistics is disabled"
        + " ('enable_writing_puffin_stats' or 'impala.iceberg_write_puffin_stats')");
    assertEquals(Arrays.asList(ReasonKind.values()), new ArrayList<>(texts.keySet()));
    for (Map.Entry<ReasonKind, String> e : texts.entrySet()) {
      Reason reason = new Reason(e.getKey(),
          reasonArgs.getOrDefault(e.getKey(), Collections.emptyList()));
      assertEquals(WARNING_PREFIX + tbl.getName() + " computes full statistics: "
          + e.getValue() + ".",
          IcebergIncrementalBaseRules.fullStatsWarning(tbl, reason));
    }
  }
}
