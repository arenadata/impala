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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.IcebergAnalysisFixture;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.iceberg.IcebergTestTables;
import org.apache.impala.catalog.iceberg.PuffinFixtures;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.FakeHmsStatsWriter.Method;
import org.apache.impala.service.IcebergIncrementalReconciler.Outcome;
import org.apache.impala.service.IcebergIncrementalReconciler.Verdict;
import org.apache.impala.thrift.TAlterTableUpdateStatsParams;
import org.apache.impala.thrift.TColumnStats;
import org.apache.impala.thrift.TColumnValue;
import org.apache.impala.thrift.TIcebergNdvBase;
import org.apache.impala.thrift.TIcebergPuffinStatsParams;
import org.apache.impala.thrift.TIcebergStatsMode;
import org.apache.impala.util.IcebergUtil;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 * IcebergIncrementalReconciler as functions: the verdict of each reconcile rule and the
 * merged column statistics. Bases are written by IcebergStatsUpdater#apply, so the
 * snapshot ids come from the property it stores, and HMS and the cached columns are
 * what a table load would see after it.
 */
public class IcebergIncrementalReconcilerTest extends IcebergStatsUpdaterTestBase {
  private static final IcebergAnalysisFixture ifx =
      IcebergAnalysisFixture.create(feFixture_);
  private static final List<String> THETA = Arrays.asList("i", "s", "b", "d");
  private static final byte[] CORRUPT = {-1, -1, -1, -1, -1, -1, -1, -1};

  /** HMS call count before the call under test. */
  private int mark_;

  @Before
  public void keepStoredParameters() {
    hms_.setStoredParameters(new HashMap<>());
  }

  @Test
  public void writeAfterImpalaBaseReadsHmsOnceAndWritesNothing() throws Exception {
    IcebergTestTables t = create();
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    long b = base(t, tbl, TIcebergStatsMode.FULL, COLUMNS);
    long s1 = t.append(t.file(100, null));
    List<StatisticsFile> files = t.table().statisticsFiles();

    Outcome o = reconcile(tbl, incremental(s1, b, 100, COLUMNS, bases(t, b, THETA)));

    assertEquals(Verdict.WRITE, o.verdict);
    assertTrue(o.mismatchedColumns.isEmpty());
    assertEquals(Collections.singletonList(Method.READ_COLUMN_STATS), callsUnderTest());
    assertEquals(files, t.table().statisticsFiles());
  }

  @Test
  public void baseMismatchListsEachFailedCheckInFieldOrder() throws Exception {
    IcebergTestTables t = create();
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    long b = base(t, tbl, TIcebergStatsMode.FULL, COLUMNS);
    long s1 = t.append(t.file(100, null));
    Map<String, ColumnStatisticsObj> stored = hmsStats();
    stored.remove("i");
    stored.get("d").getStatsData().getDecimalStats().setNumDVs(9);
    hms_.setStoredStats(stored);
    tbl.getColumn("s").getStats().setNumNulls(-1);

    Outcome o = reconcile(tbl, incremental(s1, b, 100, COLUMNS, bases(t, b, THETA)));

    // "b" has no NDV in HMS and "bin" no sketch: only their HMS key is checked.
    assertEquals(Verdict.BASE_MISMATCH, o.verdict);
    assertEquals(Arrays.asList("i", "s", "d"), o.mismatchedColumns);
    assertEquals(Collections.singletonList(Method.READ_COLUMN_STATS), callsUnderTest());
  }

  @Test
  public void foreignNdvBasesAtAncestorAndAtS1AreWritten() throws Exception {
    IcebergTestTables t = create();
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    long b0 = base(t, tbl, TIcebergStatsMode.FULL, COLUMNS);
    // LEGACY moves the metrics base to b without a sketch; Trino adds one for "s".
    long b = base(t, tbl, TIcebergStatsMode.LEGACY, COLUMNS);
    List<String> trinoS = Arrays.asList("p", "q", "r", "a");
    register(t, b, PuffinFixtures.blob(fieldId("s"), b, sketch("s", trinoS)));
    long s1 = t.append(t.file(100, null));
    List<Boolean> trinoB = Arrays.asList(true, false);
    register(t, s1, PuffinFixtures.blob(fieldId("b"), s1, sketch("b", trinoB)));
    List<String> cols = Arrays.asList("i", "s", "b");
    Map<String, TIcebergNdvBase> bases = new LinkedHashMap<>();
    bases.put("i", ndvBase(t, "i", b0));
    bases.put("s", ndvBase(t, "s", b));
    bases.put("b", ndvBase(t, "b", s1));
    TAlterTableUpdateStatsParams params = incremental(s1, b, 100, cols, bases);
    // The NDV base of "b" is S1 itself: no files after it.
    params.getIceberg_puffin_stats().putToTheta_sketches(fieldId("b"),
        ByteBuffer.wrap(new byte[0]));
    List<StatisticsFile> files = t.table().statisticsFiles();

    assertEquals(Verdict.WRITE, reconcile(tbl, params).verdict);
    Map<Integer, IcebergStatsUpdater.ColumnResult> res = finalStats(tbl, params);

    assertEquals(15L, res.get(fieldId("i")).ndv);
    assertEquals(6L, res.get(fieldId("s")).ndv);
    assertEquals(estimate("s", union(trinoS, GAP.get("s"))), res.get(fieldId("s")).ndv);
    assertEquals(2L, res.get(fieldId("b")).ndv);
    assertEquals(estimate("b", trinoB), res.get(fieldId("b")).ndv);
    assertEquals(files, t.table().statisticsFiles());
  }

  @Test
  public void timestampNdvIsReadFromHive4TimestampStats() throws Exception {
    Class<?> cls;
    try {
      cls = Class.forName(
          "org.apache.hadoop.hive.metastore.api.TimestampColumnStatsData");
    } catch (ClassNotFoundException e) {
      Assume.assumeNoException(e);
      return;
    }
    Schema schema = new Schema(optional(1, "ts", Types.TimestampType.withoutZone()));
    IcebergTestTables t = IcebergTestTables.create(tmp, DB,
        testName.getMethodName().toLowerCase(), schema, PartitionSpec.unpartitioned(), 2);
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    long b = t.append(t.file(100, null));
    TAlterTableUpdateStatsParams full = params(TIcebergStatsMode.FULL, b, 100,
        Collections.emptyList());
    TColumnStats stats = new TColumnStats(16, 16, -1, 0, -1, -1);
    full.putToColumn_stats("ts", stats);
    TIcebergPuffinStatsParams fp = full.getIceberg_puffin_stats();
    fp.putToColumn_field_ids("ts", 1);
    fp.addToTheta_field_ids(1);
    fp.putToTheta_sketches(1, ByteBuffer.wrap(PuffinFixtures.sketch(
        Types.TimestampType.withoutZone(), Arrays.asList(1L, 2L, 3L)).toByteArray()));
    apply(tbl, full);
    long s1 = t.append(t.file(100, null));
    TAlterTableUpdateStatsParams params = new TAlterTableUpdateStatsParams(full);
    TIcebergPuffinStatsParams p = params.getIceberg_puffin_stats();
    p.setMode(TIcebergStatsMode.INCREMENTAL);
    p.setCalc_snapshot_id(s1);
    p.setMetrics_base_snapshot_id(b);
    p.addToNdv_bases(new TIcebergNdvBase(1, b, statsFile(t.table(), b).path()));

    for (long hmsNdv : new long[] {3, 4}) {
      Object tsStats = cls.getConstructor().newInstance();
      cls.getMethod("setNumNulls", long.class).invoke(tsStats, 0L);
      cls.getMethod("setNumDVs", long.class).invoke(tsStats, hmsNdv);
      ColumnStatisticsData data = new ColumnStatisticsData();
      ColumnStatisticsData.class.getMethod("setTimestampStats", cls)
          .invoke(data, tsStats);
      hms_.setStoredStats(Collections.singletonMap("ts",
          new ColumnStatisticsObj("ts", "timestamp", data)));
      Column col = tbl.getColumn("ts");
      assertTrue(col.getStats().update("ts", col.getType(), data));

      Outcome o = reconcile(tbl, params);

      assertEquals("HMS NDV " + hmsNdv, hmsNdv == 3 ? Verdict.WRITE
          : Verdict.BASE_MISMATCH, o.verdict);
    }
  }

  @Test
  public void repeatAfterWriteIsAlreadyAppliedUntilHmsNdvDiffers() throws Exception {
    IcebergTestTables t = create();
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    long b = base(t, tbl, TIcebergStatsMode.FULL, COLUMNS);
    long s1 = t.append(t.file(100, null));
    TAlterTableUpdateStatsParams params =
        incremental(s1, b, 100, COLUMNS, bases(t, b, THETA));
    // Leaves the state of a written request: ids at S1, Impala sketches at S1, HMS NDV.
    apply(tbl, params(TIcebergStatsMode.FULL, s1, 100));
    syncFromHms(tbl);

    assertEquals(Verdict.ALREADY_APPLIED, reconcile(tbl, params).verdict);

    Map<String, ColumnStatisticsObj> stored = hmsStats();
    stored.get("s").getStatsData().getStringStats().setNumDVs(11);
    hms_.setStoredStats(stored);
    assertEquals(Verdict.CONFLICT, reconcile(tbl, params).verdict);
  }

  @Test
  public void foreignBlobsAtS1AreConflictNotAlreadyApplied() throws Exception {
    IcebergTestTables t = create();
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    long b = base(t, tbl, TIcebergStatsMode.FULL, COLUMNS);
    long s1 = t.append(t.file(100, null));
    TAlterTableUpdateStatsParams params =
        incremental(s1, b, 100, COLUMNS, bases(t, b, THETA));
    apply(tbl, params(TIcebergStatsMode.FULL, s1, 100));
    syncFromHms(tbl);
    Map<String, ColumnStatisticsObj> stored = hmsStats();
    // Another engine replaced the file of S1; its ndv equals the one HMS holds.
    List<PuffinFixtures.BlobSpec> foreign = new ArrayList<>();
    for (String col : THETA) {
      String ndv = col.equals("b") ? "2" : Long.toString(hmsNdv(stored.get(col)));
      foreign.add(PuffinFixtures.blob(fieldId(col), s1, sketchBytes(col))
          .ndvProperty(ndv));
    }
    register(t, s1, foreign.toArray(new PuffinFixtures.BlobSpec[0]));

    assertEquals(Verdict.CONFLICT, reconcile(tbl, params).verdict);
  }

  @Test
  public void expiredMetricsBaseIsConflictEvenWithRegisteredNdvBase() throws Exception {
    IcebergTestTables t = create();
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    long b0 = base(t, tbl, TIcebergStatsMode.FULL, COLUMNS);
    long b = base(t, tbl, TIcebergStatsMode.LEGACY, COLUMNS);
    long s1 = t.append(t.file(100, null));
    TAlterTableUpdateStatsParams params =
        incremental(s1, b, 100, COLUMNS, bases(t, b0, THETA));
    assertEquals(Verdict.WRITE, reconcile(tbl, params).verdict);

    t.table().expireSnapshots().expireSnapshotId(b).cleanExpiredFiles(false).commit();

    assertEquals(Verdict.CONFLICT, reconcile(tbl, params).verdict);
    assertTrue(callsUnderTest().isEmpty());
  }

  @Test
  public void onlyBooleanAfterLostHmsWriteIsAlreadyApplied() throws Exception {
    IcebergTestTables t = create();
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    long b = base(t, tbl, TIcebergStatsMode.FULL, COLUMNS);
    Map<String, ColumnStatisticsObj> beforeS1 = hmsStats();
    long s1 = t.append(t.file(100, null));
    List<String> cols = Collections.singletonList("b");
    TAlterTableUpdateStatsParams params =
        incremental(s1, b, 100, cols, bases(t, b, cols));
    // The Iceberg commit of S1 succeeded, its HMS write did not: HMS keeps base stats.
    apply(tbl, params(TIcebergStatsMode.FULL, s1, 100, cols));
    hms_.setStoredStats(beforeS1);

    assertEquals(Verdict.ALREADY_APPLIED, reconcile(tbl, params).verdict);
  }

  @Test
  public void conflictWhenBaseFileUnregisteredOrIdsMoved() throws Exception {
    IcebergTestTables t = create();
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    long b = base(t, tbl, TIcebergStatsMode.FULL, COLUMNS);
    long s1 = t.append(t.file(100, null));
    TAlterTableUpdateStatsParams params =
        incremental(s1, b, 100, COLUMNS, bases(t, b, THETA));
    StatisticsFile baseFile = statsFile(t.table(), b);
    t.table().updateStatistics().removeStatistics(b).commit();

    assertEquals(Verdict.CONFLICT, reconcile(tbl, params).verdict);
    assertTrue(callsUnderTest().isEmpty());

    t.table().updateStatistics().setStatistics(baseFile).commit();
    assertEquals(Verdict.WRITE, reconcile(tbl, params).verdict);
    // Another COMPUTE STATS moved the metrics base of "i" after the analysis.
    base(t, tbl, TIcebergStatsMode.LEGACY, Collections.singletonList("i"));

    assertEquals(Verdict.CONFLICT, reconcile(tbl, params).verdict);
    assertTrue(callsUnderTest().isEmpty());
  }

  @Test
  public void malformedRequestIsRejected() throws Exception {
    IcebergTestTables t = create();
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    long b = base(t, tbl, TIcebergStatsMode.FULL, COLUMNS);
    long s1 = t.append(t.file(100, null));
    String prefix = "Invalid INCREMENTAL statistics request for table " + DB + ".";
    String name = tbl.getName();

    TAlterTableUpdateStatsParams params =
        incremental(s1, b, 100, COLUMNS, bases(t, b, THETA));
    params.getIceberg_puffin_stats().unsetMetrics_base_snapshot_id();
    assertRejected(tbl, params, prefix + name + ": no metrics_base_snapshot_id");

    params = incremental(s1, b, 100, COLUMNS, bases(t, b, THETA));
    params.getIceberg_puffin_stats().unsetNdv_bases();
    assertRejected(tbl, params, prefix + name + ": no ndv_bases");

    params = incremental(s1, b, 100, COLUMNS, bases(t, b, THETA));
    params.getIceberg_puffin_stats().getNdv_bases().remove(0);
    assertRejected(tbl, params, prefix + name
        + ": field ids of ndv_bases [2, 3, 5] differ from theta_field_ids [1, 2, 3, 5]");

    params = incremental(s1, b, 100, COLUMNS, bases(t, b, THETA));
    List<TIcebergNdvBase> nb = params.getIceberg_puffin_stats().getNdv_bases();
    nb.set(1, nb.get(0).deepCopy());
    assertRejected(tbl, params, prefix + name + ": field id 1 repeated in ndv_bases");

    List<String> one = Collections.singletonList("i");
    params = incremental(s1, b, 100, one, bases(t, b, one));
    params.getIceberg_puffin_stats().addToTheta_field_ids(fieldId("i"));
    assertRejected(tbl, params,
        prefix + name + ": field id 1 repeated in theta_field_ids");
  }

  @Test
  public void columnStatsReadFailureIsThrownAsIs() throws Exception {
    IcebergTestTables t = create();
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    long b = base(t, tbl, TIcebergStatsMode.FULL, COLUMNS);
    long s1 = t.append(t.file(100, null));
    TAlterTableUpdateStatsParams params =
        incremental(s1, b, 100, COLUMNS, bases(t, b, THETA));
    hms_.failAt(hms_.calls().size() + 1);

    try {
      reconcile(tbl, params);
      fail("reconcile succeeded");
    } catch (ImpalaException e) {
      assertEquals("Injected HMS failure of call " + hms_.calls().size(), e.getMessage());
    }
  }

  @Test
  public void finalStatsMergesByFormulasAndCapsNdvByRowCount() throws Exception {
    IcebergTestTables t = create();
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    long b = base(t, tbl, TIcebergStatsMode.FULL, COLUMNS);
    t.setSummary(b, Collections.singletonMap("total-records", "20"));
    long s1 = t.append(t.file(100, null));
    TAlterTableUpdateStatsParams params =
        incremental(s1, b, 100, COLUMNS, bases(t, b, THETA));
    assertEquals(Verdict.WRITE, reconcile(tbl, params).verdict);

    Map<Integer, IcebergStatsUpdater.ColumnResult> res = finalStats(tbl, params);

    assertEquals(COLUMNS.size(), res.size());
    IcebergStatsUpdater.ColumnResult s = res.get(fieldId("s"));
    // Base: 1 null, avg 2.5, max 3 of 20 rows; gap: 2 nulls, 8 values, avg 4, max 5.
    assertEquals(3, s.stats.getNum_nulls());
    assertEquals(5, s.stats.getMax_size());
    assertEquals((2.5 * 19 + 4.0 * 8) / 27, s.stats.getAvg_size(), 1e-9);
    assertEquals(14L, s.ndv);
    assertEquals(14L, s.hmsStats.getStatsData().getStringStats().getNumDVs());
    assertSketchOf("s", union(values("s"), GAP.get("s")), s.sketch);
    IcebergStatsUpdater.ColumnResult i = res.get(fieldId("i"));
    assertEquals(15L, i.ndv);
    assertEquals(15L, hmsNdv(i.hmsStats));
    assertSketchOf("i", union(values("i"), GAP.get("i")), i.sketch);
    IcebergStatsUpdater.ColumnResult bool = res.get(fieldId("b"));
    assertEquals(10, bool.stats.getNum_trues());
    assertEquals(4, bool.stats.getNum_falses());
    assertEquals(2L, bool.ndv);
    // DECIMAL(1,0) has ten values: HMS and the blob store the capped NDV.
    assertEquals(10L, res.get(fieldId("d")).ndv);
    assertEquals(22L, estimate(res.get(fieldId("d")).sketch));
    IcebergStatsUpdater.ColumnResult bin = res.get(fieldId("bin"));
    assertNull(bin.sketch);
    assertEquals(-1L, bin.ndv);
    assertEquals(3, bin.stats.getNum_nulls());

    params.getTable_stats().setNum_rows(1);
    res = finalStats(tbl, params);
    for (String col : THETA) assertEquals(col, 1L, res.get(fieldId(col)).ndv);
    assertEquals(15L, estimate(res.get(fieldId("i")).sketch));
    assertEquals(1L, hmsNdv(res.get(fieldId("i")).hmsStats));
  }

  @Test
  public void baseAtS1KeepsItsStatistics() throws Exception {
    IcebergTestTables t = create();
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    long s1 = base(t, tbl, TIcebergStatsMode.FULL, COLUMNS);
    TAlterTableUpdateStatsParams params =
        incremental(s1, s1, 100, COLUMNS, bases(t, s1, THETA));
    TIcebergPuffinStatsParams p = params.getIceberg_puffin_stats();
    for (String col : COLUMNS) {
      int size = col.equals("s") || col.equals("bin") ? 0 : 4;
      long bools = col.equals("b") ? 0 : -1;
      params.putToColumn_stats(col, new TColumnStats(size, size, 0, 0, bools, bools));
      if (THETA.contains(col)) {
        p.putToTheta_sketches(fieldId(col), ByteBuffer.wrap(new byte[0]));
      }
    }

    assertEquals(Verdict.WRITE, reconcile(tbl, params).verdict);
    Map<Integer, IcebergStatsUpdater.ColumnResult> res = finalStats(tbl, params);

    IcebergStatsUpdater.ColumnResult s = res.get(fieldId("s"));
    assertEquals(12L, s.ndv);
    assertArrayEquals(sketchBytes("s"), s.sketch);
    assertEquals(1, s.stats.getNum_nulls());
    assertEquals(2.5, s.stats.getAvg_size(), 1e-9);
    assertEquals(3, s.stats.getMax_size());
  }

  @Test
  public void lowValueOfOtherTypeLeavesRangeUnset() throws Exception {
    IcebergTestTables t = create();
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    long b = base(t, tbl, TIcebergStatsMode.FULL, COLUMNS);
    long s1 = t.append(t.file(100, null));
    List<String> cols = Collections.singletonList("i");
    TAlterTableUpdateStatsParams params =
        incremental(s1, b, 100, cols, bases(t, b, cols));
    TColumnStats cached = tbl.getColumn("i").getStats().toThrift();
    cached.setLow_value(new TColumnValue().setLong_val(1));
    cached.setHigh_value(new TColumnValue().setLong_val(10));
    tbl.getColumn("i").getStats().update(tbl.getColumn("i").getType(), cached);
    TColumnStats gap = params.getColumn_stats().get("i");
    gap.setLow_value(new TColumnValue().setInt_val(6));
    gap.setHigh_value(new TColumnValue().setInt_val(15));

    IcebergStatsUpdater.ColumnResult i = finalStats(tbl, params).get(fieldId("i"));

    assertFalse(i.stats.isSetLow_value());
    assertFalse(i.stats.isSetHigh_value());
    assertFalse(i.hmsStats.getStatsData().getLongStats().isSetLowValue());
    assertEquals(15L, i.ndv);
  }

  @Test
  public void missingBaseBlobIsReported() throws Exception {
    IcebergTestTables t = create();
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    long b = base(t, tbl, TIcebergStatsMode.FULL, Collections.singletonList("s"));
    long s1 = t.append(t.file(100, null));
    List<String> cols = Arrays.asList("i", "s");
    String path = statsFile(t.table(), b).path();
    Map<String, TIcebergNdvBase> bases = new LinkedHashMap<>();
    bases.put("i", new TIcebergNdvBase(fieldId("i"), b, path));
    bases.put("s", new TIcebergNdvBase(fieldId("s"), b, path));

    assertFinalStatsFails(tbl, incremental(s1, b, 100, cols, bases),
        "Puffin statistics file " + path + " has no theta blob for column i (snapshot "
        + b + "); run COMPUTE STATS.");
  }

  @Test
  public void corruptBaseOrGapSketchNamesColumnAndSnapshot() throws Exception {
    IcebergTestTables t = create();
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    long b = base(t, tbl, TIcebergStatsMode.LEGACY, COLUMNS);
    register(t, b, PuffinFixtures.blob(fieldId("i"), b, CORRUPT),
        PuffinFixtures.blob(fieldId("s"), b, sketch("s", values("s"))));
    long s1 = t.append(t.file(100, null));
    List<String> cols = Arrays.asList("i", "s");

    assertFinalStatsFails(tbl, incremental(s1, b, 100, cols, bases(t, b, cols)),
        "Invalid theta sketch for column i (snapshot " + b + ")");

    TAlterTableUpdateStatsParams params = incremental(s1, b, 100,
        Collections.singletonList("s"), bases(t, b, Collections.singletonList("s")));
    params.getIceberg_puffin_stats().putToTheta_sketches(fieldId("s"),
        ByteBuffer.wrap(CORRUPT));
    assertFinalStatsFails(tbl, params,
        "Invalid theta sketch for column s (snapshot " + s1 + ")");
  }

  @Test
  public void noThetaColumnsWritesWithoutSketches() throws Exception {
    IcebergTestTables t = create();
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    long b = base(t, tbl, TIcebergStatsMode.FULL, COLUMNS);
    long s1 = t.append(t.file(100, null));
    List<String> cols = Collections.singletonList("bin");
    TAlterTableUpdateStatsParams params =
        incremental(s1, b, 100, cols, Collections.emptyMap());
    assertTrue(params.getIceberg_puffin_stats().isSetNdv_bases());
    // Reading the statistics file of the metrics base would now fail.
    t.table().io().deleteFile(statsFile(t.table(), b).path());

    assertEquals(Verdict.WRITE, reconcile(tbl, params).verdict);
    IcebergStatsUpdater.ColumnResult bin = finalStats(tbl, params).get(fieldId("bin"));

    assertNull(bin.sketch);
    assertEquals(-1L, bin.ndv);
    assertTrue(bin.hmsStats.getStatsData().isSetBinaryStats());
  }

  private Outcome reconcile(IcebergTable tbl, TAlterTableUpdateStatsParams params)
      throws ImpalaException {
    TIcebergPuffinStatsParams p = params.getIceberg_puffin_stats();
    // As apply() passes it after takeStoredIds: the stored ids of the cached table.
    org.apache.hadoop.hive.metastore.api.Table msTbl = tbl.getMetaStoreTable().deepCopy();
    String ids = hms_.readTableParameters(msTbl).get(IDS);
    if (ids != null) msTbl.putToParameters(IDS, ids);
    Set<Integer> fieldIds = new TreeSet<>(p.getColumn_field_ids().values());
    mark_ = hms_.calls().size();
    tbl.writeLock().lock();
    try {
      return IcebergIncrementalReconciler.reconcile(tbl, IcebergUtil.loadTable(tbl),
          msTbl, p, fieldIds, hms_);
    } finally {
      tbl.releaseWriteLock();
    }
  }

  private Map<Integer, IcebergStatsUpdater.ColumnResult> finalStats(IcebergTable tbl,
      TAlterTableUpdateStatsParams params) throws ImpalaException {
    tbl.writeLock().lock();
    try {
      return IcebergIncrementalReconciler.finalStats(tbl, IcebergUtil.loadTable(tbl),
          params);
    } finally {
      tbl.releaseWriteLock();
    }
  }

  private List<Method> callsUnderTest() {
    List<Method> all = hms_.methods();
    return new ArrayList<>(all.subList(mark_, all.size()));
  }

  private void assertRejected(IcebergTable tbl, TAlterTableUpdateStatsParams params,
      String message) throws Exception {
    try {
      reconcile(tbl, params);
      fail("reconcile accepted: " + message);
    } catch (ImpalaException e) {
      assertEquals(message, e.getMessage());
    }
    assertTrue(callsUnderTest().isEmpty());
  }

  private void assertFinalStatsFails(IcebergTable tbl,
      TAlterTableUpdateStatsParams params, String message) throws Exception {
    try {
      finalStats(tbl, params);
      fail("finalStats succeeded: " + message);
    } catch (ImpalaException e) {
      assertEquals(message, e.getMessage());
    }
  }

  private static long estimate(String col, List<?> values) {
    return Math.round(PuffinFixtures.sketch(SCHEMA.findField(col).type(), values)
        .getEstimate());
  }

  private static long estimate(byte[] sketch) {
    return Math.round(CompactSketch.heapify(Memory.wrap(sketch), 9001).getEstimate());
  }

  @SafeVarargs
  private static List<Object> union(List<?>... lists) {
    List<Object> res = new ArrayList<>();
    for (List<?> l : lists) res.addAll(l);
    return res;
  }

  /** The sketch holds exactly the hashes of 'values' (exact mode). */
  private static void assertSketchOf(String col, List<?> values, byte[] sketch) {
    CompactSketch expected = PuffinFixtures.sketch(SCHEMA.findField(col).type(), values);
    assertArrayEquals(col, expected.toByteArray(), sketch);
  }
}
