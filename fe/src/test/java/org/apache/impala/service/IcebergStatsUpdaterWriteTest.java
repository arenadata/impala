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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.iceberg.FailingCommitTables;
import org.apache.impala.catalog.iceberg.FailingCommitTables.Failure;
import org.apache.impala.catalog.iceberg.FailingCommitTables.Mode;
import org.apache.impala.catalog.iceberg.IcebergPuffinStatsWriter;
import org.apache.impala.catalog.iceberg.IcebergTestTables;
import org.apache.impala.catalog.iceberg.PuffinFixtures;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.analysis.IcebergAnalysisFixture;
import org.apache.impala.service.FakeHmsStatsWriter.Method;
import org.apache.impala.thrift.TAlterTableUpdateStatsParams;
import org.apache.impala.thrift.TIcebergStatsMode;
import org.apache.impala.util.IcebergUtil;
import org.apache.impala.util.IcebergUtil.ComputeStatsSnapshotPropertyConverter;
import org.apache.impala.util.NoOpEventSequence;
import org.junit.Test;

/**
 * Write path of IcebergStatsUpdater#apply in LEGACY and FULL mode. Oracles: the table
 * metadata after refresh(), the calls recorded by FakeHmsStatsWriter and NDV values
 * computed from the sketches the test feeds in.
 */
public class IcebergStatsUpdaterWriteTest extends IcebergStatsUpdaterTestBase {
  private static final IcebergAnalysisFixture ifx =
      IcebergAnalysisFixture.create(feFixture_);

  @Test
  public void fullRegistersImpalaBlobsAndWritesBlobNdvToHms() throws Exception {
    IcebergTestTables t = create();
    long s1 = t.append(t.file(100, null));
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());

    assertEquals(5, apply(tbl, params(TIcebergStatsMode.FULL, s1, 100)));

    Table table = t.table();
    Map<Integer, Long> blobNdv = blobNdvs(table, s1, Arrays.asList("i", "s", "b", "d"));
    Map<String, ColumnStatisticsObj> hms = hmsStats();
    assertEquals(COLUMNS.size(), hms.size());
    assertEquals(10L, hmsNdv(hms.get("i")));
    assertEquals(12L, hmsNdv(hms.get("s")));
    assertEquals(10L, hmsNdv(hms.get("d")));
    for (String col : Arrays.asList("i", "s", "d")) {
      assertEquals(col, hmsNdv(hms.get(col)), (long) blobNdv.get(fieldId(col)));
    }
    assertEquals(2L, (long) blobNdv.get(fieldId("b")));
    assertTrue(hms.get("b").getStatsData().isSetBooleanStats());
    assertTrue(hms.get("bin").getStatsData().isSetBinaryStats());
    assertEquals("decimal(1,0)", hms.get("d").getColType());
    assertEquals(idsOf(COLUMNS, s1), hmsIds());
    assertFalse(table.properties().containsKey(IDS));
    assertEquals(Arrays.asList(Method.ALTER_TABLE_PROPERTIES, Method.UPDATE_COLUMN_STATS),
        hms_.methods());
  }

  @Test
  public void fullNdvCappedByRowCountIsSameInBlobAndHms() throws Exception {
    IcebergTestTables t = create();
    long s1 = t.append(t.file(100, null));
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());

    apply(tbl, params(TIcebergStatsMode.FULL, s1, 1));

    Map<Integer, Long> blobNdv =
        blobNdvs(t.table(), s1, Arrays.asList("i", "s", "b", "d"));
    Map<String, ColumnStatisticsObj> hms = hmsStats();
    for (String col : Arrays.asList("i", "s", "d")) {
      assertEquals(col, 1L, hmsNdv(hms.get(col)));
      assertEquals(col, 1L, (long) blobNdv.get(fieldId(col)));
    }
    assertEquals(1L, (long) blobNdv.get(fieldId("b")));
  }

  @Test
  public void fullWithOnlyBinaryWritesNoFile() throws Exception {
    IcebergTestTables t = create();
    long s1 = t.append(t.file(100, null));
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());

    assertEquals(1, apply(tbl,
        params(TIcebergStatsMode.FULL, s1, 100, Collections.singletonList("bin"))));

    assertTrue(t.table().statisticsFiles().isEmpty());
    assertEquals(idsOf(Collections.singletonList("bin"), s1), hmsIds());
    assertTrue(hmsStats().get("bin").getStatsData().isSetBinaryStats());
  }

  @Test
  public void legacyWritesHmsNdvWithoutPuffin() throws Exception {
    IcebergTestTables t = create();
    long s1 = t.append(t.file(100, null));
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());

    assertEquals(5, apply(tbl, params(TIcebergStatsMode.LEGACY, s1, 100)));

    assertTrue(t.table().statisticsFiles().isEmpty());
    assertEquals(LEGACY_NDV, hmsNdv(hmsStats().get("i")));
    assertEquals(idsOf(COLUMNS, s1), hmsIds());
  }

  @Test
  public void legacySampledRemovesColumnsFromIds() throws Exception {
    IcebergTestTables t = create();
    long s0 = t.append(t.file(100, null));
    long s1 = t.append(t.file(100, null));
    IcebergTable tbl =
        ifx.addPathB(t, Collections.singletonMap(IDS, "1-5:" + s0 + ",9:" + s0));
    TAlterTableUpdateStatsParams params =
        params(TIcebergStatsMode.LEGACY, s1, 100, Arrays.asList("i", "s"));
    params.getIceberg_puffin_stats().setSampled(true);

    assertEquals(2, apply(tbl, params));

    Map<Integer, Long> expected = idsOf(Arrays.asList("b", "bin", "d"), s0);
    expected.put(9, s0);
    assertEquals(expected, hmsIds());
    assertEquals(LEGACY_NDV, hmsNdv(hmsStats().get("s")));
  }

  @Test
  public void emptyTableWritesMinusOne() throws Exception {
    IcebergTestTables t = create();
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());

    apply(tbl, params(TIcebergStatsMode.LEGACY, -1, 0));

    assertEquals(idsOf(COLUMNS, -1), hmsIds());
    assertTrue(t.table().statisticsFiles().isEmpty());
  }

  @Test
  public void fullWritesEmptySketchForNullSketch() throws Exception {
    IcebergTestTables t = create();
    long s1 = t.append(t.file(100, null));
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    TAlterTableUpdateStatsParams params = params(TIcebergStatsMode.FULL, s1, 100);
    // The backend passes a NULL sketch as no bytes.
    params.getIceberg_puffin_stats().putToTheta_sketches(fieldId("i"),
        ByteBuffer.allocate(0));

    apply(tbl, params);

    Table table = t.table();
    StatisticsFile file = statsFile(table, s1);
    assertArrayEquals(sketch("i", Collections.emptyList()),
        PuffinFixtures.impalaThetaPayloads(table, file).get(fieldId("i")));
    assertEquals("0", blobsByField(file).get(fieldId("i")).properties()
        .get(IcebergPuffinStatsWriter.NDV));
    assertEquals(0L, hmsNdv(hmsStats().get("i")));
  }

  @Test
  public void fullThenLegacyKeepsImpalaBlob() throws Exception {
    IcebergTestTables t = create();
    long s1 = t.append(t.file(100, null));
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    apply(tbl, params(TIcebergStatsMode.FULL, s1, 100));
    List<StatisticsFile> afterFull = t.table().statisticsFiles();

    apply(tbl, params(TIcebergStatsMode.LEGACY, s1, 100));

    assertEquals(afterFull, t.table().statisticsFiles());
    assertEquals(LEGACY_NDV, hmsNdv(hmsStats().get("i")));
    assertEquals(idsOf(COLUMNS, s1), hmsIds());
  }

  @Test
  public void legacyThenFullRegistersBlobs() throws Exception {
    IcebergTestTables t = create();
    long s1 = t.append(t.file(100, null));
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    apply(tbl, params(TIcebergStatsMode.LEGACY, s1, 100));

    apply(tbl, params(TIcebergStatsMode.FULL, s1, 100));

    blobNdvs(t.table(), s1, Arrays.asList("i", "s", "b", "d"));
    assertEquals(10L, hmsNdv(hmsStats().get("i")));
    assertEquals(idsOf(COLUMNS, s1), hmsIds());
  }

  @Test
  public void snapshotAfterAnalysisKeepsStatsAtS1() throws Exception {
    IcebergTestTables t = create();
    long s1 = t.append(t.file(100, null));
    TAlterTableUpdateStatsParams params = params(TIcebergStatsMode.FULL, s1, 100);
    long s2 = t.append(t.file(100, null));
    // Event processing may reload the cached table before the request arrives.
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    assertEquals(s2, tbl.snapshotId());

    apply(tbl, params);

    Table table = t.table();
    assertEquals(s2, table.currentSnapshot().snapshotId());
    assertEquals(1, table.statisticsFiles().size());
    blobNdvs(table, s1, Arrays.asList("i", "s", "b", "d"));
    assertEquals(idsOf(COLUMNS, s1), hmsIds());
  }

  @Test
  public void typePromotedAfterLoadWritesFreshType() throws Exception {
    IcebergTestTables t = create();
    long s1 = t.append(t.file(100, null));
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    assertEquals("INT", tbl.getColumn("i").getType().toSql());
    t.table().updateSchema().updateColumn("i", Types.LongType.get()).commit();

    assertEquals(5, apply(tbl, params(TIcebergStatsMode.FULL, s1, 100)));

    Map<String, ColumnStatisticsObj> hms = hmsStats();
    assertEquals("bigint", hms.get("i").getColType());
    assertEquals(10L, hmsNdv(hms.get("i")));
    assertEquals(idsOf(COLUMNS, s1), hmsIds());
    blobNdvs(t.table(), s1, Arrays.asList("i", "s", "b", "d"));
  }

  /**
   * Another engine expires S1 after the load: the commit is rejected on the refreshed
   * metadata, which must fail like a load that no longer finds S1.
   */
  private void checkS1ExpiredAfterLoad(TIcebergStatsMode mode, boolean integrated)
      throws Exception {
    IcebergTestTables t = create();
    long s1 = t.append(t.file(100, null));
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    org.apache.hadoop.hive.metastore.api.Table msTbl =
        integrated ? integrated(tbl) : tbl.getMetaStoreTable().deepCopy();
    afterLoad_ = () -> {
      t.append(t.file(100, null));
      t.expire(s1);
    };
    try {
      apply(tbl, msTbl, params(mode, s1, 100));
      fail("no error");
    } catch (ImpalaRuntimeException e) {
      assertTrue(e.getMessage(), e.getMessage().endsWith("Snapshot " + s1 + " of table "
          + tbl.getFullName() + " no longer exists; run COMPUTE STATS again."));
    }
    t.table().refresh();
    assertTrue(t.table().statisticsFiles().isEmpty());
    assertFalse(t.table().properties().containsKey(IDS));
    File dir = new File(new URI(t.table().location() + "/metadata"));
    assertEquals(0, dir.listFiles((d, n) -> n.endsWith(".stats")).length);
    assertTrue(hms_.calls().isEmpty());
  }

  @Test
  public void s1ExpiredAfterLoadFailsFullWithoutWriting() throws Exception {
    checkS1ExpiredAfterLoad(TIcebergStatsMode.FULL, false);
  }

  @Test
  public void s1ExpiredAfterLoadFailsLegacyWithoutWriting() throws Exception {
    checkS1ExpiredAfterLoad(TIcebergStatsMode.LEGACY, true);
  }

  private void checkRepeatEqualsSingle(TIcebergStatsMode mode) throws Exception {
    IcebergTestTables t = create();
    long s1 = t.append(t.file(100, null));
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    apply(tbl, params(mode, s1, 100));
    List<StatisticsFile> once = t.table().statisticsFiles();
    Map<Integer, Long> idsOnce = hmsIds();
    Map<String, ColumnStatisticsObj> statsOnce = hmsStats();

    // A retried RPC finds the ids of the first call in the table it carries.
    apply(tbl, hms_.last(Method.ALTER_TABLE_PROPERTIES).msTbl.deepCopy(),
        params(mode, s1, 100));

    List<StatisticsFile> twice = t.table().statisticsFiles();
    assertEquals(once.size(), twice.size());
    if (mode == TIcebergStatsMode.FULL) {
      assertNotEquals(once.get(0).path(), twice.get(0).path());
      assertEquals(once.get(0).blobMetadata(), twice.get(0).blobMetadata());
      blobNdvs(t.table(), s1, Arrays.asList("i", "s", "b", "d"));
    }
    assertEquals(idsOnce, hmsIds());
    assertEquals(statsOnce, hmsStats());
  }

  @Test
  public void repeatedFullApplyEqualsSingle() throws Exception {
    checkRepeatEqualsSingle(TIcebergStatsMode.FULL);
  }

  @Test
  public void repeatedLegacyApplyEqualsSingle() throws Exception {
    checkRepeatEqualsSingle(TIcebergStatsMode.LEGACY);
  }

  private TAlterTableUpdateStatsParams repeatParams(TIcebergStatsMode mode, long s1,
      boolean sampled) {
    TAlterTableUpdateStatsParams params = params(mode, s1, 100);
    params.getIceberg_puffin_stats().setSampled(sampled);
    return params;
  }

  /**
   * Repeats a command on an integrated table with the msTbl the first call left, as a
   * retried RPC does. The sampled run starts with ids covering all columns, so the ids
   * after the command are empty.
   */
  private void checkIntegratedRepeatEqualsSingle(TIcebergStatsMode mode,
      boolean sampled) throws Exception {
    IcebergTestTables t = create();
    Map<String, String> initial = Collections.emptyMap();
    if (sampled) {
      long s0 = t.append(t.file(100, null));
      initial = Collections.singletonMap(IDS, "1-5:" + s0);
    }
    long s1 = t.append(t.file(100, null));
    IcebergTable tbl = ifx.addPathB(t, initial);
    org.apache.hadoop.hive.metastore.api.Table msTbl = integrated(tbl);
    apply(tbl, msTbl, repeatParams(mode, s1, sampled));
    t.table().refresh();
    String idsOnce = t.table().properties().get(IDS);
    List<StatisticsFile> once = t.table().statisticsFiles();
    Map<String, ColumnStatisticsObj> statsOnce = hmsStats();

    apply(tbl, msTbl, repeatParams(mode, s1, sampled));

    t.table().refresh();
    assertEquals(idsOnce, t.table().properties().get(IDS));
    assertEquals(sampled, idsOnce == null);
    assertEquals(statsOnce, hmsStats());
    assertEquals(once.size(), t.table().statisticsFiles().size());
    if (mode == TIcebergStatsMode.FULL) {
      assertEquals(once.get(0).blobMetadata(),
          statsFile(t.table(), s1).blobMetadata());
    }
    if (!sampled) {
      assertEquals(idsOf(COLUMNS, s1),
          ComputeStatsSnapshotPropertyConverter.stringToMap(idsOnce));
    }
    assertTrue(hms_.last(Method.ALTER_TABLE_PROPERTIES) == null);
  }

  @Test
  public void repeatedIntegratedFullApplyEqualsSingle() throws Exception {
    checkIntegratedRepeatEqualsSingle(TIcebergStatsMode.FULL, false);
  }

  @Test
  public void repeatedIntegratedLegacyApplyEqualsSingle() throws Exception {
    checkIntegratedRepeatEqualsSingle(TIcebergStatsMode.LEGACY, false);
  }

  @Test
  public void repeatedIntegratedLegacySampledApplyEqualsSingle() throws Exception {
    checkIntegratedRepeatEqualsSingle(TIcebergStatsMode.LEGACY, true);
  }

  private void checkInvalid(IcebergTable tbl, TAlterTableUpdateStatsParams params,
      String expected) {
    try {
      apply(tbl, params);
      fail("no error for " + expected);
    } catch (ImpalaException e) {
      assertTrue(e instanceof ImpalaRuntimeException);
      assertEquals(expected, e.getMessage());
    }
    assertEquals("table loaded before input checks", 0, loads_);
    assertTrue(hms_.calls().isEmpty());
  }

  private void checkInvalid(IcebergTable tbl, long s1, TIcebergStatsMode mode,
      Consumer<TAlterTableUpdateStatsParams> change, String what) {
    TAlterTableUpdateStatsParams params = params(mode, s1, 100);
    change.accept(params);
    checkInvalid(tbl, params, "Invalid Iceberg statistics parameters for table "
        + tbl.getFullName() + ": " + what);
  }

  @Test
  public void unknownCommitWithoutFileIgnoresForeignPropertiesOtherThanIds()
      throws Exception {
    IcebergTestTables t = create();
    long s1 = t.append(t.file(100, null));
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    org.apache.hadoop.hive.metastore.api.Table msTbl = integrated(tbl);
    msTbl.putToParameters(HdfsTable.TBL_PROP_LAST_COMPUTE_STATS_TIME, "100");
    FailingCommitTables.Rule rule = new FailingCommitTables.Rule();
    rule.failAt(1, Failure.STATE_UNKNOWN, Mode.AFTER_APPLY);
    int[] loads = {0};
    IcebergStatsUpdater updater = new IcebergStatsUpdater(feTbl -> {
      // Another engine changes a property between the commit and the reload.
      if (++loads[0] == 2) {
        t.table().updateProperties()
            .set(HdfsTable.TBL_PROP_LAST_COMPUTE_STATS_TIME, "foreign").commit();
      }
      org.apache.iceberg.Table real = IcebergUtil.loadTable(feTbl);
      return new BaseTable(new FailingCommitTables(
          ((HasTableOperations) real).operations(), rule), real.name());
    });

    tbl.writeLock().lock();
    try {
      updater.apply(tbl, msTbl, params(TIcebergStatsMode.LEGACY, s1, 100), hms_,
          NoOpEventSequence.INSTANCE);
    } finally {
      tbl.releaseWriteLock();
    }

    t.table().refresh();
    assertEquals(2, loads[0]);
    assertEquals(idsOf(COLUMNS, s1),
        ComputeStatsSnapshotPropertyConverter.stringToMap(
            t.table().properties().get(IDS)));
    assertEquals("foreign",
        t.table().properties().get(HdfsTable.TBL_PROP_LAST_COMPUTE_STATS_TIME));
    assertEquals(Collections.singletonList(Method.UPDATE_COLUMN_STATS), hms_.methods());
  }

  @Test
  public void invalidParamsFailBeforeLoad() throws Exception {
    IcebergTestTables t = create();
    long s1 = t.append(t.file(100, null));
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    TIcebergStatsMode full = TIcebergStatsMode.FULL;
    TIcebergStatsMode legacy = TIcebergStatsMode.LEGACY;

    checkInvalid(tbl, s1, TIcebergStatsMode.INCREMENTAL,
        p -> p.getIceberg_puffin_stats().setSampled(true),
        "sampled statistics in INCREMENTAL mode");
    checkInvalid(tbl, s1, full,
        p -> p.getIceberg_puffin_stats().putToColumn_field_ids("x", 9),
        "keys of column_field_ids [b, bin, d, i, s, x] differ from keys of column_stats "
            + "[b, bin, d, i, s]");
    checkInvalid(tbl, s1, legacy,
        p -> p.getIceberg_puffin_stats().addToTheta_field_ids(1),
        "LEGACY mode with theta sketches");
    checkInvalid(tbl, s1, full,
        p -> p.getIceberg_puffin_stats().putToTheta_sketches(4, ByteBuffer.allocate(0)),
        "theta sketch of field id 4 not in theta_field_ids");
    checkInvalid(tbl, s1, full,
        p -> p.getIceberg_puffin_stats().getTheta_sketches().remove(2),
        "no theta sketch for field id 2");
    checkInvalid(tbl, s1, full,
        p -> p.getIceberg_puffin_stats().unsetTheta_sketches(),
        "no theta sketch for field id 1");
    checkInvalid(tbl, s1, full, p -> {
          p.getIceberg_puffin_stats().addToTheta_field_ids(8);
          p.getIceberg_puffin_stats().putToTheta_sketches(8, ByteBuffer.allocate(0));
        }, "theta field id 8 not in column_field_ids");
    checkInvalid(tbl, s1, full,
        p -> p.getIceberg_puffin_stats().setSampled(true),
        "sampled statistics in FULL mode");
    checkInvalid(tbl, -1, full, p -> {}, "theta sketches without a snapshot");
  }
}
