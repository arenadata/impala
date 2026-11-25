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
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiConsumer;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.util.Pair;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.iceberg.FailingCommitTables;
import org.apache.impala.catalog.iceberg.FailingCommitTables.Failure;
import org.apache.impala.catalog.iceberg.FailingCommitTables.Mode;
import org.apache.impala.catalog.iceberg.FailingFileIO;
import org.apache.impala.catalog.iceberg.IcebergPuffinStatsWriter;
import org.apache.impala.catalog.iceberg.IcebergTestTables;
import org.apache.impala.catalog.iceberg.PuffinFixtures;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.service.FakeHmsStatsWriter.Method;
import org.apache.impala.thrift.TAlterTableUpdateStatsParams;
import org.apache.impala.thrift.TIcebergNdvBase;
import org.apache.impala.thrift.TIcebergStatsMode;
import org.apache.impala.util.IcebergUtil;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;

/**
 * IcebergStatsUpdater#apply with incremental requests: the merged statistics it
 * writes, repeated requests, and the compensation of a base that does not match HMS,
 * with and without HMS integration. Bases are written by apply, so the snapshot ids
 * come from the property it stores.
 */
public class IcebergStatsUpdaterIncrementalTest extends IcebergStatsUpdaterIncrementalTestBase {
  /** Columns of the mismatch cases: HMS disagrees with the base NDV of "i" and "s". */
  private static final List<String> MISMATCH_COLS = Arrays.asList("i", "s", "b");
  private static final String OTHER_BLOB_TYPE = "other-engine-blob-v1";
  private static final String LAST_COMPUTE_STATS_TIME =
      HdfsTable.TBL_PROP_LAST_COMPUTE_STATS_TIME;
  /** Table statistics stored before the mismatch cases. */
  private static final Map<String, String> OLD_TABLE_STATS = ImmutableMap.of(
      StatsSetupConst.ROW_COUNT, "100", StatsSetupConst.TOTAL_SIZE, "10000",
      LAST_COMPUTE_STATS_TIME, "1000", StatsSetupConst.COLUMN_STATS_ACCURATE,
      "{\"BASIC_STATS\":\"true\"}");

  private final IcebergStatsUpdater plain_ =
      new IcebergStatsUpdater(IcebergUtil::loadTable);

  @Test
  public void writeStoresUnionAtS1AndRepeatIsAlreadyAppliedWithoutHmsIntegration()
      throws Exception {
    checkWriteAndRepeat(false);
  }

  @Test
  public void writeStoresUnionAtS1AndRepeatIsAlreadyAppliedWithHmsIntegration()
      throws Exception {
    checkWriteAndRepeat(true);
  }

  private void checkWriteAndRepeat(boolean integration) throws Exception {
    IcebergTestTables t = create();
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    long b = fullBase(t, tbl, integration);
    StatisticsFile baseFile = statsFile(t.table(), b);
    long s1 = t.append(t.file(100, null));
    TAlterTableUpdateStatsParams params =
        incremental(s1, b, 100, COLUMNS, bases(t, b, THETA));
    int mark = hms_.calls().size();

    assertEquals(COLUMNS.size(), apply(tbl, msTbl(tbl, integration), params));

    Map<Integer, Long> ndvs = new TreeMap<>();
    ndvs.put(fieldId("i"), 15L);
    ndvs.put(fieldId("s"), 14L);
    ndvs.put(fieldId("b"), 2L);
    // HMS caps the NDV of DECIMAL(1,0): the blob stores the same number.
    ndvs.put(fieldId("d"), 10L);
    assertEquals(ndvs, impalaNdvs(t, s1));
    assertUnionAtS1(t, s1, 0);
    Map<String, ColumnStatisticsObj> hms = hmsStats();
    for (String col : Arrays.asList("i", "s", "d")) {
      assertEquals(col, (long) ndvs.get(fieldId(col)), hmsNdv(hms.get(col)));
    }
    assertTrue(hms.get("b").getStatsData().isSetBooleanStats());
    assertEquals(idsOf(COLUMNS, s1), storedIds(t, tbl, integration));
    assertEquals(integration, !callsAfter(mark).contains(Method.ALTER_TABLE_PROPERTIES));
    if (!integration) assertEquals(idsOf(COLUMNS, s1), hmsIds());
    assertEquals(baseFile, statsFile(t.table(), b));

    // The repeated RPC finds its result and writes nothing.
    syncFromHms(tbl);
    List<StatisticsFile> files = t.table().statisticsFiles();
    mark = hms_.calls().size();
    assertEquals(COLUMNS.size(), apply(tbl, msTbl(tbl, integration), params));
    assertEquals(Collections.singletonList(Method.READ_COLUMN_STATS), callsAfter(mark));
    assertEquals(files, t.table().statisticsFiles());
    assertEquals(idsOf(COLUMNS, s1), storedIds(t, tbl, integration));
  }

  /**
   * The Impala blobs at 's1' hold the union of the base and the gap sketches; 'foreign'
   * blobs of other engines come on top.
   */
  private static void assertUnionAtS1(IcebergTestTables t, long s1, int foreign)
      throws Exception {
    Map<List<Object>, byte[]> payloads = payloads(t, s1);
    assertEquals(THETA.size() + foreign, payloads.size());
    for (String col : THETA) {
      List<Object> values = new ArrayList<>(values(col));
      values.addAll(GAP.get(col));
      assertArrayEquals(col, sketch(col, values), payloads.get(theta(col, true)));
    }
  }

  @Test
  public void commitAfterAnalysisWritesAtS1WithoutHmsIntegration() throws Exception {
    checkCommitAfterAnalysis(false);
  }

  @Test
  public void commitAfterAnalysisWritesAtS1WithHmsIntegration() throws Exception {
    checkCommitAfterAnalysis(true);
  }

  /** A data commit S2 between the analysis at S1 and the RPC leaves the result at S1. */
  private void checkCommitAfterAnalysis(boolean integration) throws Exception {
    IcebergTestTables t = create();
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    long b = fullBase(t, tbl, integration);
    long s1 = t.append(t.file(100, null));
    byte[] trinoBin = sketch("s", Arrays.asList("p", "q"));
    register(t, s1, PuffinFixtures.blob(fieldId("bin"), s1, trinoBin));
    TAlterTableUpdateStatsParams params =
        incremental(s1, b, 100, COLUMNS, bases(t, b, THETA));
    long s2 = t.append(t.file(100, null));

    assertEquals(COLUMNS.size(), apply(tbl, msTbl(tbl, integration), params));

    assertTrue(t.table().statisticsFiles().stream().noneMatch(f -> f.snapshotId() == s2));
    assertUnionAtS1(t, s1, 1);
    assertArrayEquals(trinoBin, payloads(t, s1).get(theta("bin", false)));
    assertEquals(new TreeSet<>(idsOf(THETA, s1).keySet()), impalaNdvs(t, s1).keySet());
    // The base average weighs as the rows of B less one null; the gap one as 8 values.
    long baseValues = Long.parseLong(t.table().snapshot(b).summary()
        .get(SnapshotSummary.TOTAL_RECORDS_PROP)) - 1;
    assertEquals((2.5 * baseValues + 4 * 8) / (baseValues + 8),
        hmsStats().get("s").getStatsData().getStringStats().getAvgColLen(), 1e-9);
    assertEquals(idsOf(COLUMNS, s1), storedIds(t, tbl, integration));
    if (!integration) assertEquals(idsOf(COLUMNS, s1), hmsIds());

    syncFromHms(tbl);
    List<StatisticsFile> files = t.table().statisticsFiles();
    int mark = hms_.calls().size();
    assertEquals(COLUMNS.size(), apply(tbl, msTbl(tbl, integration), params));
    assertEquals(Collections.singletonList(Method.READ_COLUMN_STATS), callsAfter(mark));
    assertEquals(files, t.table().statisticsFiles());
    assertEquals(idsOf(COLUMNS, s1), storedIds(t, tbl, integration));
  }

  @Test
  public void foreignBlobsOfS1AreCopiedByteForByte() throws Exception {
    IcebergTestTables t = create();
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    long b = base(t, tbl, TIcebergStatsMode.FULL, COLUMNS);
    long s1 = t.append(t.file(100, null));
    byte[] trinoBin = sketch("s", Arrays.asList("p", "q"));
    byte[] other = {1, 2, 3, 4};
    register(t, s1,
        PuffinFixtures.blob(fieldId("b"), s1, sketch("b", Arrays.asList(true, false))),
        PuffinFixtures.blob(fieldId("bin"), s1, trinoBin),
        PuffinFixtures.blob(fieldId("i"), s1, other).type(OTHER_BLOB_TYPE));
    // The NDV base of "b" is the blob of another engine at S1: no files after it.
    Map<String, TIcebergNdvBase> bases = bases(t, b, THETA);
    bases.put("b", ndvBase(t, "b", s1));
    TAlterTableUpdateStatsParams params = incremental(s1, b, 100, COLUMNS, bases);
    params.getIceberg_puffin_stats().putToTheta_sketches(fieldId("b"),
        ByteBuffer.wrap(new byte[0]));

    assertEquals(COLUMNS.size(), apply(tbl, params));

    Map<List<Object>, byte[]> payloads = payloads(t, s1);
    assertEquals(THETA.size() + 2, payloads.size());
    assertArrayEquals(trinoBin, payloads.get(theta("bin", false)));
    assertArrayEquals(other,
        payloads.get(Arrays.asList(OTHER_BLOB_TYPE, fieldId("i"), false)));
    assertArrayEquals(sketch("b", Arrays.asList(true, false)),
        payloads.get(theta("b", true)));
    assertEquals(2L, (long) impalaNdvs(t, s1).get(fieldId("b")));
    assertEquals(idsOf(COLUMNS, s1), storedIds(t, tbl, false));
  }

  @Test
  public void baseMismatchRemovesIdsOfColumnsWithoutHmsIntegration() throws Exception {
    checkBaseMismatch(false);
  }

  @Test
  public void baseMismatchRemovesIdsOfColumnsWithHmsIntegration() throws Exception {
    checkBaseMismatch(true);
  }

  @Test
  public void failedCompensationWithoutHmsIntegrationKeepsIds() throws Exception {
    checkFailedCompensation(false);
  }

  @Test
  public void failedCompensationWithHmsIntegrationKeepsIds() throws Exception {
    checkFailedCompensation(true);
  }

  @Test
  public void baseMismatchMessageListsFirstTenColumns() {
    List<String> cols = new ArrayList<>();
    for (int i = 1; i <= 10; ++i) cols.add("c" + i);
    String ten = "c1, c2, c3, c4, c5, c6, c7, c8, c9, c10";
    assertEquals(mismatch("db.t", ten),
        new IcebergStatsBaseMismatchException("db.t", cols).getMessage());
    cols.add("c11");
    assertEquals(mismatch("db.t", ten + ", …"),
        new IcebergStatsBaseMismatchException("db.t", cols).getMessage());
  }

  @Test
  public void baseUnregisteredBeforeFreshLoadIsConflict() throws Exception {
    checkBaseGoneBeforeFreshLoad(
        (t, b) -> t.table().updateStatistics().removeStatistics(b).commit());
  }

  @Test
  public void baseExpiredBeforeFreshLoadIsConflict() throws Exception {
    checkBaseGoneBeforeFreshLoad(IcebergTestTables::expire);
  }

  /** Another engine drops the base 'b' after the analysis, before the fresh load. */
  private void checkBaseGoneBeforeFreshLoad(BiConsumer<IcebergTestTables, Long> drop)
      throws Exception {
    IcebergTestTables t = create();
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    long b = base(t, tbl, TIcebergStatsMode.FULL, COLUMNS);
    long s1 = t.append(t.file(100, null));
    TAlterTableUpdateStatsParams params =
        incremental(s1, b, 100, COLUMNS, bases(t, b, THETA));
    IcebergStatsUpdater updater = new IcebergStatsUpdater(feTbl -> {
      drop.accept(t, b);
      return IcebergUtil.loadTable(feTbl);
    });
    int mark = hms_.calls().size();

    assertFails(() -> apply(updater, tbl, msTbl(tbl, false), params),
        ImpalaRuntimeException.class, conflict(tbl));

    assertTrue(t.table().statisticsFiles().isEmpty());
    assertEquals(idsOf(COLUMNS, b), storedIds(t, tbl, false));
    assertTrue(callsAfter(mark).isEmpty());
  }

  @Test
  public void unreadableBaseFileWritesNothing() throws Exception {
    checkBaseFileFailure(false, (t, io, path, b) -> {
      io.failPath(FailingFileIO.Op.READ, p -> p.equals(path));
      return "Failed to read Puffin statistics file " + path
          + ": Injected read failure: " + path;
    });
  }

  @Test
  public void baseFileWithoutThetaBlobWritesNothing() throws Exception {
    checkBaseFileFailure(true, (t, io, path, b) -> {
      // Another engine rewrites the file; its registered blob metadata stays.
      List<PuffinFixtures.BlobSpec> blobs = new ArrayList<>();
      for (String col : Arrays.asList("s", "b", "d")) {
        blobs.add(PuffinFixtures.blob(fieldId(col), b, sketchBytes(col)));
      }
      rewrite(t, path, PuffinFixtures.write(t.table(), b, blobs).path());
      return "Puffin statistics file " + path + " has no theta blob for column i "
          + "(snapshot " + b + "); run COMPUTE STATS.";
    });
  }

  private interface BaseFileFailure {
    /** Breaks the base file 'path' of snapshot 'b'; returns the expected error. */
    String inject(IcebergTestTables t, FailingFileIO io, String path, long b)
        throws Exception;
  }

  /**
   * A base file that cannot be read fails the incremental request before it writes the
   * Puffin file, the Iceberg metadata or HMS.
   */
  private void checkBaseFileFailure(boolean integration, BaseFileFailure failure)
      throws Exception {
    IcebergTestTables t = create();
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    long b = fullBase(t, tbl, integration);
    long s1 = t.append(t.file(100, null));
    String path = statsFile(t.table(), b).path();
    TAlterTableUpdateStatsParams params =
        incremental(s1, b, 100, COLUMNS, bases(t, b, THETA));
    FailingFileIO io = new FailingFileIO(t.table().io());
    String message = failure.inject(t, io, path, b);
    IcebergStatsUpdater updater = new IcebergStatsUpdater(feTbl -> {
      org.apache.iceberg.Table real = IcebergUtil.loadTable(feTbl);
      return new BaseTable(new FailingCommitTables(((HasTableOperations) real)
          .operations(), io, new FailingCommitTables.Rule()), real.name());
    });
    List<StatisticsFile> files = t.table().statisticsFiles();
    Set<String> onDisk = puffinFilesOnDisk(t);
    Map<String, String> props = t.table().properties();
    int mark = hms_.calls().size();

    assertFails(() -> apply(updater, tbl, msTbl(tbl, integration), params),
        ImpalaRuntimeException.class, message);

    assertEquals(files, t.table().statisticsFiles());
    assertTrue(files.stream().noneMatch(f -> f.snapshotId() == s1));
    assertEquals(onDisk, puffinFilesOnDisk(t));
    assertEquals(props, t.table().properties());
    assertEquals(idsOf(COLUMNS, b), storedIds(t, tbl, integration));
    assertEquals(Collections.singletonList(Method.READ_COLUMN_STATS), callsAfter(mark));
  }

  /** Replaces the content of 'path' with that of 'src' and deletes 'src'. */
  private static void rewrite(IcebergTestTables t, String path, String src)
      throws Exception {
    FileIO io = t.table().io();
    byte[] bytes;
    try (InputStream in = io.newInputFile(src).newStream()) {
      bytes = ByteStreams.toByteArray(in);
    }
    io.deleteFile(src);
    io.deleteFile(path);
    try (OutputStream out = io.newOutputFile(path).create()) {
      out.write(bytes);
    }
  }

  private void checkBaseMismatch(boolean integration) throws Exception {
    IcebergTestTables t = create();
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    tbl.getMetaStoreTable().getParameters().putAll(OLD_TABLE_STATS);
    long b = fullBase(t, tbl, integration);
    long s1 = t.append(t.file(100, null));
    mismatchHmsNdv();
    TAlterTableUpdateStatsParams params = mismatchRequest(t, s1, b);
    assertEquals(idsOf(COLUMNS, b), storedIds(t, tbl, integration));
    List<StatisticsFile> files = t.table().statisticsFiles();
    Map<String, String> hmsParams = withoutIds(hms_.readTableParameters(msTbl(tbl,
        integration)));
    String statsTime = t.table().properties().get(LAST_COMPUTE_STATS_TIME);
    int mark = hms_.calls().size();

    assertFails(() -> apply(tbl, requestMsTbl(tbl, integration), params),
        IcebergStatsBaseMismatchException.class, mismatch(tbl.getFullName(), "i, s"));

    Map<Integer, Long> rest = idsOf(Arrays.asList("bin", "d"), b);
    assertEquals(rest, storedIds(t, tbl, integration));
    assertEquals(files, t.table().statisticsFiles());
    List<Method> calls = callsAfter(mark);
    assertFalse(calls.contains(Method.UPDATE_COLUMN_STATS));
    assertEquals(!integration, calls.contains(Method.ALTER_TABLE_PROPERTIES));
    // The compensation keeps the table statistics of the failed request out of HMS and
    // of the Iceberg properties.
    if (integration) {
      assertEquals(OLD_TABLE_STATS.get(LAST_COMPUTE_STATS_TIME), statsTime);
      assertEquals(statsTime, t.table().properties().get(LAST_COMPUTE_STATS_TIME));
    } else {
      assertEquals(OLD_TABLE_STATS, Maps.filterKeys(hmsParams,
          OLD_TABLE_STATS::containsKey));
      assertEquals(hmsParams, withoutIds(hms_.readTableParameters(msTbl(tbl, false))));
      assertEquals(hmsParams, withoutIds(
          hms_.last(Method.ALTER_TABLE_PROPERTIES).msTbl.getParameters()));
    }

    // The repeated RPC no longer finds its base in the stored ids.
    mark = hms_.calls().size();
    assertFails(() -> apply(tbl, msTbl(tbl, integration), params),
        ImpalaRuntimeException.class, conflict(tbl));
    assertEquals(rest, storedIds(t, tbl, integration));
    assertEquals(files, t.table().statisticsFiles());
    assertTrue(callsAfter(mark).isEmpty());
  }

  private void checkFailedCompensation(boolean integration) throws Exception {
    IcebergTestTables t = create();
    IcebergTable tbl = ifx.addPathB(t, Collections.emptyMap());
    long b = fullBase(t, tbl, integration);
    long s1 = t.append(t.file(100, null));
    mismatchHmsNdv();
    TAlterTableUpdateStatsParams params = mismatchRequest(t, s1, b);
    FailingCommitTables.Rule rule = new FailingCommitTables.Rule();
    IcebergStatsUpdater updater;
    if (integration) {
      rule.failAt(1, Failure.STATE_UNKNOWN, Mode.BEFORE_APPLY);
      updater = new IcebergStatsUpdater(feTbl -> {
        org.apache.iceberg.Table real = IcebergUtil.loadTable(feTbl);
        return new BaseTable(new FailingCommitTables(
            ((HasTableOperations) real).operations(), rule), real.name());
      });
    } else {
      // The column statistics read of the reconciliation, then the compensation.
      hms_.failAt(hms_.calls().size() + 2);
      updater = plain_;
    }
    String mismatch = mismatch(tbl.getFullName(), "i, s");

    assertFails(() -> apply(updater, tbl, msTbl(tbl, integration), params),
        IcebergStatsBaseMismatchException.class, mismatch);

    if (integration) {
      assertTrue(rule.commits() > 0);
    } else {
      assertTrue(hms_.last(Method.ALTER_TABLE_PROPERTIES).failed);
    }
    assertEquals(idsOf(COLUMNS, b), storedIds(t, tbl, integration));
    // The repeated RPC compensates again.
    assertFails(() -> apply(tbl, msTbl(tbl, integration), params),
        IcebergStatsBaseMismatchException.class, mismatch);
    assertEquals(idsOf(Arrays.asList("bin", "d"), b), storedIds(t, tbl, integration));
  }

  private void mismatchHmsNdv() {
    Map<String, ColumnStatisticsObj> stored = hmsStats();
    stored.get("i").getStatsData().getLongStats().setNumDVs(9);
    stored.get("s").getStatsData().getStringStats().setNumDVs(11);
    hms_.setStoredStats(stored);
  }

  private static TAlterTableUpdateStatsParams mismatchRequest(IcebergTestTables t,
      long s1, long b) {
    return incremental(s1, b, 100, MISMATCH_COLS, bases(t, b, MISMATCH_COLS));
  }

  /**
   * The HMS table of a request after catalogd has set its table statistics: new row
   * count, size and statistics time, and no COLUMN_STATS_ACCURATE.
   */
  private static org.apache.hadoop.hive.metastore.api.Table requestMsTbl(
      IcebergTable tbl, boolean integration) {
    org.apache.hadoop.hive.metastore.api.Table msTbl = msTbl(tbl, integration);
    msTbl.putToParameters(StatsSetupConst.ROW_COUNT, "200");
    msTbl.putToParameters(StatsSetupConst.TOTAL_SIZE, "20000");
    msTbl.putToParameters(LAST_COMPUTE_STATS_TIME, "2000");
    msTbl.getParameters().remove(StatsSetupConst.COLUMN_STATS_ACCURATE);
    return msTbl;
  }

  private static Map<String, String> withoutIds(Map<String, String> params) {
    Map<String, String> res = new HashMap<>(params);
    res.remove(IDS);
    return res;
  }

  private static String mismatch(String table, String columns) {
    return "Base statistics of column(s) " + columns + " of table " + table
        + " do not match the Hive Metastore; run COMPUTE STATS.";
  }

  private static String conflict(IcebergTable tbl) {
    return "Statistics of table " + tbl.getFullName()
        + " were changed concurrently; run COMPUTE INCREMENTAL STATS again.";
  }

  private static List<Object> theta(String col, boolean impala) {
    return Arrays.asList(StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1, fieldId(col),
        impala);
  }

  /** Payloads of the blobs registered for 's1' by type, field id and authorship. */
  private static Map<List<Object>, byte[]> payloads(IcebergTestTables t, long s1)
      throws Exception {
    StatisticsFile file = statsFile(t.table(), s1);
    Map<List<Object>, byte[]> res = new HashMap<>();
    try (PuffinReader reader =
        Puffin.read(t.table().io().newInputFile(file.path())).build()) {
      for (Pair<org.apache.iceberg.puffin.BlobMetadata, ByteBuffer> p
          : reader.readAll(reader.fileMetadata().blobs())) {
        org.apache.iceberg.puffin.BlobMetadata b = p.first();
        assertEquals(1, b.inputFields().size());
        byte[] bytes = new byte[p.second().remaining()];
        p.second().duplicate().get(bytes);
        boolean impala =
            b.properties().containsKey(IcebergPuffinStatsWriter.IMPALA_CREATED_BY);
        assertTrue(res.put(Arrays.asList(b.type(), b.inputFields().get(0), impala),
            bytes) == null);
      }
    }
    return res;
  }
}
