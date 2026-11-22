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
import static org.apache.impala.analysis.IcebergIncrementalBaseRules.MAX_DELETED_RATIO;
import static org.apache.impala.analysis.IcebergIncrementalBaseRules.hasBase;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.IcebergIncrementalBaseRules.Decision;
import org.apache.impala.analysis.IcebergStatsPlan.Mode;
import org.apache.impala.analysis.IcebergStatsPlan.NdvBase;
import org.apache.impala.analysis.IcebergStatsPlan.ReasonKind;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.iceberg.IcebergAddedFiles;
import org.apache.impala.catalog.iceberg.IcebergAddedFiles.AddedFiles;
import org.apache.impala.catalog.iceberg.IcebergAddedFiles.GapException;
import org.apache.impala.catalog.iceberg.IcebergTestTables;
import org.apache.impala.catalog.iceberg.PuffinFixtures;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.mockito.MockedStatic;

/**
 * Rules of IcebergIncrementalBaseRules#decide on the files added since the bases: the
 * deleted-row threshold, unsupported snapshots, replace, empty gaps, schemas and types,
 * and the profile; and the "Incremental stats" flag of hasBase().
 */
public class IcebergIncrementalGapRulesTest extends FrontendTestBase {
  private static final String DB = "iceberg_incremental_gap_rules";
  /** Theta columns: id, s. */
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "s", Types.StringType.get()),
      optional(3, "bin", Types.BinaryType.get()));
  private static final List<Integer> THETA_FIELDS = Arrays.asList(1, 2);
  private static final long NDV = 10;
  private static final String WARNING_PREFIX =
      "COMPUTE INCREMENTAL STATS on Iceberg table " + DB + ".";

  @Rule public TemporaryFolder tmp = new TemporaryFolder();
  @Rule public TestName testName = new TestName();

  private static final IcebergAnalysisFixture ifx =
      IcebergAnalysisFixture.create(feFixture_);

  private IcebergTestTables create(String suffix, Schema schema, int formatVersion) {
    return IcebergTestTables.create(tmp, DB,
        testName.getMethodName().toLowerCase() + suffix, schema,
        PartitionSpec.unpartitioned(), formatVersion);
  }

  private IcebergTestTables create() { return create("", SCHEMA, 2); }

  /**
   * Loads 't' with HMS statistics of every column computed after every commit so far;
   * null properties are not set.
   */
  private static IcebergTable load(IcebergTestTables t, String ids, String maxRatio)
      throws Exception {
    Map<String, String> params = new HashMap<>();
    if (ids != null) params.putAll(IcebergAnalysisFixture.computedAfterNow(ids));
    if (maxRatio != null) params.put(MAX_DELETED_RATIO, maxRatio);
    IcebergTable tbl = ifx.addPathB(t, params);
    for (Column c : tbl.getColumns()) {
      c.getStats().setNumNulls(0);
      c.getStats().setNumDistinctValues(NDV);
    }
    return tbl;
  }

  private static IcebergTable load(IcebergTestTables t, String ids) throws Exception {
    return load(t, ids, null);
  }

  private static DataFile rows(IcebergTestTables t, long sizeBytes, int rows) {
    return t.file(sizeBytes, null,
        Collections.singletonMap("id", Collections.nCopies(rows, 1)));
  }

  private static StatisticsFile registerImpalaBlobs(IcebergTestTables t, long snapshotId,
      List<Integer> fieldIds) {
    List<PuffinFixtures.BlobSpec> blobs = new ArrayList<>();
    for (int f : fieldIds) {
      blobs.add(PuffinFixtures.blob(f, snapshotId, new byte[] {1, 2, 3})
          .ndvProperty(Long.toString(NDV)).impalaCreatedBy("Impala 4.5.0"));
    }
    StatisticsFile file = PuffinFixtures.write(t.table(), snapshotId, blobs);
    t.setStats(snapshotId, file);
    return file;
  }

  private static StatisticsFile registerImpalaBlobs(IcebergTestTables t,
      long snapshotId) {
    return registerImpalaBlobs(t, snapshotId, THETA_FIELDS);
  }

  /** Columns and theta field ids as the resolver plans them. */
  private static Decision decide(IcebergTable tbl) throws AnalysisException {
    IcebergStatsPlan plan = IcebergStatsBaseResolver.resolve(
        tbl, new ArrayList<>(tbl.getColumns()), false, false);
    return IcebergIncrementalBaseRules.decide(
        tbl, plan.metricColumns(), plan.thetaFieldIds(), tbl.snapshotId());
  }

  private static Map<Integer, NdvBase> sameFile(StatisticsFile file, int... fieldIds) {
    Map<Integer, NdvBase> res = new LinkedHashMap<>();
    for (int f : fieldIds) res.put(f, new NdvBase(file.snapshotId(), file.path()));
    return res;
  }

  /** Profile counters of the metric gap 'gh' and, unless null, the sketch gap 'gn'. */
  private static Map<String, Long> counters(List<DataFile> gh, List<DataFile> gn) {
    Map<String, Long> res = new LinkedHashMap<>();
    res.put(IcebergIncrementalBaseRules.PROFILE_GAP_FILES, (long) gh.size());
    res.put(IcebergIncrementalBaseRules.PROFILE_GAP_BYTES, bytes(gh));
    long records = 0;
    for (DataFile f : gh) records += f.recordCount();
    res.put(IcebergIncrementalBaseRules.PROFILE_GAP_RECORDS, records);
    if (gn != null) {
      res.put(IcebergIncrementalBaseRules.PROFILE_THETA_GAP_FILES, (long) gn.size());
      res.put(IcebergIncrementalBaseRules.PROFILE_THETA_GAP_BYTES, bytes(gn));
    }
    return res;
  }

  private static long bytes(List<DataFile> files) {
    long res = 0;
    for (DataFile f : files) res += f.fileSizeInBytes();
    return res;
  }

  private static <K, V> void assertOrderedEquals(Map<K, V> expected, Map<K, V> actual) {
    assertEquals(
        new ArrayList<>(expected.entrySet()), new ArrayList<>(actual.entrySet()));
  }

  /**
   * NOOP, INCREMENTAL or, for FULL, a gap with only deletes; 'ratio' is the deleted-row
   * ratio of the profile if the threshold is on.
   */
  private static void assertGap(Decision d, Mode mode, long bh, Long bn,
      Map<Integer, NdvBase> ndvBases, Map<String, Long> counters, String ratio,
      List<String> warnings) {
    assertSame(mode, d.mode);
    assertNull(d.reason);
    assertEquals(mode == Mode.FULL, d.deletesOnly);
    assertEquals(Long.valueOf(bh), d.bh);
    assertEquals(bn, d.bn);
    assertEquals(
        new ArrayList<>(ndvBases.keySet()), new ArrayList<>(d.ndvBases.keySet()));
    for (Map.Entry<Integer, NdvBase> e : ndvBases.entrySet()) {
      NdvBase actual = d.ndvBases.get(e.getKey());
      assertEquals(e.getValue().blobSnapshotId(), actual.blobSnapshotId());
      assertEquals(e.getValue().statsFilePath(), actual.statsFilePath());
    }
    assertEquals(warnings, d.warnings);
    Map<String, String> info = new LinkedHashMap<>();
    info.put(IcebergIncrementalBaseRules.PROFILE_METRICS_BASE, Long.toString(bh));
    if (bn != null) info.put(IcebergIncrementalBaseRules.PROFILE_NDV_BASE, bn.toString());
    if (ratio != null) info.put(IcebergIncrementalBaseRules.PROFILE_DELETED_RATIO, ratio);
    assertOrderedEquals(info, d.profileInfo);
    assertOrderedEquals(counters, d.profileCounters);
  }

  private static void assertGap(Decision d, Mode mode, long bh, Long bn,
      Map<Integer, NdvBase> ndvBases, Map<String, Long> counters) {
    assertGap(d, mode, bh, bn, ndvBases, counters, null, Collections.emptyList());
  }

  /**
   * FULL with 'kind' and 'args': 'earlierWarnings' and then 'reasonText', the profile
   * names the reason and, unless null, the deleted-row ratio.
   */
  private static void assertFull(Decision d, IcebergTable tbl, ReasonKind kind,
      List<String> args, String reasonText, String ratio, String... earlierWarnings) {
    assertSame(Mode.FULL, d.mode);
    assertSame(kind, d.reason.kind());
    assertEquals(args, d.reason.args());
    assertFalse(d.deletesOnly);
    assertNull(d.bh);
    assertNull(d.bn);
    assertTrue(d.ndvBases.isEmpty());
    List<String> warnings = new ArrayList<>(Arrays.asList(earlierWarnings));
    warnings.add(WARNING_PREFIX + tbl.getName() + " computes full statistics: "
        + reasonText + ".");
    assertEquals(warnings, d.warnings);
    Map<String, String> info = new LinkedHashMap<>();
    info.put(IcebergIncrementalBaseRules.PROFILE_REASON, kind.name());
    if (ratio != null) info.put(IcebergIncrementalBaseRules.PROFILE_DELETED_RATIO, ratio);
    assertOrderedEquals(info, d.profileInfo);
    assertTrue(d.profileCounters.isEmpty());
  }

  private static void assertFull(Decision d, IcebergTable tbl, ReasonKind kind,
      List<String> args, String reasonText) {
    assertFull(d, tbl, kind, args, reasonText, null);
  }

  private static List<String> args(Object... values) {
    List<String> res = new ArrayList<>();
    for (Object v : values) res.add(String.valueOf(v));
    return res;
  }

  private static String invalidRatio(String value) {
    return "Ignoring invalid value '" + value + "' of table property '"
        + MAX_DELETED_RATIO + "'.";
  }

  @Test
  public void gapsOfMetricsAndSketchesGiveIncrementalWithProfile() throws Exception {
    IcebergTestTables t = create();
    DataFile f0 = rows(t, 100, 2);
    long s0 = t.append(f0);
    StatisticsFile blobs = registerImpalaBlobs(t, s0);
    // LEGACY statistics at 'bh' kept the sketches of s0 as NDV base.
    DataFile f1 = rows(t, 200, 3);
    long bh = t.append(f1);
    // Only the sketches have a gap.
    assertGap(decide(load(t, "1-3:" + bh)), Mode.INCREMENTAL, bh, s0,
        sameFile(blobs, 1, 2), counters(Collections.emptyList(), Arrays.asList(f1)));
    DataFile f2 = rows(t, 300, 4);
    t.append(f2);
    // Compaction of a base file adds nothing to the gap.
    t.replace(Collections.singletonList(f0), Collections.singletonList(rows(t, 150, 2)));
    DataFile f3 = rows(t, 400, 5);
    long s1 = t.append(f3);
    IcebergTable tbl = load(t, "1-3:" + bh);
    Decision d = decide(tbl);
    assertGap(d, Mode.INCREMENTAL, bh, s0, sameFile(blobs, 1, 2),
        counters(Arrays.asList(f2, f3), Arrays.asList(f1, f2, f3)));
    // The scans of the child queries filter by the same builder.
    Table api = tbl.getIcebergApiTable();
    AddedFiles gh = IcebergAddedFiles.compute(api, bh, s1);
    AddedFiles gn = IcebergAddedFiles.compute(api, s0, s1);
    assertOrderedEquals(counters(gh, gn), d.profileCounters);
  }

  private static Map<String, Long> counters(AddedFiles gh, AddedFiles gn) {
    Map<String, Long> res = new LinkedHashMap<>();
    res.put(IcebergIncrementalBaseRules.PROFILE_GAP_FILES, gh.fileCount);
    res.put(IcebergIncrementalBaseRules.PROFILE_GAP_BYTES, gh.bytes);
    res.put(IcebergIncrementalBaseRules.PROFILE_GAP_RECORDS, gh.records);
    res.put(IcebergIncrementalBaseRules.PROFILE_THETA_GAP_FILES, gn.fileCount);
    res.put(IcebergIncrementalBaseRules.PROFILE_THETA_GAP_BYTES, gn.bytes);
    return res;
  }

  @Test
  public void emptyGapIsNoopWhateverTypesAre() throws Exception {
    IcebergTestTables t = create();
    long bh = t.append(rows(t, 100, 1));
    StatisticsFile blobs = registerImpalaBlobs(t, bh);
    List<DataFile> none = Collections.emptyList();
    assertGap(decide(load(t, "1-3:" + bh)), Mode.NOOP, bh, bh, sameFile(blobs, 1, 2),
        counters(none, none));
    // ALTER without a new snapshot leaves HMS statistics of the column in the cache.
    t.table().updateSchema().updateColumn("id", Types.LongType.get()).commit();
    IcebergTable tbl = load(t, "1-3:" + bh);
    assertEquals("BIGINT", tbl.getColumn("id").getType().toSql());
    assertGap(decide(tbl), Mode.NOOP, bh, bh, sameFile(blobs, 1, 2),
        counters(none, none));
    t.append(rows(t, 100, 1));
    tbl = load(t, "1-3:" + bh);
    assertFull(decide(tbl), tbl, ReasonKind.COLUMNS_TYPE_CHANGED, args("id"),
        "type of column(s) id changed since their base statistics");
  }

  @Test
  public void typeOfNdvBaseSchemaIsChecked() throws Exception {
    IcebergTestTables t = create();
    long s0 = t.append(rows(t, 100, 1));
    registerImpalaBlobs(t, s0);
    t.table().updateSchema().updateColumn("id", Types.LongType.get()).commit();
    long bh = t.append(rows(t, 100, 1));
    t.append(rows(t, 100, 1));
    IcebergTable tbl = load(t, "1-3:" + bh);
    assertFull(decide(tbl), tbl, ReasonKind.COLUMNS_TYPE_CHANGED, args("id"),
        "type of column(s) id changed since their base statistics");
  }

  @Test
  public void addedColumnWithBaseBeforeItIsTypeChanged() throws Exception {
    IcebergTestTables t = create();
    long bh = t.append(rows(t, 100, 1));
    t.table().updateSchema().addColumn("n", Types.IntegerType.get()).commit();
    // Statistics computed after ADD COLUMNS without a new snapshot cover 'n' at 'bh'.
    StatisticsFile blobs = registerImpalaBlobs(t, bh, Arrays.asList(1, 2, 4));
    List<DataFile> none = Collections.emptyList();
    assertGap(decide(load(t, "1-4:" + bh)), Mode.NOOP, bh, bh, sameFile(blobs, 1, 2, 4),
        counters(none, none));
    t.append(rows(t, 100, 1));
    IcebergTable tbl = load(t, "1-4:" + bh);
    assertFull(decide(tbl), tbl, ReasonKind.COLUMNS_TYPE_CHANGED, args("n"),
        "type of column(s) n changed since their base statistics");
  }

  @Test
  public void gapWithOnlyDeletesUpdatesRowCountOnly() throws Exception {
    IcebergTestTables t = create();
    DataFile f0 = rows(t, 100, 1);
    long bh = t.append(f0, rows(t, 100, 1));
    registerImpalaBlobs(t, bh);
    t.delete(f0);
    List<DataFile> none = Collections.emptyList();
    Map<Integer, NdvBase> noBases = Collections.emptyMap();
    assertGap(decide(load(t, "1-3:" + bh)), Mode.FULL, bh, bh, noBases,
        counters(none, none));
    // Such statistics keep the bases, so the next run sees the same gap.
    assertGap(decide(load(t, "1-3:" + bh)), Mode.FULL, bh, bh, noBases,
        counters(none, none));
    t.rowDelta(t.positionDeletes(50, 1, null));
    assertGap(decide(load(t, "1-3:" + bh)), Mode.FULL, bh, bh, noBases,
        counters(none, none));
  }

  @Test
  public void snapshotWithoutManifestListIsUnsupported() throws Exception {
    IcebergTestTables t = create("", SCHEMA, 1);
    long bh = t.append(rows(t, 100, 1));
    registerImpalaBlobs(t, bh);
    long s2 = t.append(rows(t, 100, 1));
    t.append(rows(t, 100, 1));
    t.removeManifestList(s2);
    IcebergTable tbl = load(t, "1-3:" + bh);
    assertFull(decide(tbl), tbl, ReasonKind.UNSUPPORTED_SNAPSHOT, args(s2),
        "snapshot " + s2 + " has no manifest list or operation");
  }

  @Test
  public void baseLeavingChainBeforeGapIsReadIsNotAncestor() throws Exception {
    IcebergTestTables t = create();
    long bh = t.append(rows(t, 100, 1));
    registerImpalaBlobs(t, bh);
    long s1 = t.append(rows(t, 100, 1));
    IcebergTable tbl = load(t, "1-3:" + bh);
    Constructor<GapException> ctor = GapException.class.getDeclaredConstructor(
        GapException.Kind.class, long.class, String.class);
    ctor.setAccessible(true);
    GapException expired = ctor.newInstance(GapException.Kind.NOT_ANCESTOR, bh, "gone");
    Decision d;
    try (MockedStatic<IcebergAddedFiles> gap = mockStatic(IcebergAddedFiles.class)) {
      gap.when(() -> IcebergAddedFiles.read(any(Table.class), anyLong(), anyLong()))
          .thenThrow(expired);
      d = decide(tbl);
    }
    assertFull(d, tbl, ReasonKind.BASE_NOT_ANCESTOR, args(bh, s1),
        "base snapshot " + bh + " is not an ancestor of snapshot " + s1);
  }

  @Test
  public void replaceOfGapFilesIsFull() throws Exception {
    IcebergTestTables t = create();
    long bh = t.append(rows(t, 100, 1));
    registerImpalaBlobs(t, bh);
    DataFile added = rows(t, 100, 1);
    t.append(added);
    t.replace(Collections.singletonList(added), Collections.singletonList(
        rows(t, 100, 1)));
    IcebergTable tbl = load(t, "1-3:" + bh);
    assertFull(decide(tbl), tbl, ReasonKind.REPLACE_REWROTE_NEW_FILES, args(bh),
        "data files added after snapshot " + bh + " were rewritten by a 'replace'"
            + " snapshot");
  }

  @Test
  public void snapshotWithoutKnownSchemaIsFullUnlessGapIsEmpty() throws Exception {
    IcebergTestTables t = create();
    long bh = t.append(rows(t, 100, 1));
    StatisticsFile blobs = registerImpalaBlobs(t, bh);
    t.removeSnapshotSchemaIds();
    List<DataFile> none = Collections.emptyList();
    assertGap(decide(load(t, "1-3:" + bh)), Mode.NOOP, bh, bh, sameFile(blobs, 1, 2),
        counters(none, none));
    t.append(rows(t, 100, 1));
    IcebergTable tbl = load(t, "1-3:" + bh);
    assertFull(decide(tbl), tbl, ReasonKind.SNAPSHOT_WITHOUT_SCHEMA, args(bh),
        "snapshot " + bh + " has no schema id");

    // The metric base comes first, then the NDV bases.
    t = create("_ndv", SCHEMA, 2);
    long s0 = t.append(rows(t, 100, 1));
    registerImpalaBlobs(t, s0);
    bh = t.append(rows(t, 100, 1));
    t.append(rows(t, 100, 1));
    t.setSnapshotSchemaId(s0, null);
    tbl = load(t, "1-3:" + bh);
    assertFull(decide(tbl), tbl, ReasonKind.SNAPSHOT_WITHOUT_SCHEMA, args(s0),
        "snapshot " + s0 + " has no schema id");
    t.setSnapshotSchemaId(bh, 99);
    tbl = load(t, "1-3:" + bh);
    assertFull(decide(tbl), tbl, ReasonKind.SNAPSHOT_WITHOUT_SCHEMA, args(bh),
        "snapshot " + bh + " has no schema id");
  }

  @Test
  public void deletedRatioAboveThresholdIsFull() throws Exception {
    IcebergTestTables t = create();
    DataFile f0 = rows(t, 100, 1);
    long bh = t.append(f0, rows(t, 100, 1));
    StatisticsFile blobs = registerImpalaBlobs(t, bh);
    t.delete(f0);
    DataFile f2 = rows(t, 300, 1);
    t.append(f2);
    Map<Integer, NdvBase> bases = sameFile(blobs, 1, 2);
    List<DataFile> gap = Collections.singletonList(f2);
    List<String> noWarnings = Collections.emptyList();
    // Without the property deletes do not matter.
    assertGap(decide(load(t, "1-3:" + bh)), Mode.INCREMENTAL, bh, bh, bases,
        counters(gap, gap));
    IcebergTable tbl = load(t, "1-3:" + bh, "0.1");
    assertFull(decide(tbl), tbl, ReasonKind.DELETED_RATIO, args(0.5, "0.1"),
        "estimated ratio of deleted rows 0.5000 exceeds '" + MAX_DELETED_RATIO + "'=0.1",
        "0.5000");
    assertGap(decide(load(t, "1-3:" + bh, " 0.5")), Mode.INCREMENTAL, bh, bh, bases,
        counters(gap, gap), "0.5000", noWarnings);

    for (String invalid : new String[] {"abc", "", "NaN", "1.5", "-0.1"}) {
      assertGap(decide(load(t, "1-3:" + bh, invalid)), Mode.INCREMENTAL, bh, bh, bases,
          counters(gap, gap), null, Collections.singletonList(invalidRatio(invalid)));
    }
    tbl = load(t, null, "abc");
    assertFull(decide(tbl), tbl, ReasonKind.NO_BASE, args(),
        "no statistics computed by Impala to use as a base", null, invalidRatio("abc"));

    String noTotal = "snapshot " + bh + " has no usable 'total-records' to evaluate '"
        + MAX_DELETED_RATIO + "'";
    for (String total : new String[] {"x", "0", null}) {
      if (total == null) {
        t.removeFromSummary(bh, SnapshotSummary.TOTAL_RECORDS_PROP);
      } else {
        t.setSummary(bh, Collections.singletonMap(SnapshotSummary.TOTAL_RECORDS_PROP,
            total));
      }
      tbl = load(t, "1-3:" + bh, "0.1");
      assertFull(decide(tbl), tbl, ReasonKind.NO_TOTAL_RECORDS, args(bh), noTotal);
    }
  }

  @Test
  public void hasBaseFollowsEveryCondition() throws Exception {
    // Complex columns get no statistics, so they need no base.
    Schema schema = new Schema(SCHEMA.columns().get(0), SCHEMA.columns().get(1),
        SCHEMA.columns().get(2), optional(4, "st", Types.StructType.of(
            optional(5, "x", Types.IntegerType.get()))));
    IcebergTestTables t = create("", schema, 2);
    assertFalse(hasBase(load(t, "1-3:1")));
    long s0 = t.append(rows(t, 100, 1));
    long bh = t.append(rows(t, 100, 1));
    // A theta column needs an NDV base, a BINARY one does not.
    registerImpalaBlobs(t, bh, Collections.singletonList(1));
    assertFalse(hasBase(load(t, "1-3:" + bh)));
    registerImpalaBlobs(t, bh);
    assertTrue(hasBase(load(t, "1-3:" + bh)));
    assertFalse(hasBase(load(t, null)));
    assertFalse(hasBase(load(t, "1-2:" + bh)));
    IcebergTable tbl = load(t, "1-3:" + bh);
    tbl.getColumn("bin").getStats().setNumNulls(-1);
    assertFalse(hasBase(tbl));
    assertFalse(hasBase(load(t, "1:" + s0 + ",2-3:" + bh)));

    // Type changes do not matter while the gap is empty.
    t.table().updateSchema().updateColumn("id", Types.LongType.get()).commit();
    assertTrue(hasBase(load(t, "1-3:" + bh)));
    long s1 = t.append(rows(t, 100, 1));
    assertFalse(hasBase(load(t, "1-3:" + bh)));
    registerImpalaBlobs(t, s1);
    t.append(rows(t, 100, 1));
    assertTrue(hasBase(load(t, "1-3:" + s1)));
    int schemaId = t.table().snapshot(s1).schemaId();
    t.setSnapshotSchemaId(s1, 99);
    assertFalse(hasBase(load(t, "1-3:" + s1)));
    t.setSnapshotSchemaId(s1, schemaId);
    assertTrue(hasBase(load(t, "1-3:" + s1)));
    t.table().updateSchema().addColumn("n", Types.IntegerType.get()).commit();
    assertFalse(hasBase(load(t, "1-3:" + s1)));

    t = create("_rollback", SCHEMA, 2);
    long kept = t.append(rows(t, 100, 1));
    // The NDV bases stay in the chain, the metric base leaves it.
    registerImpalaBlobs(t, kept);
    bh = t.append(rows(t, 100, 1));
    registerImpalaBlobs(t, bh);
    t.rollbackTo(kept);
    t.append(rows(t, 100, 1));
    assertFalse(hasBase(load(t, "1-3:" + bh)));
  }

  @Test
  public void hasBaseReadsNoManifests() throws Exception {
    IcebergTestTables t = create();
    long bh = t.append(rows(t, 100, 1));
    registerImpalaBlobs(t, bh);
    t.append(rows(t, 100, 1));
    IcebergTable tbl = load(t, "1-3:" + bh);
    File metadata =
        new File(new Path(t.table().location()).toUri().getPath(), "metadata");
    int deleted = 0;
    for (File f : metadata.listFiles()) {
      if (f.getName().endsWith(".avro")) deleted += f.delete() ? 1 : 0;
    }
    assertTrue(deleted >= 4);
    assertTrue(hasBase(tbl));
  }

  @Test
  public void icebergFailureGivesNoBaseWithWarning() throws Exception {
    IcebergTestTables t = create();
    long bh = t.append(rows(t, 100, 1));
    registerImpalaBlobs(t, bh);
    IcebergTable tbl = spy(load(t, "1-3:" + bh));
    Table api = spy(tbl.getIcebergApiTable());
    doThrow(new NotFoundException("File does not exist: x.stats"))
        .when(api).statisticsFiles();
    doReturn(api).when(tbl).getIcebergApiTable();
    List<String> warnings = new CopyOnWriteArrayList<>();
    AppenderSkeleton appender = new AppenderSkeleton() {
      @Override
      protected void append(LoggingEvent event) {
        if (event.getLevel().equals(Level.WARN)) warnings.add(event.getRenderedMessage());
      }

      @Override
      public void close() {}

      @Override
      public boolean requiresLayout() { return false; }
    };
    Logger log = Logger.getLogger(IcebergIncrementalBaseRules.class);
    log.addAppender(appender);
    try {
      assertFalse(hasBase(tbl));
    } finally {
      log.removeAppender(appender);
    }
    assertEquals(Collections.singletonList("Failed to read Iceberg metadata of table "
        + DB + "." + tbl.getName()), warnings);
  }
}
