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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.StringWriter;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.IcebergAnalysisFixture;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.iceberg.FailingCommitTables;
import org.apache.impala.catalog.iceberg.FailingCommitTables.Failure;
import org.apache.impala.catalog.iceberg.FailingCommitTables.Mode;
import org.apache.impala.catalog.iceberg.FailingFileIO;
import org.apache.impala.catalog.iceberg.IcebergPuffinStatsWriter;
import org.apache.impala.catalog.iceberg.IcebergTestTables;
import org.apache.impala.catalog.iceberg.PuffinFixtures;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.service.FakeHmsStatsWriter.Method;
import org.apache.impala.thrift.TAlterTableUpdateStatsParams;
import org.apache.impala.thrift.TColumnStats;
import org.apache.impala.thrift.TIcebergPuffinStatsParams;
import org.apache.impala.thrift.TIcebergStatsMode;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.util.IcebergUtil;
import org.apache.impala.util.IcebergUtil.ComputeStatsSnapshotPropertyConverter;
import org.apache.impala.util.NoOpEventSequence;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

/**
 * Failures of IcebergStatsUpdater#apply and #detachColumns: the error, the table
 * metadata, the Puffin files on disk and the HMS calls after each failure.
 */
public class IcebergStatsUpdaterFailureTest extends FrontendTestBase {
  private static final String DB = "iceberg_stats_updater_failure";
  private static final String IDS = IcebergTable.COMPUTE_STATS_SNAPSHOT_IDS;
  private static final String VERSION = "4.5.0-updater-failure-test";
  private static final TIcebergStatsMode FULL = TIcebergStatsMode.FULL;
  private static final TIcebergStatsMode LEGACY = TIcebergStatsMode.LEGACY;
  private static final Schema SCHEMA = new Schema(
      optional(1, "i", Types.IntegerType.get()),
      optional(2, "s", Types.StringType.get()),
      optional(3, "bin", Types.BinaryType.get()),
      optional(4, "x", Types.IntegerType.get()));
  /** Columns of the command; "x" keeps the statistics of an older command. */
  private static final List<String> COLUMNS = Arrays.asList("i", "s", "bin");
  private static final Class<?>[] LOGGED =
      {IcebergStatsUpdater.class, IcebergPuffinStatsWriter.class};

  @Rule public TemporaryFolder tmp = new TemporaryFolder();
  @Rule public TestName testName = new TestName();

  private static final IcebergAnalysisFixture ifx =
      IcebergAnalysisFixture.create(feFixture_);

  private final FakeHmsStatsWriter hms_ = new FakeHmsStatsWriter();
  private final FailingCommitTables.Rule rule_ = new FailingCommitTables.Rule();
  private final StringWriter log_ = new StringWriter();
  private final WriterAppender appender_ =
      new WriterAppender(new PatternLayout("%p %m%n"), log_);
  private String savedVersion_;
  private IcebergTestTables t_;
  private IcebergTable tbl_;
  private FailingFileIO io_;
  private long s0_;
  private long s1_;
  private int loads_;
  /** Number of the load that fails, 0 if none. */
  private int failLoadAt_;
  /** Runs after each load, so the loaded table does not see its changes. */
  private Runnable afterLoad_;
  private IcebergStatsUpdater updater_;

  @Before
  public void setUpUpdater() throws Exception {
    savedVersion_ = BackendConfig.INSTANCE.getBackendCfg().getImpala_build_version();
    BackendConfig.INSTANCE.getBackendCfg().setImpala_build_version(VERSION);
    for (Class<?> c : LOGGED) Logger.getLogger(c).addAppender(appender_);
    t_ = IcebergTestTables.create(tmp, DB, testName.getMethodName().toLowerCase(),
        SCHEMA, PartitionSpec.unpartitioned(), 2);
    // Iceberg would retry a failed commit and hide the injected failure.
    t_.table().updateProperties().set(TableProperties.COMMIT_NUM_RETRIES, "0").commit();
    s0_ = t_.append(t_.file(100, null));
    s1_ = t_.append(t_.file(100, null));
    tbl_ = ifx.addPathB(t_, Collections.emptyMap());
    io_ = new FailingFileIO(t_.table().io());
    updater_ = new IcebergStatsUpdater(feTbl -> {
      if (++loads_ == failLoadAt_) {
        throw new NotFoundException("Injected failure of load %s", loads_);
      }
      org.apache.iceberg.Table real = IcebergUtil.loadTable(feTbl);
      if (afterLoad_ != null) afterLoad_.run();
      return new BaseTable(new FailingCommitTables(
          ((HasTableOperations) real).operations(), io_, rule_), real.name());
    });
  }

  @After
  public void restoreBuildVersion() {
    for (Class<?> c : LOGGED) Logger.getLogger(c).removeAppender(appender_);
    BackendConfig.INSTANCE.getBackendCfg().setImpala_build_version(savedVersion_);
  }

  private static int fieldId(String col) { return SCHEMA.findField(col).fieldId(); }

  /** Parameters of the analysis of snapshot S1; FULL has a sketch for all but BINARY. */
  private TAlterTableUpdateStatsParams params(TIcebergStatsMode mode, String... cols) {
    TAlterTableUpdateStatsParams params = new TAlterTableUpdateStatsParams();
    params.setTable_name(new TTableName(DB, t_.name()));
    params.setTable_stats(new TTableStats(100));
    TIcebergPuffinStatsParams p =
        new TIcebergPuffinStatsParams(mode, s1_, new HashMap<>());
    for (String col : cols.length == 0 ? COLUMNS : Arrays.asList(cols)) {
      params.putToColumn_stats(col, new TColumnStats(2.5, 4, mode == LEGACY ? 3 : -1, 1,
          -1, -1));
      p.putToColumn_field_ids(col, fieldId(col));
      if (mode == FULL && !col.equals("bin")) {
        List<?> values = col.equals("s") ? Arrays.asList("a", "b", "c")
            : Arrays.asList(1, 2, 3, 4);
        p.addToTheta_field_ids(fieldId(col));
        p.putToTheta_sketches(fieldId(col), ByteBuffer.wrap(PuffinFixtures.sketch(
            SCHEMA.findField(col).type(), values).toByteArray()));
      }
    }
    params.setIceberg_puffin_stats(p);
    return params;
  }

  /** HMS table copy with 'ids'; integrated: of HiveCatalog, 'ids' in Iceberg too. */
  private Table msTbl(boolean integrated, Map<Integer, Long> ids) {
    Table msTbl = tbl_.getMetaStoreTable().deepCopy();
    if (!ids.isEmpty()) msTbl.putToParameters(IDS, idsString(ids));
    if (integrated) {
      msTbl.setTableType(TableType.EXTERNAL_TABLE.toString());
      msTbl.putToParameters("EXTERNAL", "TRUE");
      msTbl.putToParameters(IcebergTable.ICEBERG_CATALOG, "hive.catalog");
      msTbl.putToParameters("external.table.purge", "TRUE");
      if (!ids.isEmpty()) setIcebergIds(ids);
    }
    return msTbl;
  }

  /** Integrated HMS table whose ids property is missing from the Iceberg properties. */
  private Table idsOnlyInHms(Map<Integer, Long> ids) {
    Table msTbl = msTbl(true, Collections.emptyMap());
    msTbl.putToParameters(IDS, idsString(ids));
    return msTbl;
  }

  private void setIcebergIds(Map<Integer, Long> ids) {
    t_.table().updateProperties().set(IDS, idsString(ids)).commit();
  }

  private static String idsString(Map<Integer, Long> ids) {
    return ComputeStatsSnapshotPropertyConverter.mapToString(new TreeMap<>(ids));
  }

  private static Map<Integer, Long> parseIds(String value) {
    return ComputeStatsSnapshotPropertyConverter.stringToMap(value);
  }

  private static Map<Integer, Long> ids(Object... kv) {
    Map<Integer, Long> res = new TreeMap<>();
    for (int i = 0; i < kv.length; i += 2) res.put((Integer) kv[i], (Long) kv[i + 1]);
    return res;
  }

  /** ids before the command: only "x" has statistics, of snapshot S0. */
  private Map<Integer, Long> oldIds() { return ids(4, s0_); }

  private Map<Integer, Long> newIds() { return ids(1, s1_, 2, s1_, 3, s1_, 4, s0_); }

  private Map<Integer, Long> icebergIds() {
    return parseIds(t_.table().properties().get(IDS));
  }

  private Map<Integer, Long> lastHmsIds() {
    FakeHmsStatsWriter.Call call = hms_.lastSucceeded(Method.ALTER_TABLE_PROPERTIES);
    assertNotNull("no successful alterTableProperties", call);
    return parseIds(call.msTbl.getParameters().get(IDS));
  }

  private String metadataLocation() {
    return ((HasTableOperations) t_.table()).operations().current()
        .metadataFileLocation();
  }

  private Set<String> puffinFilesOnDisk() throws Exception {
    File dir = new File(new URI(t_.table().location() + "/metadata"));
    return Arrays.stream(dir.listFiles((d, n) -> n.endsWith(".stats")))
        .map(File::getName).collect(Collectors.toCollection(TreeSet::new));
  }

  private Set<String> registeredFiles() {
    return t_.table().statisticsFiles().stream().map(f -> new Path(f.path()).getName())
        .collect(Collectors.toCollection(TreeSet::new));
  }

  private boolean registeredAtS1() {
    return t_.table().statisticsFiles().stream().anyMatch(f -> f.snapshotId() == s1_);
  }

  /** No commit went through FailingCommitTables, no Puffin file and no HMS call. */
  private void assertNothingWritten() throws Exception {
    assertEquals(0, rule_.commits());
    assertTrue(puffinFilesOnDisk().isEmpty());
    assertTrue(hms_.calls().isEmpty());
  }

  /** Registers a Puffin file of another engine for S1 with a blob of "x". */
  private StatisticsFile registerForeignFile() {
    StatisticsFile file = PuffinFixtures.write(t_.table(), s1_, Collections.singletonList(
        PuffinFixtures.blob(fieldId("x"), s1_, PuffinFixtures.sketch(
            SCHEMA.findField("x").type(), Arrays.asList(5, 6)))));
    t_.setStats(s1_, file);
    return file;
  }

  @FunctionalInterface
  private interface Call { void run() throws Exception; }

  private void locked(Call call) throws Exception {
    tbl_.writeLock().lock();
    try {
      call.run();
    } finally {
      tbl_.releaseWriteLock();
    }
  }

  private Exception error(Call call) throws Exception {
    try {
      locked(call);
    } catch (Exception e) {
      return e;
    }
    fail("no error");
    return null;
  }

  private Call apply(Table msTbl, TAlterTableUpdateStatsParams params) {
    return () -> updater_.apply(tbl_, msTbl, params, hms_, NoOpEventSequence.INSTANCE);
  }

  /** Error of FULL statistics of all columns over the ids of an older command. */
  private Exception fullError(boolean integrated) throws Exception {
    return error(apply(msTbl(integrated, oldIds()), params(FULL)));
  }

  private Call detach(Table msTbl, String... cols) {
    return () -> updater_.detachColumns(tbl_, msTbl, Arrays.asList(cols), hms_);
  }

  private static void assertError(String expectedStart, Exception e) {
    assertTrue("not ImpalaRuntimeException: " + e, e instanceof ImpalaRuntimeException);
    assertTrue(e.getMessage(), e.getMessage().startsWith(expectedStart));
  }

  private String registerFailed(String cause) {
    return "Failed to register Puffin statistics of table " + tbl_.getFullName() + ": "
        + cause;
  }

  private String unknownState() {
    return "Commit state of statistics of table " + tbl_.getFullName()
        + " is unknown; run COMPUTE STATS.";
  }

  /** Some message of 'level' names the table and contains all 'parts'. */
  private boolean logged(Level level, String... parts) {
    return Arrays.stream(log_.toString().split("\n")).anyMatch(
        l -> l.startsWith(level + " ") && l.contains(tbl_.getFullName())
            && Arrays.stream(parts).allMatch(l::contains));
  }

  @Test
  public void loadFailureWritesNothing() throws Exception {
    String before = metadataLocation();
    failLoadAt_ = 1;
    assertError("Failed to load Iceberg table " + tbl_.getFullName()
        + ": Injected failure of load 1", fullError(false));
    failLoadAt_ = 2;
    assertError("Failed to load Iceberg table " + tbl_.getFullName()
        + ": Injected failure of load 2", error(detach(msTbl(false, oldIds()), "x")));
    assertEquals(before, metadataLocation());
    assertNothingWritten();
  }

  @Test
  public void invalidOrStaleAnalysisWritesNothing() throws Exception {
    TAlterTableUpdateStatsParams params = params(FULL);
    params.getIceberg_puffin_stats().putToTheta_sketches(fieldId("s"),
        ByteBuffer.wrap(new byte[] {-1, -1, -1, -1, -1, -1, -1, -1}));
    assertError("Invalid theta sketch for column s (snapshot " + s1_ + ")",
        error(apply(msTbl(false, oldIds()), params)));
    t_.table().updateSchema().renameColumn("s", "s2").commit();
    assertError("Column s of table " + tbl_.getFullName() + " was changed concurrently",
        error(apply(msTbl(false, oldIds()), params(FULL, "s"))));
    t_.table().updateSchema().deleteColumn("i").commit();
    assertError("Column i of table " + tbl_.getFullName() + " was changed concurrently",
        error(apply(msTbl(false, oldIds()), params(FULL, "i"))));
    t_.append(t_.file(100, null));
    t_.expire(s1_);
    assertError("Snapshot " + s1_ + " of table " + tbl_.getFullName() + " no longer",
        error(apply(msTbl(false, oldIds()), params(LEGACY, "bin"))));
    assertNothingWritten();
  }

  @Test
  public void fileFailuresWriteNothing() throws Exception {
    io_.failCall(FailingFileIO.Op.WRITE, 1);
    assertError("Failed to write Puffin statistics file ", fullError(false));
    assertNothingWritten();
    StatisticsFile foreign = registerForeignFile();
    io_.failPath(FailingFileIO.Op.READ, p -> p.equals(foreign.path()));
    assertError("Failed to read Puffin statistics file " + foreign.path() + " of table "
        + tbl_.getFullName() + ": ", fullError(false));
    assertEquals(Collections.singleton(new Path(foreign.path()).getName()),
        puffinFilesOnDisk());
    assertEquals(puffinFilesOnDisk(), registeredFiles());
    assertEquals(0, rule_.commits());
    assertTrue(hms_.calls().isEmpty());
    // A missing registered file is replaced with a file of the Impala blobs only.
    io_.clearRules();
    t_.table().io().deleteFile(foreign.path());
    locked(apply(msTbl(false, oldIds()), params(FULL)));
    StatisticsFile file = t_.table().statisticsFiles().get(0);
    assertEquals(s1_, file.snapshotId());
    assertEquals(Arrays.asList(Arrays.asList(1), Arrays.asList(2)),
        file.blobMetadata().stream().map(b -> b.fields()).collect(Collectors.toList()));
    assertTrue(logged(Level.WARN, foreign.path(), "is missing; lost blobs"));
    assertEquals(newIds(), lastHmsIds());
  }

  @Test
  public void registrationFailureDeletesFile() throws Exception {
    String before = metadataLocation();
    rule_.failAt(1, Failure.COMMIT_FAILED, Mode.BEFORE_APPLY);
    assertError(registerFailed("Injected commit failure of commit 1"), fullError(false));
    assertEquals(before, metadataLocation());
    assertTrue(puffinFilesOnDisk().isEmpty());
    assertTrue(logged(Level.WARN, Long.toString(s1_)));
    // Another engine registers its file for S1 after the load.
    afterLoad_ = () -> { if (loads_ == 2) registerForeignFile(); };
    assertError(registerFailed("Statistics of snapshot " + s1_ + " of table "
        + tbl_.getFullName() + " changed concurrently"), fullError(false));
    Set<String> foreign = registeredFiles();
    assertEquals(1, foreign.size());
    assertEquals(foreign, puffinFilesOnDisk());
    assertEquals(1, rule_.commits());
    assertTrue(hms_.calls().isEmpty());
  }

  @Test
  public void setCommitFailureFailsRegistration() throws Exception {
    // Sampled statistics of "i" leave no ids, but the HMS table still has the key.
    TAlterTableUpdateStatsParams params = params(LEGACY, "i");
    params.getIceberg_puffin_stats().setSampled(true);
    Table msTbl = idsOnlyInHms(ids(1, s0_));
    String before = metadataLocation();
    rule_.failAt(1, Failure.COMMIT_FAILED, Mode.BEFORE_APPLY);
    assertError(registerFailed("Injected commit failure of commit 1"),
        error(apply(msTbl.deepCopy(), params)));
    assertEquals(before, metadataLocation());
    assertEquals(1, rule_.commits());

    rule_.failAt(3, Failure.VALIDATION, Mode.BEFORE_APPLY);
    assertError(registerFailed("Injected validation failure of commit 3"),
        error(apply(msTbl.deepCopy(), params)));
    assertEquals(ids(1, s0_), icebergIds());
    // The outcome of the set commit is not looked for: nothing else is written.
    t_.table().updateProperties().remove(IDS).commit();
    rule_.failAt(4, Failure.STATE_UNKNOWN, Mode.AFTER_APPLY);
    assertError(unknownState(), error(apply(msTbl.deepCopy(), params)));
    assertEquals(3, loads_);
    assertEquals(4, rule_.commits());
    assertTrue(logged(Level.ERROR, Long.toString(s1_)));
    assertTrue(hms_.calls().isEmpty());
  }

  @Test
  public void unknownRegistrationStateIsResolvedByReload() throws Exception {
    rule_.failAt(1, Failure.STATE_UNKNOWN, Mode.BEFORE_APPLY);
    Exception e = fullError(false);
    assertError(registerFailed("Injected unknown state of commit 1"), e);
    assertTrue(e.getCause() instanceof CommitStateUnknownException);
    assertEquals(2, loads_);
    assertTrue(puffinFilesOnDisk().isEmpty());
    assertTrue(hms_.calls().isEmpty());

    rule_.failAt(2, Failure.STATE_UNKNOWN, Mode.AFTER_APPLY);
    locked(apply(msTbl(false, oldIds()), params(FULL)));
    assertEquals(4, loads_);
    assertEquals(puffinFilesOnDisk(), registeredFiles());
    assertTrue(registeredAtS1());
    assertEquals(Arrays.asList(Method.ALTER_TABLE_PROPERTIES, Method.UPDATE_COLUMN_STATS),
        hms_.methods());
    assertEquals(newIds(), lastHmsIds());
  }

  @Test
  public void unknownStateWithoutFileIsResolvedByProperties() throws Exception {
    Table msTbl = msTbl(true, oldIds());
    rule_.failAt(1, Failure.STATE_UNKNOWN, Mode.BEFORE_APPLY);
    assertError(registerFailed("Injected unknown state of commit 1"),
        error(apply(msTbl.deepCopy(), params(LEGACY))));
    assertEquals(oldIds(), icebergIds());
    assertTrue(hms_.calls().isEmpty());

    rule_.failAt(2, Failure.STATE_UNKNOWN, Mode.AFTER_APPLY);
    locked(apply(msTbl.deepCopy(), params(LEGACY)));
    assertEquals(newIds(), icebergIds());
    assertEquals(Collections.singletonList(Method.UPDATE_COLUMN_STATS), hms_.methods());
  }

  @Test
  public void failedReloadAfterUnknownStateKeepsFileAndCompensates() throws Exception {
    rule_.failAt(1, Failure.STATE_UNKNOWN, Mode.AFTER_APPLY);
    failLoadAt_ = 2;
    Exception e = fullError(true);
    assertError(unknownState(), e);
    assertEquals(unknownState(), e.getMessage());
    assertTrue(e.getCause() instanceof CommitStateUnknownException);
    assertTrue(registeredAtS1());
    assertEquals(puffinFilesOnDisk(), registeredFiles());
    assertEquals(oldIds(), icebergIds());
    assertEquals(2, rule_.commits());
    assertTrue(hms_.calls().isEmpty());
    assertTrue(logged(Level.ERROR, Long.toString(s1_)));
  }

  @Test
  public void interruptedCommitRestoresInterruptFlag() throws Exception {
    io_.failPath(FailingFileIO.Op.DELETE, p -> Thread.currentThread().isInterrupted());
    rule_.failAt(1, Failure.INTERRUPTED, Mode.BEFORE_APPLY);
    try {
      Exception e = fullError(false);
      assertTrue("interrupt flag", Thread.currentThread().isInterrupted());
      assertError(registerFailed("Injected interruption of commit 1"), e);
    } finally {
      Thread.interrupted();
    }
    assertTrue(puffinFilesOnDisk().isEmpty());
    assertTrue(hms_.calls().isEmpty());
  }

  @Test
  public void hmsFailureRemovesColumnsFromIds() throws Exception {
    hms_.failAt(1).failAt(4).failAt(6).failAt(7);
    assertEquals("Injected HMS failure of call 1", fullError(false).getMessage());
    assertEquals(Collections.nCopies(2, Method.ALTER_TABLE_PROPERTIES), hms_.methods());
    assertEquals(oldIds(), lastHmsIds());
    assertEquals("Injected HMS failure of call 4", fullError(false).getMessage());
    assertEquals(Arrays.asList(Method.ALTER_TABLE_PROPERTIES, Method.UPDATE_COLUMN_STATS,
        Method.ALTER_TABLE_PROPERTIES), hms_.methods().subList(2, 5));
    assertEquals(oldIds(), lastHmsIds());
    assertTrue(registeredAtS1());
    assertFalse(logged(Level.ERROR));
    // The compensation fails too: only the log tells to recompute.
    assertEquals("Injected HMS failure of call 6", fullError(false).getMessage());
    assertEquals(7, hms_.calls().size());
    assertTrue(logged(Level.ERROR, Long.toString(s1_), "COMPUTE STATS"));
  }

  @Test
  public void hmsFailureWithIntegrationRemovesColumnsFromIds() throws Exception {
    hms_.failAtInterrupting(1).failAt(2);
    io_.failPath(FailingFileIO.Op.WRITE, p -> Thread.currentThread().isInterrupted());
    assertEquals("Injected HMS failure of call 1", fullError(true).getMessage());
    assertTrue("interrupt flag", Thread.interrupted());
    assertEquals(Collections.singletonList(Method.UPDATE_COLUMN_STATS), hms_.methods());
    assertEquals(oldIds(), icebergIds());
    assertTrue(registeredAtS1());
    assertEquals(2, rule_.commits());
    assertFalse(logged(Level.ERROR));
    // The compensation fails too: the new ids stay with the old HMS statistics.
    rule_.failAt(4, Failure.COMMIT_FAILED, Mode.BEFORE_APPLY);
    assertEquals("Injected HMS failure of call 2", fullError(true).getMessage());
    assertEquals(newIds(), icebergIds());
    assertTrue(logged(Level.ERROR, Long.toString(s1_), "COMPUTE STATS"));
  }

  @Test
  public void detachColumnsWithoutIntegrationWritesIds() throws Exception {
    String before = metadataLocation();
    Table msTbl = msTbl(false, ids(1, s1_, 2, s1_, 4, s0_));
    locked(detach(msTbl, "I", "missing"));
    assertEquals(Arrays.asList(Method.ALTER_TABLE_PROPERTIES), hms_.methods());
    assertEquals(ids(2, s1_, 4, s0_), lastHmsIds());
    assertEquals(ids(2, s1_, 4, s0_), parseIds(msTbl.getParameters().get(IDS)));
    locked(detach(msTbl, "i"));
    assertEquals(1, hms_.calls().size());
    assertEquals(before, metadataLocation());
    // ids left in the Iceberg properties are removed even if no column is detached.
    setIcebergIds(ids(1, s1_));
    locked(detach(msTbl, "i"));
    assertFalse(t_.table().properties().containsKey(IDS));
    assertEquals(2, hms_.calls().size());
    assertEquals(ids(2, s1_, 4, s0_), lastHmsIds());
    assertEquals(1, rule_.commits());
  }

  @Test
  public void detachColumnsWithIntegrationCommitsIds() throws Exception {
    Table msTbl = msTbl(true, ids(1, s1_, 2, s1_, 4, s0_));
    locked(detach(msTbl, "S"));
    assertEquals(ids(1, s1_, 4, s0_), icebergIds());
    assertEquals(ids(1, s1_, 4, s0_), parseIds(msTbl.getParameters().get(IDS)));
    locked(detach(msTbl, "i", "x"));
    assertFalse(t_.table().properties().containsKey(IDS));
    assertFalse(msTbl.getParameters().containsKey(IDS));
    String before = metadataLocation();
    locked(detach(msTbl, "i"));
    assertEquals(before, metadataLocation());
    assertEquals(2, rule_.commits());
    assertTrue(hms_.calls().isEmpty());
  }

  @Test
  public void detachColumnsFailureLeavesHmsUntouched() throws Exception {
    Table msTbl = msTbl(true, ids(1, s1_, 4, s0_));
    rule_.failAt(1, Failure.COMMIT_FAILED, Mode.BEFORE_APPLY);
    assertError("Injected commit failure of commit 1",
        error(detach(msTbl.deepCopy(), "i")));
    rule_.failAt(2, Failure.STATE_UNKNOWN, Mode.AFTER_APPLY);
    assertError(unknownState(), error(detach(msTbl.deepCopy(), "i")));
    assertTrue(logged(Level.ERROR));
    assertTrue(hms_.calls().isEmpty());

    // Without ids in the Iceberg properties the set commit runs first and fails.
    t_.table().updateProperties().remove(IDS).commit();
    Table setFirst = idsOnlyInHms(ids(2, s1_));
    rule_.failAt(3, Failure.STATE_UNKNOWN, Mode.BEFORE_APPLY);
    assertError(unknownState(), error(detach(setFirst, "s")));
    assertEquals(3, rule_.commits());
    assertEquals(ids(2, s1_), parseIds(setFirst.getParameters().get(IDS)));

    hms_.failAt(1);
    Exception e = error(detach(msTbl(false, ids(1, s1_)), "i"));
    assertEquals("Injected HMS failure of call 1", e.getMessage());
    assertEquals(Arrays.asList(Method.ALTER_TABLE_PROPERTIES), hms_.methods());
  }
}
