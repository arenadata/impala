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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.StringWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.IcebergAnalysisFixture;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEventPropertyKey;
import org.apache.impala.catalog.iceberg.FailingCommitTables;
import org.apache.impala.catalog.iceberg.FailingCommitTables.Failure;
import org.apache.impala.catalog.iceberg.FailingCommitTables.Mode;
import org.apache.impala.catalog.iceberg.FailingFileIO;
import org.apache.impala.catalog.iceberg.IcebergPuffinStatsWriter;
import org.apache.impala.catalog.iceberg.IcebergTestTables;
import org.apache.impala.catalog.iceberg.PuffinFixtures;
import org.apache.impala.catalog.iceberg.PuffinFixtures.BlobSpec;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.service.FakeHmsStatsWriter.Method;
import org.apache.impala.util.IcebergUtil;
import org.apache.impala.util.IcebergUtil.ComputeStatsSnapshotPropertyConverter;
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
 * IcebergStatsUpdater#dropImpalaStatistics: the Puffin registrations, the snapshot ids
 * property, the Puffin files on disk and the HMS calls after DROP STATS and its
 * failures. S0 has a file of another engine, S1 a file with Impala blobs and a blob of
 * another engine, S2 a file of Impala blobs only.
 */
public class IcebergStatsUpdaterDropTest extends FrontendTestBase {
  private static final String DB = "iceberg_stats_updater_drop";
  private static final String IDS = IcebergTable.COMPUTE_STATS_SNAPSHOT_IDS;
  private static final String IMPALA = "Impala 4.5.0-updater-drop-test";
  private static final String SERVICE_ID =
      MetastoreEventPropertyKey.CATALOG_SERVICE_ID.getKey();
  private static final String VERSION =
      MetastoreEventPropertyKey.CATALOG_VERSION.getKey();
  private static final Schema SCHEMA = new Schema(
      optional(1, "i", Types.IntegerType.get()),
      optional(2, "s", Types.StringType.get()),
      optional(3, "x", Types.IntegerType.get()));
  private static final Class<?>[] LOGGED =
      {IcebergStatsUpdater.class, IcebergPuffinStatsWriter.class};

  @Rule public TemporaryFolder tmp = new TemporaryFolder();
  @Rule public TestName testName = new TestName();

  private static final IcebergAnalysisFixture ifx =
      IcebergAnalysisFixture.create(feFixture_);

  private final FailingCommitTables.Rule rule_ = new FailingCommitTables.Rule();
  /** Number of Iceberg commits made before each alterTableProperties call. */
  private final List<Integer> commitsAtAlter_ = new ArrayList<>();
  private final FakeHmsStatsWriter hms_ = new FakeHmsStatsWriter() {
    @Override
    public void alterTableProperties(Table msTbl) throws ImpalaException {
      commitsAtAlter_.add(rule_.commits());
      super.alterTableProperties(msTbl);
    }
  };
  private final StringWriter log_ = new StringWriter();
  private final WriterAppender appender_ =
      new WriterAppender(new PatternLayout("%p %m%n"), log_);
  private IcebergTestTables t_;
  private IcebergTable tbl_;
  private FailingFileIO io_;
  private long s0_;
  private long s1_;
  private long s2_;
  private StatisticsFile foreign_;
  private StatisticsFile mixed_;
  private int loads_;
  /** Number of the load that fails, 0 if none. */
  private int failLoadAt_;
  /** Runs after each load, so the loaded table does not see its changes. */
  private Runnable afterLoad_;
  /** Runs before each load, so the loaded table sees its changes. */
  private Runnable beforeLoad_;
  private IcebergStatsUpdater updater_;

  @Before
  public void setUpTable() throws Exception {
    for (Class<?> c : LOGGED) Logger.getLogger(c).addAppender(appender_);
    t_ = IcebergTestTables.create(tmp, DB, testName.getMethodName().toLowerCase(),
        SCHEMA, PartitionSpec.unpartitioned(), 2);
    // Iceberg would retry a failed commit and hide the injected failure.
    t_.table().updateProperties().set(TableProperties.COMMIT_NUM_RETRIES, "0").commit();
    s0_ = t_.append(t_.file(100, null));
    s1_ = t_.append(t_.file(100, null));
    s2_ = t_.append(t_.file(100, null));
    foreign_ = register(s0_, foreignBlob(s0_));
    mixed_ = register(s1_, impalaBlob("i", s1_), impalaBlob("s", s1_), foreignBlob(s1_));
    register(s2_, impalaBlob("i", s2_), impalaBlob("s", s2_));
    tbl_ = ifx.addPathB(t_, Collections.emptyMap());
    io_ = new FailingFileIO(t_.table().io());
    updater_ = new IcebergStatsUpdater(feTbl -> {
      if (++loads_ == failLoadAt_) {
        throw new NotFoundException("Injected failure of load %s", loads_);
      }
      if (beforeLoad_ != null) beforeLoad_.run();
      org.apache.iceberg.Table real = IcebergUtil.loadTable(feTbl);
      if (afterLoad_ != null) afterLoad_.run();
      return new BaseTable(new FailingCommitTables(
          ((HasTableOperations) real).operations(), io_, rule_), real.name());
    });
  }

  @After
  public void removeAppender() {
    for (Class<?> c : LOGGED) Logger.getLogger(c).removeAppender(appender_);
  }

  private static BlobSpec impalaBlob(String col, long snapshotId) {
    return blob(col, snapshotId).impalaCreatedBy(IMPALA);
  }

  /** Blob of another engine; it has no Impala marker. */
  private static BlobSpec foreignBlob(long snapshotId) { return blob("x", snapshotId); }

  private static BlobSpec blob(String col, long snapshotId) {
    Types.NestedField field = SCHEMA.findField(col);
    List<?> values = col.equals("s") ? Arrays.asList("a", "b") : Arrays.asList(1, 2, 3);
    return PuffinFixtures.blob(field.fieldId(), snapshotId,
        PuffinFixtures.sketch(field.type(), values));
  }

  private StatisticsFile register(long snapshotId, BlobSpec... blobs) {
    StatisticsFile file = PuffinFixtures.write(t_.table(), snapshotId,
        Arrays.asList(blobs));
    t_.setStats(snapshotId, file);
    return file;
  }

  private String idsValue() {
    Map<Integer, Long> ids = new TreeMap<>();
    ids.put(1, s2_);
    ids.put(2, s1_);
    return ComputeStatsSnapshotPropertyConverter.mapToString(new TreeMap<>(ids));
  }

  /** HMS table copy with the ids; integrated: of synchronized HiveCatalog table. */
  private Table msTbl(boolean integrated) {
    Table msTbl = tbl_.getMetaStoreTable().deepCopy();
    msTbl.putToParameters(IDS, idsValue());
    if (integrated) hiveCatalog(msTbl).putToParameters("external.table.purge", "TRUE");
    return msTbl;
  }

  private static Table hiveCatalog(Table msTbl) {
    msTbl.setTableType(TableType.EXTERNAL_TABLE.toString());
    msTbl.putToParameters("EXTERNAL", "TRUE");
    msTbl.putToParameters(IcebergTable.ICEBERG_CATALOG, "hive.catalog");
    return msTbl;
  }

  private void setIcebergIds() {
    t_.table().updateProperties().set(IDS, idsValue()).commit();
  }

  private TableMetadata metadata() {
    return ((HasTableOperations) t_.table()).operations().current();
  }

  private String metadataLocation() { return metadata().metadataFileLocation(); }

  private static String fileName(String path) { return new Path(path).getName(); }

  private Set<String> puffinFilesOnDisk() throws Exception {
    File dir = new File(new URI(t_.table().location() + "/metadata"));
    return Arrays.stream(dir.listFiles((d, n) -> n.endsWith(".stats")))
        .map(File::getName).collect(Collectors.toCollection(TreeSet::new));
  }

  private Map<Long, StatisticsFile> registered() {
    return t_.table().statisticsFiles().stream().collect(
        Collectors.toMap(StatisticsFile::snapshotId, f -> f, (a, b) -> a, TreeMap::new));
  }

  private Map<Long, String> registeredPaths() {
    Map<Long, String> res = new TreeMap<>();
    registered().forEach((s, f) -> res.put(s, f.path()));
    return res;
  }

  /**
   * S0 keeps the file of another engine, S1 has a new copy with the blob of another
   * engine only, S2 has no file; the files on disk before stay there.
   */
  private void assertImpalaBlobsDropped(Set<String> filesBefore) throws Exception {
    Map<Long, StatisticsFile> files = registered();
    assertEquals(new TreeSet<>(Arrays.asList(s0_, s1_)), files.keySet());
    assertEquals(foreign_.path(), files.get(s0_).path());
    StatisticsFile copy = files.get(s1_);
    assertFalse(filesBefore.contains(fileName(copy.path())));
    assertEquals(Collections.singletonList(mixed_.blobMetadata().get(2)),
        copy.blobMetadata());
    Set<String> expected = new TreeSet<>(filesBefore);
    expected.add(fileName(copy.path()));
    assertEquals(expected, puffinFilesOnDisk());
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

  private Call drop(Table msTbl) {
    return () -> updater_.dropImpalaStatistics(tbl_, msTbl, hms_);
  }

  private static void assertError(String expectedStart, Exception e) {
    assertTrue("not ImpalaRuntimeException: " + e, e instanceof ImpalaRuntimeException);
    assertTrue(e.getMessage(), e.getMessage().startsWith(expectedStart));
  }

  private String dropFailed(String cause) {
    return "Failed to drop Puffin statistics of table " + tbl_.getFullName() + ": "
        + cause;
  }

  private String unknownState() {
    return "Commit state of statistics of table " + tbl_.getFullName()
        + " is unknown; run DROP STATS again.";
  }

  /** Some message of 'level' names the table and contains all 'parts'. */
  private boolean logged(Level level, String... parts) {
    return Arrays.stream(log_.toString().split("\n")).anyMatch(
        l -> l.startsWith(level + " ") && l.contains(tbl_.getFullName())
            && Arrays.stream(parts).allMatch(l::contains));
  }

  @Test
  public void dropWithoutIntegrationRemovesImpalaBlobsAndIds() throws Exception {
    Set<String> before = puffinFilesOnDisk();
    // A stale copy of the ids in the Iceberg properties goes in the same commit.
    setIcebergIds();
    Table msTbl = msTbl(false);
    locked(drop(msTbl));
    assertImpalaBlobsDropped(before);
    assertEquals(1, rule_.commits());
    assertFalse(t_.table().properties().containsKey(IDS));
    assertFalse(msTbl.getParameters().containsKey(IDS));
    assertEquals(Collections.singletonList(Method.ALTER_TABLE_PROPERTIES),
        hms_.methods());
    assertFalse(hms_.last(Method.ALTER_TABLE_PROPERTIES).msTbl.getParameters()
        .containsKey(IDS));
    assertEquals(Collections.singletonList(1), commitsAtAlter_);

    // Nothing is left to drop: no commit, and outside HiveCatalog HMS keeps no metadata
    // location, so no HMS write either.
    String location = metadataLocation();
    locked(drop(msTbl));
    assertEquals(location, metadataLocation());
    assertEquals(1, rule_.commits());
    assertEquals(1, hms_.calls().size());
  }

  @Test
  public void dropWithIntegrationCommitsBlobsAndIdsTogether() throws Exception {
    Set<String> before = puffinFilesOnDisk();
    setIcebergIds();
    Table msTbl = msTbl(true);
    msTbl.putToParameters(SERVICE_ID, "catalog-1");
    msTbl.putToParameters(VERSION, "42");
    msTbl.putToParameters(HdfsTable.TBL_PROP_LAST_COMPUTE_STATS_TIME, "1700000000");
    locked(drop(msTbl));
    assertImpalaBlobsDropped(before);
    assertEquals(1, rule_.commits());
    Map<String, String> props = t_.table().properties();
    assertFalse(props.containsKey(IDS));
    assertEquals("catalog-1", props.get(SERVICE_ID));
    assertEquals("42", props.get(VERSION));
    assertFalse(props.containsKey(HdfsTable.TBL_PROP_LAST_COMPUTE_STATS_TIME));
    assertFalse(msTbl.getParameters().containsKey(IDS));
    assertTrue(hms_.calls().isEmpty());

    // ids known only to HMS are set first, so that the next commit removes them there.
    Table idsOnlyInHms = msTbl(true);
    locked(drop(idsOnlyInHms));
    assertEquals(3, rule_.commits());
    assertFalse(t_.table().properties().containsKey(IDS));
    String previous = metadata().previousFiles()
        .get(metadata().previousFiles().size() - 1).file();
    assertEquals(idsValue(), TableMetadataParser.read(t_.table().io(), previous)
        .properties().get(IDS));
    assertFalse(idsOnlyInHms.getParameters().containsKey(IDS));
    assertTrue(hms_.calls().isEmpty());
  }

  @Test
  public void externalHiveCatalogTableWithoutIdsAltersAfterCommit() throws Exception {
    Table msTbl = hiveCatalog(tbl_.getMetaStoreTable().deepCopy());
    assertFalse(msTbl.getParameters().containsKey(IDS));
    locked(drop(msTbl));
    assertEquals(1, rule_.commits());
    assertEquals(Collections.singletonList(Method.ALTER_TABLE_PROPERTIES),
        hms_.methods());
    assertEquals(Collections.singletonList(1), commitsAtAlter_);

    // Nothing is left to commit, yet the caller writes 'msTbl' next: it must take the
    // metadata location from HMS.
    locked(drop(msTbl));
    assertEquals(1, rule_.commits());
    assertEquals(Arrays.asList(1, 1), commitsAtAlter_);
  }

  @Test
  public void dropFailureKeepsStatisticsAndHms() throws Exception {
    Set<String> files = puffinFilesOnDisk();
    Map<Long, String> registration = registeredPaths();
    String location = metadataLocation();
    Table msTbl = msTbl(false);
    failLoadAt_ = 1;
    assertError("Failed to load Iceberg table " + tbl_.getFullName()
        + ": Injected failure of load 1", error(drop(msTbl)));
    io_.failCall(FailingFileIO.Op.WRITE, 1);
    assertError(dropFailed("Failed to write Puffin statistics file "),
        error(drop(msTbl)));
    io_.clearRules();
    rule_.failAt(1, Failure.COMMIT_FAILED, Mode.BEFORE_APPLY);
    assertError(dropFailed("Injected commit failure of commit 1"), error(drop(msTbl)));
    rule_.failAt(2, Failure.VALIDATION, Mode.BEFORE_APPLY);
    assertError(dropFailed("Injected validation failure of commit 2"),
        error(drop(msTbl)));
    assertEquals(registration, registeredPaths());
    assertEquals(files, puffinFilesOnDisk());
    assertEquals(location, metadataLocation());

    // Another engine replaces the file of S1 after the load.
    List<StatisticsFile> replaced = new ArrayList<>();
    afterLoad_ = () -> {
      if (loads_ == 5) replaced.add(register(s1_, foreignBlob(s1_)));
    };
    assertError(dropFailed("Statistics of snapshot " + s1_ + " of table "
        + tbl_.getFullName() + " changed concurrently"), error(drop(msTbl)));
    registration.put(s1_, replaced.get(0).path());
    assertEquals(registration, registeredPaths());
    files.add(fileName(replaced.get(0).path()));
    assertEquals(files, puffinFilesOnDisk());
    assertEquals(2, rule_.commits());
    assertEquals(idsValue(), msTbl.getParameters().get(IDS));
    assertTrue(hms_.calls().isEmpty());
    assertTrue(logged(Level.WARN, "Failed to drop"));
  }

  @Test
  public void dropCommitFailureAfterSetCommitKeepsIdsInIceberg() throws Exception {
    Set<String> files = puffinFilesOnDisk();
    Map<Long, String> registration = registeredPaths();
    Table msTbl = msTbl(true);
    rule_.failAt(2, Failure.COMMIT_FAILED, Mode.BEFORE_APPLY);
    assertError(dropFailed("Injected commit failure of commit 2"), error(drop(msTbl)));
    assertEquals(2, rule_.commits());
    assertEquals(idsValue(), t_.table().properties().get(IDS));
    assertEquals(registration, registeredPaths());
    assertEquals(files, puffinFilesOnDisk());
    assertEquals(idsValue(), msTbl.getParameters().get(IDS));
    assertTrue(hms_.calls().isEmpty());
  }

  @Test
  public void unknownStateIsResolvedByReload() throws Exception {
    Set<String> files = puffinFilesOnDisk();
    Table msTbl = msTbl(false);
    rule_.failAt(1, Failure.STATE_UNKNOWN, Mode.BEFORE_APPLY);
    Exception e = error(drop(msTbl));
    assertError(dropFailed("Injected unknown state of commit 1"), e);
    assertTrue(e.getCause() instanceof CommitStateUnknownException);
    assertEquals(2, loads_);
    assertEquals(files, puffinFilesOnDisk());
    assertEquals(idsValue(), msTbl.getParameters().get(IDS));
    assertTrue(hms_.calls().isEmpty());

    rule_.failAt(2, Failure.STATE_UNKNOWN, Mode.AFTER_APPLY);
    locked(drop(msTbl));
    assertEquals(4, loads_);
    assertImpalaBlobsDropped(files);
    assertFalse(msTbl.getParameters().containsKey(IDS));
    assertEquals(Collections.singletonList(2), commitsAtAlter_);
  }

  @Test
  public void interruptedCommitIsResolvedByReload() throws Exception {
    Set<String> files = puffinFilesOnDisk();
    rule_.failAt(1, Failure.INTERRUPTED, Mode.BEFORE_APPLY);
    try {
      Exception e = error(drop(msTbl(false)));
      assertTrue("interrupt flag", Thread.currentThread().isInterrupted());
      assertError(dropFailed("Injected interruption of commit 1"), e);
    } finally {
      Thread.interrupted();
    }
    assertEquals(2, loads_);
    assertEquals(files, puffinFilesOnDisk());
    assertTrue(hms_.calls().isEmpty());
  }

  @Test
  public void interruptedCommitDeletesCopiesWithInterruptFlagCleared() throws Exception {
    Set<String> files = puffinFilesOnDisk();
    io_.failPath(FailingFileIO.Op.DELETE, p -> Thread.currentThread().isInterrupted());
    rule_.failAt(1, Failure.INTERRUPTED, Mode.BEFORE_APPLY);
    try {
      Exception e = error(drop(msTbl(false)));
      assertTrue("interrupt flag", Thread.currentThread().isInterrupted());
      assertError(dropFailed("Injected interruption of commit 1"), e);
    } finally {
      Thread.interrupted();
    }
    assertEquals(files, puffinFilesOnDisk());
  }

  @Test
  public void unresolvedUnknownStateKeepsCopiesAndHms() throws Exception {
    Set<String> files = puffinFilesOnDisk();
    Table msTbl = msTbl(false);
    rule_.failAt(1, Failure.STATE_UNKNOWN, Mode.AFTER_APPLY);
    failLoadAt_ = 2;
    Exception e = error(drop(msTbl));
    assertEquals(unknownState(), e.getMessage());
    assertTrue(e.getCause() instanceof CommitStateUnknownException);
    assertImpalaBlobsDropped(files);
    assertEquals(idsValue(), msTbl.getParameters().get(IDS));
    assertTrue(hms_.calls().isEmpty());
    assertTrue(logged(Level.ERROR, "DROP STATS"));

    // The outcome of the set commit is not looked for.
    Table setFirst = msTbl(true);
    rule_.failAt(2, Failure.STATE_UNKNOWN, Mode.BEFORE_APPLY);
    assertEquals(unknownState(), error(drop(setFirst)).getMessage());
    assertEquals(3, loads_);
    assertEquals(2, rule_.commits());
    assertEquals(idsValue(), setFirst.getParameters().get(IDS));
    assertTrue(hms_.calls().isEmpty());
  }

  @Test
  public void registeredCopyMeansAppliedDespiteForeignIdsKey() throws Exception {
    Set<String> files = puffinFilesOnDisk();
    Table msTbl = msTbl(false);
    rule_.failAt(1, Failure.STATE_UNKNOWN, Mode.AFTER_APPLY);
    // Another engine sets the ids key between the commit and the reload.
    beforeLoad_ = () -> {
      if (loads_ == 2) t_.table().updateProperties().set(IDS, idsValue()).commit();
    };
    locked(drop(msTbl));
    assertEquals(2, loads_);
    assertImpalaBlobsDropped(files);
    assertFalse(msTbl.getParameters().containsKey(IDS));
    assertFalse(logged(Level.WARN, "Failed to drop"));
  }

  @Test
  public void alterFailureAfterCommitIsReported() throws Exception {
    Set<String> files = puffinFilesOnDisk();
    Table msTbl = msTbl(false);
    hms_.failAt(1);
    assertEquals("Injected HMS failure of call 1", error(drop(msTbl)).getMessage());
    assertImpalaBlobsDropped(files);
    assertFalse(msTbl.getParameters().containsKey(IDS));
    assertEquals(Collections.singletonList(Method.ALTER_TABLE_PROPERTIES),
        hms_.methods());
  }

  @Test
  public void unreadableMixedFileFailsAndMissingOneLosesRegistration()
      throws Exception {
    Set<String> files = puffinFilesOnDisk();
    Table msTbl = msTbl(false);
    io_.failPath(FailingFileIO.Op.READ, p -> p.equals(mixed_.path()));
    assertError(dropFailed("Failed to read Puffin statistics file " + mixed_.path()
        + " of table " + tbl_.getFullName() + ": "), error(drop(msTbl)));
    assertEquals(files, puffinFilesOnDisk());
    assertEquals(0, rule_.commits());
    assertTrue(hms_.calls().isEmpty());

    // The blobs of another engine in a missing file are already lost.
    io_.clearRules();
    t_.table().io().deleteFile(mixed_.path());
    locked(drop(msTbl));
    assertEquals(Collections.singletonMap(s0_, foreign_.path()), registeredPaths());
    assertTrue(logged(Level.WARN, mixed_.path(), "is missing; lost blobs"));
    assertEquals(1, rule_.commits());
    assertEquals(Collections.singletonList(1), commitsAtAlter_);
    files.remove(fileName(mixed_.path()));
    assertEquals(files, puffinFilesOnDisk());
  }
}
