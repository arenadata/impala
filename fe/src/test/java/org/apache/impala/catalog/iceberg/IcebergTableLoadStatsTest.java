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

package org.apache.impala.catalog.iceberg;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.datasketches.theta.CompactSketch;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.IcebergAnalysisFixture;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.catalog.PuffinStatsLoader;
import org.apache.impala.catalog.PuffinStatsLoader.PuffinStatsRecord;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.service.BackendConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

/**
 * Loads Iceberg tables through the embedded HMS and checks which NDV, HMS or Puffin,
 * each column gets. Every load also checks that the column of the IcebergTable and the
 * one of its HdfsTable (read by local catalog) agree.
 */
public class IcebergTableLoadStatsTest extends FrontendTestBase {
  private static final String DB = "iceberg_load_stats";
  private static final int ID_FIELD = 1;
  private static final int S_FIELD = 2;
  private static final int B_FIELD = 3;
  private static final Schema SCHEMA = new Schema(
      required(ID_FIELD, "id", Types.IntegerType.get()),
      optional(S_FIELD, "s", Types.StringType.get()),
      optional(B_FIELD, "b", Types.BooleanType.get()));
  private static final long HMS_NDV = 1000;

  @Rule public TemporaryFolder tmp = new TemporaryFolder();
  @Rule public TestName testName = new TestName();

  private final IcebergAnalysisFixture ifx = IcebergAnalysisFixture.create(feFixture_);
  private final boolean readPuffinBefore =
      BackendConfig.INSTANCE.getBackendCfg().isEnable_reading_puffin_stats();

  @Before
  public void enablePuffinReading() {
    setReadPuffin(true);
  }

  @After
  public void restorePuffinReading() {
    setReadPuffin(readPuffinBefore);
  }

  private static void setReadPuffin(boolean enabled) {
    BackendConfig.INSTANCE.getBackendCfg().setEnable_reading_puffin_stats(enabled);
  }

  private IcebergTestTables newTable() {
    return IcebergTestTables.create(tmp, DB, testName.getMethodName().toLowerCase(),
        SCHEMA, PartitionSpec.unpartitioned(), 2);
  }

  private static CompactSketch distinctStrings(int count) {
    List<Object> values = new ArrayList<>();
    for (int i = 0; i < count; ++i) values.add("v" + i);
    // Duplicates and nulls must not change the estimate.
    values.add("v0");
    values.add(null);
    return PuffinFixtures.sketch(Types.StringType.get(), values);
  }

  private static long estimate(CompactSketch sketch) {
    return Math.round(sketch.getEstimate());
  }

  /** Registers a Puffin file with one blob per spec, all from 'fileSnapshot'. */
  private static StatisticsFile addStats(IcebergTestTables t, long fileSnapshot,
      PuffinFixtures.BlobSpec... blobs) {
    StatisticsFile file = PuffinFixtures.write(t.table(), fileSnapshot,
        Arrays.asList(blobs));
    t.setStats(fileSnapshot, file);
    return file;
  }

  private static String ids(int fieldId, long snapshotId) {
    return fieldId + ":" + snapshotId;
  }

  /** Registers 't' in HMS with the given ids property (none if null) and HMS NDV. */
  private void register(IcebergTestTables t, String ids) throws Exception {
    Map<String, String> params = new HashMap<>();
    if (ids != null) params.put(IcebergTable.COMPUTE_STATS_SNAPSHOT_IDS, ids);
    ifx.registerPathA(t, params);
    ifx.setHmsColumnNdv(t, "s", HMS_NDV);
  }

  private void setIds(IcebergTestTables t, String ids) throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put(IcebergTable.COMPUTE_STATS_SNAPSHOT_IDS, ids);
    ifx.setTableParams(t, params);
  }

  private void setReadPuffinProperty(IcebergTestTables t, String value)
      throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put(IcebergTable.ICEBERG_DISABLE_READING_PUFFIN_STATS, value);
    ifx.setTableParams(t, params);
  }

  /** Loads the table anew and returns the NDV of 'col', equal in both column copies. */
  private long loadNdv(IcebergTestTables t, String col) throws Exception {
    IcebergTable tbl = ifx.reloadPathA(t);
    long ndv = tbl.getColumn(col).getStats().getNumDistinctValues();
    assertEquals("NDV of " + col + " in getHdfsTable()", ndv,
        tbl.getHdfsTable().getColumn(col).getStats().getNumDistinctValues());
    return ndv;
  }

  private static File localFile(StatisticsFile file) {
    return new File(new Path(file.path()).toUri().getPath());
  }

  private static void corrupt(StatisticsFile file) throws Exception {
    File local = localFile(file);
    byte[] garbage = new byte[(int) local.length()];
    new Random(42).nextBytes(garbage);
    try (RandomAccessFile f = new RandomAccessFile(local, "rw")) {
      f.write(garbage);
    }
  }

  @Test
  public void noIdsKeyUsesBlob() throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.append(t.file(10, null));
    CompactSketch sketch = distinctStrings(5);
    addStats(t, s1, PuffinFixtures.blob(S_FIELD, s1, sketch).ndvProperty(null));
    register(t, null);
    // A later COMPUTE STATS time must not hide the blob.
    Map<String, String> params = new HashMap<>();
    params.put("impala.lastComputeStatsTime",
        Long.toString(System.currentTimeMillis() / 1000 + 3600));
    ifx.setTableParams(t, params);
    assertEquals(estimate(sketch), loadNdv(t, "s"));
  }

  @Test
  public void minusOneIdsUsesBlob() throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.append(t.file(10, null));
    CompactSketch sketch = distinctStrings(5);
    addStats(t, s1, PuffinFixtures.blob(S_FIELD, s1, sketch).ndvProperty(null));
    register(t, ids(S_FIELD, -1));
    assertEquals(estimate(sketch), loadNdv(t, "s"));
  }

  @Test
  public void expiredIdsSnapshotUsesBlob() throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.append(t.file(10, null));
    long s2 = t.append(t.file(10, null));
    CompactSketch sketch = distinctStrings(6);
    addStats(t, s2, PuffinFixtures.blob(S_FIELD, s2, sketch).ndvProperty(null));
    register(t, ids(S_FIELD, s1));
    t.expire(s1);
    assertEquals(estimate(sketch), loadNdv(t, "s"));
  }

  @Test
  public void unknownIdsSnapshotUsesBlob() throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.append(t.file(10, null));
    CompactSketch sketch = distinctStrings(4);
    addStats(t, s1, PuffinFixtures.blob(S_FIELD, s1, sketch).ndvProperty(null));
    register(t, ids(S_FIELD, s1 + 1));
    assertEquals(estimate(sketch), loadNdv(t, "s"));
  }

  @Test
  public void sameSnapshotUsesBlob() throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.append(t.file(10, null));
    long s2 = t.append(t.file(10, null));
    CompactSketch sketch = distinctStrings(7);
    addStats(t, s2, PuffinFixtures.blob(S_FIELD, s2, sketch).ndvProperty(null));
    register(t, ids(S_FIELD, s2));
    assertEquals(estimate(sketch), loadNdv(t, "s"));
  }

  @Test
  public void olderIdsSnapshotUsesBlob() throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.append(t.file(10, null));
    long s2 = t.append(t.file(10, null));
    CompactSketch sketch = distinctStrings(8);
    addStats(t, s2, PuffinFixtures.blob(S_FIELD, s2, sketch).ndvProperty(null));
    register(t, ids(S_FIELD, s1));
    assertEquals(estimate(sketch), loadNdv(t, "s"));
  }

  @Test
  public void newerIdsSnapshotUsesHms() throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.append(t.file(10, null));
    long s2 = t.append(t.file(10, null));
    addStats(t, s1,
        PuffinFixtures.blob(S_FIELD, s1, distinctStrings(9)).ndvProperty(null));
    register(t, ids(S_FIELD, s2));
    assertEquals(HMS_NDV, loadNdv(t, "s"));
  }

  @Test
  public void newestRemainingBlobWins() throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.append(t.file(10, null));
    long s2 = t.append(t.file(10, null));
    long s3 = t.append(t.file(10, null));
    CompactSketch older = distinctStrings(3);
    CompactSketch newer = distinctStrings(11);
    addStats(t, s1, PuffinFixtures.blob(S_FIELD, s1, older).ndvProperty(null));
    addStats(t, s3, PuffinFixtures.blob(S_FIELD, s3, newer).ndvProperty(null));
    register(t, ids(S_FIELD, s2));
    assertEquals(estimate(newer), loadNdv(t, "s"));
  }

  /**
   * Snapshots of equal time cannot be committed on purpose, so the time of the ids
   * snapshot is faked on a spy of the table.
   */
  @Test
  public void equalSnapshotTimeUsesBlob() throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.append(t.file(10, null));
    long s2 = t.append(t.file(10, null));
    CompactSketch sketch = distinctStrings(5);
    addStats(t, s1, PuffinFixtures.blob(S_FIELD, s1, sketch).ndvProperty(null));
    Table table = t.table();
    Map<Integer, Long> fieldIdsWithHmsStats = Collections.singletonMap(S_FIELD, s2);
    String tblName = t.db() + "." + t.name();

    Map<Integer, PuffinStatsRecord> realTimes = PuffinStatsLoader.loadPuffinStats(
        table, tblName, fieldIdsWithHmsStats);
    assertFalse("ids snapshot is newer than the blob", realTimes.containsKey(S_FIELD));

    Table spyTable = spy(table);
    Snapshot sameTime = mock(Snapshot.class);
    when(sameTime.snapshotId()).thenReturn(s2);
    when(sameTime.timestampMillis()).thenReturn(table.snapshot(s1).timestampMillis());
    doReturn(sameTime).when(spyTable).snapshot(s2);
    Map<Integer, PuffinStatsRecord> equalTimes = PuffinStatsLoader.loadPuffinStats(
        spyTable, tblName, fieldIdsWithHmsStats);
    assertTrue(equalTimes.containsKey(S_FIELD));
    assertEquals(s1, equalTimes.get(S_FIELD).snapshotId);
    assertEquals(estimate(sketch), equalTimes.get(S_FIELD).ndv);
  }

  @Test
  public void invalidIdsUseBlob() throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.append(t.file(10, null));
    long s2 = t.append(t.file(10, null));
    CompactSketch sketch = distinctStrings(5);
    addStats(t, s1, PuffinFixtures.blob(S_FIELD, s1, sketch).ndvProperty(null));
    register(t, ids(S_FIELD, s2));
    assertEquals(HMS_NDV, loadNdv(t, "s"));
    // Each value is rejected as a whole, so the newer ids snapshot is ignored.
    for (String ids : Arrays.asList("garbage", "2:abc", "2", ":", "2:" + s2 + ":1",
        "x-2:" + s2, "1-" + Integer.MAX_VALUE + ":" + s2)) {
      setIds(t, ids);
      assertEquals("ids '" + ids + "'", estimate(sketch), loadNdv(t, "s"));
    }
  }

  @Test
  public void readingDisabledUsesHms() throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.append(t.file(10, null));
    CompactSketch sketch = distinctStrings(5);
    addStats(t, s1, PuffinFixtures.blob(S_FIELD, s1, sketch).ndvProperty(null));
    // With reading on, the blob would win over HMS.
    register(t, ids(S_FIELD, -1));
    setReadPuffin(false);
    assertEquals(HMS_NDV, loadNdv(t, "s"));
    setReadPuffin(true);
    for (String off : Arrays.asList("false", "no", "0", "")) {
      setReadPuffinProperty(t, off);
      assertEquals("property '" + off + "'", HMS_NDV, loadNdv(t, "s"));
    }
  }

  @Test
  public void readingSwitchAppliesFromNextLoad() throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.append(t.file(10, null));
    CompactSketch sketch = distinctStrings(5);
    addStats(t, s1, PuffinFixtures.blob(S_FIELD, s1, sketch).ndvProperty(null));
    register(t, null);
    setReadPuffin(false);
    assertEquals(HMS_NDV, loadNdv(t, "s"));
    setReadPuffin(true);
    assertEquals(estimate(sketch), loadNdv(t, "s"));
    setReadPuffinProperty(t, "false");
    assertEquals(HMS_NDV, loadNdv(t, "s"));
    setReadPuffinProperty(t, "TRUE");
    assertEquals(estimate(sketch), loadNdv(t, "s"));
  }

  @Test
  public void expiredBlobSnapshotUsesHms() throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.append(t.file(10, null));
    long s2 = t.append(t.file(10, null));
    CompactSketch sketch = distinctStrings(5);
    addStats(t, s2, PuffinFixtures.blob(S_FIELD, s1, sketch).ndvProperty(null));
    register(t, null);
    assertEquals(estimate(sketch), loadNdv(t, "s"));
    t.expire(s1);
    assertEquals(HMS_NDV, loadNdv(t, "s"));
  }

  @Test
  public void missingOrCorruptFileWithoutNdvUsesHms() throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.append(t.file(10, null));
    StatisticsFile file = addStats(t, s1,
        PuffinFixtures.blob(S_FIELD, s1, distinctStrings(5)).ndvProperty(null));
    register(t, null);
    corrupt(file);
    assertEquals(HMS_NDV, loadNdv(t, "s"));
    assertTrue(localFile(file).delete());
    assertEquals(HMS_NDV, loadNdv(t, "s"));
  }

  @Test
  public void missingOrCorruptFileKeepsNdvProperty() throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.append(t.file(10, null));
    StatisticsFile file = addStats(t, s1,
        PuffinFixtures.blob(S_FIELD, s1, distinctStrings(5)).ndvProperty("42"));
    register(t, null);
    corrupt(file);
    assertEquals(42, loadNdv(t, "s"));
    assertTrue(localFile(file).delete());
    assertEquals(42, loadNdv(t, "s"));
  }

  @Test
  public void nonNumericNdvPropertyUsesSketch() throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.append(t.file(10, null));
    CompactSketch sketch = distinctStrings(12);
    addStats(t, s1, PuffinFixtures.blob(S_FIELD, s1, sketch).ndvProperty("abc"));
    register(t, null);
    assertEquals(estimate(sketch), loadNdv(t, "s"));
  }

  @Test
  public void avroDataFileBlobWins() throws Exception {
    IcebergTestTables t = newTable();
    // Avro data files make the load rebuild the HdfsTable columns from HMS.
    long s1 = t.append(t.file(10, null, FileFormat.AVRO));
    CompactSketch sketch = distinctStrings(5);
    addStats(t, s1, PuffinFixtures.blob(S_FIELD, s1, sketch).ndvProperty(null));
    register(t, null);
    assertEquals(estimate(sketch), loadNdv(t, "s"));
  }

  @Test
  public void staleHmsColumnsSkipBlob() throws Exception {
    IcebergTestTables t = newTable();
    // Avro data files make the load take the HdfsTable columns from HMS 'sd.cols'.
    long s1 = t.append(t.file(10, null, FileFormat.AVRO));
    CompactSketch sketch = distinctStrings(5);
    addStats(t, s1, PuffinFixtures.blob(S_FIELD, s1, sketch).ndvProperty(null));
    register(t, null);
    // Another engine renames the column but leaves 'sd.cols' in HMS as is.
    t.table().updateSchema().renameColumn("s", "s_renamed").commit();
    IcebergTable tbl = ifx.reloadPathA(t);
    long icebergNdv =
        tbl.getColumn("s_renamed").getStats().getNumDistinctValues();
    // The HdfsTable still has the stale HMS columns, so the copies cannot be compared.
    assertNull(tbl.getHdfsTable().getColumn("s_renamed"));
    assertEquals(-1, icebergNdv);
    assertEquals(HMS_NDV,
        tbl.getHdfsTable().getColumn("s").getStats().getNumDistinctValues());
  }

  @Test
  public void renamedColumnKeepsFieldId() throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.append(t.file(10, null));
    long s2 = t.append(t.file(10, null));
    CompactSketch sketch = distinctStrings(5);
    addStats(t, s1, PuffinFixtures.blob(S_FIELD, s1, sketch).ndvProperty(null));
    register(t, ids(S_FIELD, s2));
    t.table().updateSchema().renameColumn("s", "s_renamed").commit();
    // The table is in hadoop.catalog, so HMS 'sd.cols' is synced here the way a
    // HiveCatalog commit would do it. This lets HMS hold stats for the new name.
    try (MetaStoreClient client = ifx.catalog().getMetaStoreClient()) {
      org.apache.hadoop.hive.metastore.api.Table msTbl =
          client.getHiveClient().getTable(t.db(), t.name());
      msTbl.getSd().setCols(t.msTable(Collections.emptyMap()).getSd().getCols());
      client.getHiveClient().alter_table(t.db(), t.name(), msTbl);
    }
    ifx.setHmsColumnNdv(t, "s_renamed", HMS_NDV);
    // ids still points at a newer snapshot than the blob, so the blob loses.
    assertEquals(HMS_NDV, loadNdv(t, "s_renamed"));
    setIds(t, ids(S_FIELD, s1));
    assertEquals(estimate(sketch), loadNdv(t, "s_renamed"));
  }

  @Test
  public void duplicateBlobsUseFirst() throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.append(t.file(10, null));
    CompactSketch first = distinctStrings(5);
    addStats(t, s1,
        PuffinFixtures.blob(S_FIELD, s1, first).ndvProperty(null),
        PuffinFixtures.blob(S_FIELD, s1, distinctStrings(13)).ndvProperty(null));
    register(t, null);
    assertEquals(estimate(first), loadNdv(t, "s"));
  }

  @Test
  public void negativeNdvPropertyKeepsHms() throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.append(t.file(10, null));
    addStats(t, s1,
        PuffinFixtures.blob(S_FIELD, s1, distinctStrings(5)).ndvProperty("-5"));
    register(t, null);
    assertEquals(HMS_NDV, loadNdv(t, "s"));
  }

  @Test
  public void booleanBlobKeepsHms() throws Exception {
    IcebergTestTables t = newTable();
    long s1 = t.append(t.file(10, null));
    CompactSketch sketch = PuffinFixtures.sketch(Types.BooleanType.get(),
        Arrays.asList(true, false));
    addStats(t, s1, PuffinFixtures.blob(B_FIELD, s1, sketch).ndvProperty(null));
    register(t, null);
    setReadPuffin(false);
    long hmsNdv = loadNdv(t, "b");
    setReadPuffin(true);
    assertEquals(hmsNdv, loadNdv(t, "b"));
  }
}
