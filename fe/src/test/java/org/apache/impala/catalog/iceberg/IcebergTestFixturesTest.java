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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.impala.common.FileSystemUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class IcebergTestFixturesTest {
  private static final Schema SCHEMA = new Schema(
      required(1, "id", Types.IntegerType.get()),
      optional(2, "s", Types.StringType.get()));
  private static final String IMPALA = "Impala 4.5.0";

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  private static Set<String> dataFilePaths(Table table) throws Exception {
    Set<String> paths = new HashSet<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) paths.add(task.file().location());
    }
    return paths;
  }

  private static Set<String> paths(Iterable<? extends ContentFile<?>> files) {
    Set<String> paths = new HashSet<>();
    for (ContentFile<?> f : files) paths.add(f.location());
    return paths;
  }

  private static void assertChange(Table table, long snapshotId, String operation,
      List<DataFile> added, List<DataFile> removed) {
    Snapshot s = table.snapshot(snapshotId);
    assertEquals(operation, s.operation());
    assertEquals(paths(added), paths(s.addedDataFiles(table.io())));
    assertEquals(paths(removed), paths(s.removedDataFiles(table.io())));
  }

  private static void assertRejected(Runnable call) {
    try {
      call.run();
      fail("IllegalArgumentException expected");
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void appendsToTableOfAnyCatalogMakeDistinctFiles() throws Exception {
    String location = new Path(tmp.newFolder("plain").toURI()).toString();
    Table table = new HadoopTables(FileSystemUtil.getConfiguration())
        .create(SCHEMA, PartitionSpec.unpartitioned(), new HashMap<>(), location);
    DataFile f1 = IcebergTestTables.placeholderFile(table, 10, null, FileFormat.PARQUET);
    long s1 = IcebergTestTables.append(table, f1);
    DataFile f2 = IcebergTestTables.placeholderFile(table, 20, null, FileFormat.PARQUET);
    long s2 = IcebergTestTables.append(table, f2);

    assertNotEquals(s1, s2);
    assertEquals(s2, table.currentSnapshot().snapshotId());
    assertEquals(2, Iterables.size(table.snapshots()));
    assertNotEquals(f1.location(), f2.location());
    assertEquals(new HashSet<>(Arrays.asList(f1.location(), f2.location())),
        dataFilePaths(table));
    for (DataFile f : ImmutableList.of(f1, f2)) {
      assertTrue(f.location(), f.location().startsWith(table.location() + "/data/"));
      File local = new File(new Path(f.location()).toUri().getPath());
      assertEquals(f.location(), f.fileSizeInBytes(), local.length());
    }
  }

  @Test
  public void tableOfFixtureTakesManyFilesAndSnapshots() throws Exception {
    IcebergTestTables t = IcebergTestTables.create(tmp, "db", "t", SCHEMA,
        PartitionSpec.unpartitioned(), 2);
    long s1 = t.append(t.file(10, null), t.file(10, null));
    long s2 = t.append(t.file(10, null));
    assertNotEquals(s1, s2);
    assertEquals(2, Iterables.size(t.table().snapshots()));
    assertEquals(3, dataFilePaths(t.table()).size());
  }

  @Test
  public void impalaBlobCarriesImpalaCreatedBy() throws Exception {
    IcebergTestTables t = IcebergTestTables.create(tmp, "db", "t", SCHEMA,
        PartitionSpec.unpartitioned(), 2);
    long s1 = t.append(t.file(10, null));
    StatisticsFile stats = PuffinFixtures.write(t.table(), s1, ImmutableList.of(
        PuffinFixtures.blob(1, s1, PuffinFixtures.sketch(Types.IntegerType.get(),
            ImmutableList.of(1, 2, 3))).impalaCreatedBy(IMPALA),
        PuffinFixtures.blob(2, s1, PuffinFixtures.sketch(Types.StringType.get(),
            ImmutableList.of("a")))));

    Map<Integer, Map<String, String>> byField = new HashMap<>();
    for (BlobMetadata b : stats.blobMetadata()) {
      byField.put(b.fields().get(0), b.properties());
    }
    assertEquals(IMPALA, byField.get(1).get(PuffinFixtures.IMPALA_CREATED_BY));
    assertEquals("3", byField.get(1).get("ndv"));
    assertFalse(byField.get(2).containsKey(PuffinFixtures.IMPALA_CREATED_BY));
    assertEquals("1", byField.get(2).get("ndv"));

    try (PuffinReader reader = Puffin.read(t.table().io().newInputFile(stats.path()))
        .build()) {
      List<org.apache.iceberg.puffin.BlobMetadata> blobs =
          reader.fileMetadata().blobs();
      assertEquals(2, blobs.size());
      for (org.apache.iceberg.puffin.BlobMetadata b : blobs) {
        String expected = b.inputFields().get(0) == 1 ? IMPALA : null;
        assertEquals(expected, b.properties().get(PuffinFixtures.IMPALA_CREATED_BY));
      }
      assertEquals(IMPALA + ", trino",
          reader.fileMetadata().properties().get("created-by"));
    }
  }

  @Test
  public void branchSnapshotsForkFromMainAndStayOffIt() throws Exception {
    IcebergTestTables t = IcebergTestTables.create(tmp, "db", "t", SCHEMA,
        PartitionSpec.unpartitioned(), 2);
    long s1 = t.append(t.file(10, null));
    DataFile onBranch = t.file(20, null);
    long b1 = t.appendToBranch("b", onBranch);
    long b2 = t.appendToBranch("b", t.file(20, null));
    long s2 = t.append(t.file(30, null));

    Table table = t.table();
    assertEquals(s2, table.currentSnapshot().snapshotId());
    assertEquals(b2, table.snapshot("b").snapshotId());
    assertEquals(Long.valueOf(s1), table.snapshot(b1).parentId());
    assertEquals(Long.valueOf(b1), table.snapshot(b2).parentId());
    assertEquals(Long.valueOf(s1), table.snapshot(s2).parentId());
    assertFalse(SnapshotUtil.isAncestorOf(table, s2, b1));
    assertFalse(dataFilePaths(table).contains(onBranch.location()));
    assertTrue(
        table.snapshot(s2).timestampMillis() > table.snapshot(b2).timestampMillis());
    assertChange(table, b1, DataOperations.APPEND, ImmutableList.of(onBranch),
        ImmutableList.of());
  }

  @Test
  public void snapshotAfterRollbackDoesNotDescendFromDroppedOne() {
    IcebergTestTables t = IcebergTestTables.create(tmp, "db", "t", SCHEMA,
        PartitionSpec.unpartitioned(), 2);
    long s1 = t.append(t.file(10, null));
    long s2 = t.append(t.file(10, null));
    t.rollbackTo(s1);
    assertEquals(s1, t.table().currentSnapshot().snapshotId());
    long s3 = t.append(t.file(10, null));

    Table table = t.table();
    assertNotNull(table.snapshot(s2));
    assertEquals(Long.valueOf(s1), table.snapshot(s3).parentId());
    assertFalse(SnapshotUtil.isAncestorOf(table, s3, s2));
  }

  @Test
  public void replaceOverwriteAndDeleteChangeLiveFiles() throws Exception {
    IcebergTestTables t = IcebergTestTables.create(tmp, "db", "t", SCHEMA,
        PartitionSpec.unpartitioned(), 2);
    DataFile a = t.file(10, null);
    DataFile b = t.file(10, null);
    DataFile c = t.file(10, null);
    t.append(a, b, c);
    DataFile compacted = t.file(15, null);
    long r = t.replace(ImmutableList.of(a), ImmutableList.of(compacted));
    DataFile rewritten = t.file(25, null);
    long o = t.overwrite(ImmutableList.of(b), ImmutableList.of(rewritten));
    long d = t.delete(c);

    Table table = t.table();
    assertChange(table, r, DataOperations.REPLACE, ImmutableList.of(compacted),
        ImmutableList.of(a));
    assertChange(table, o, DataOperations.OVERWRITE, ImmutableList.of(rewritten),
        ImmutableList.of(b));
    assertChange(table, d, DataOperations.DELETE, ImmutableList.of(),
        ImmutableList.of(c));
    assertEquals(paths(ImmutableList.of(compacted, rewritten)), dataFilePaths(table));
  }

  @Test
  public void deleteSnapshotsCarryDeclaredSummaryCounters() {
    IcebergTestTables t = IcebergTestTables.create(tmp, "db", "t", SCHEMA,
        PartitionSpec.unpartitioned(), 2);
    DataFile f = t.file(10, null);
    long s1 = t.append(f);
    DeleteFile pos = t.positionDeletes(5, 3, null);
    DeleteFile eq = t.equalityDeletes(6, 2, null, 1);
    long rd = t.rowDelta(pos, eq);
    Map<String, String> counters = ImmutableMap.<String, String>builder()
        .put("deleted-records", "7")
        .put("added-position-deletes", "3")
        .put("removed-position-deletes", "1")
        .put("added-equality-deletes", "4")
        .put("removed-equality-deletes", "0")
        .put("added-delete-files", "2")
        .put("deleted-data-files", "0")
        .build();
    t.setSummary(rd, counters);
    long d = t.delete(f);
    t.setSummary(d, ImmutableMap.of("deleted-records", "not a number"));

    Table table = t.table();
    Snapshot delta = table.snapshot(rd);
    assertEquals(DataOperations.DELETE, delta.operation());
    for (Map.Entry<String, String> e : counters.entrySet()) {
      assertEquals(e.getKey(), e.getValue(), delta.summary().get(e.getKey()));
    }
    assertTrue(delta.summary().containsKey("total-records"));
    Map<String, FileContent> deletes = new HashMap<>();
    for (DeleteFile df : delta.addedDeleteFiles(table.io())) {
      deletes.put(df.location(), df.content());
    }
    assertEquals(ImmutableMap.of(pos.location(), FileContent.POSITION_DELETES,
        eq.location(), FileContent.EQUALITY_DELETES), deletes);
    assertEquals(3, pos.recordCount());
    assertEquals(ImmutableList.of(1), eq.equalityFieldIds());
    assertChange(table, d, DataOperations.DELETE, ImmutableList.of(),
        ImmutableList.of(f));
    assertEquals("not a number", table.snapshot(d).summary().get("deleted-records"));
    assertEquals("1", table.snapshot(s1).summary().get("added-data-files"));
  }

  @Test
  public void metadataEditsDropManifestListOperationAndTotalRecords() throws Exception {
    IcebergTestTables t = IcebergTestTables.create(tmp, "db", "t", SCHEMA,
        PartitionSpec.unpartitioned(), 1);
    DataFile f1 = t.file(10, null);
    long s1 = t.append(f1);
    long s2 = t.append(t.file(10, null));
    long s3 = t.append(t.file(10, null));
    long s4 = t.append(t.file(10, null));
    t.removeManifestList(s1);
    t.removeSummary(s2);
    t.removeFromSummary(s3, "total-records");
    assertRejected(() -> t.removeFromSummary(s4, "operation"));

    Table table = t.table();
    Snapshot old = table.snapshot(s1);
    assertNull(old.manifestListLocation());
    assertEquals(DataOperations.APPEND, old.operation());
    assertEquals(paths(ImmutableList.of(f1)), paths(old.addedDataFiles(table.io())));
    Snapshot noSummary = table.snapshot(s2);
    assertNull(noSummary.operation());
    assertNull(noSummary.summary());
    assertNotNull(noSummary.manifestListLocation());
    Snapshot noTotal = table.snapshot(s3);
    assertEquals(DataOperations.APPEND, noTotal.operation());
    assertFalse(noTotal.summary().containsKey("total-records"));
    assertEquals("1", noTotal.summary().get("added-data-files"));
    assertEquals(DataOperations.APPEND, table.snapshot(s4).operation());
    assertEquals("4", table.snapshot(s4).summary().get("total-records"));
    assertEquals(4, dataFilePaths(table).size());
  }

  @Test
  public void declaredColumnValuesSetRecordCount() {
    IcebergTestTables t = IcebergTestTables.create(tmp, "db", "t", SCHEMA,
        PartitionSpec.unpartitioned(), 2);
    DataFile f = t.file(100, null, ImmutableMap.of(
        "id", Arrays.asList(1, 2, 3), "s", Arrays.asList("a", null, "b")));
    DataFile idOnly = t.file(100, null, ImmutableMap.of("id", Arrays.asList(4, 5)));
    long s1 = t.append(f, idOnly);

    assertEquals(3, f.recordCount());
    assertEquals(2, idOnly.recordCount());
    assertEquals("5", t.table().snapshot(s1).summary().get("total-records"));
    assertEquals(Arrays.asList(1, 2, 3), t.columnValues(f.location(), "id"));
    assertEquals(Arrays.asList("a", null, "b"), t.columnValues(f.location(), "s"));
    assertEquals(Arrays.asList(null, null), t.columnValues(idOnly.location(), "s"));
    assertRejected(() -> t.columnValues(t.file(10, null).location(), "id"));
    assertRejected(() -> t.columnValues(f.location(), "missing"));
    assertRejected(() -> t.file(10, null,
        ImmutableMap.of("id", Arrays.asList(1), "s", Arrays.asList("a", "b"))));
    assertRejected(() -> t.file(10, null, ImmutableMap.of("missing", Arrays.asList(1))));
  }

  @Test
  public void blobFromBytesKeepsPayloadAndProperties() throws Exception {
    IcebergTestTables t = IcebergTestTables.create(tmp, "db", "t", SCHEMA,
        PartitionSpec.unpartitioned(), 2);
    long s1 = t.append(t.file(10, null));
    byte[] payload = {1, 3, 3, 0, 0, 58, (byte) 204, (byte) 147};
    StatisticsFile stats = PuffinFixtures.write(t.table(), s1, ImmutableList.of(
        PuffinFixtures.blob(1, s1, payload).ndvProperty("5").property("k", "v"),
        PuffinFixtures.blob(2, s1, new byte[0]).impalaCreatedBy(IMPALA)));

    try (PuffinReader reader = Puffin.read(t.table().io().newInputFile(stats.path()))
        .build()) {
      Map<Integer, byte[]> payloads = new HashMap<>();
      Map<Integer, Map<String, String>> props = new HashMap<>();
      for (Pair<org.apache.iceberg.puffin.BlobMetadata, ByteBuffer> b :
          reader.readAll(reader.fileMetadata().blobs())) {
        int field = b.first().inputFields().get(0);
        assertEquals(s1, b.first().snapshotId());
        assertEquals("apache-datasketches-theta-v1", b.first().type());
        payloads.put(field, ByteBuffers.toByteArray(b.second()));
        props.put(field, b.first().properties());
      }
      assertTrue(Arrays.equals(payload, payloads.get(1)));
      assertEquals(0, payloads.get(2).length);
      assertEquals(ImmutableMap.of("ndv", "5", "k", "v"), props.get(1));
      assertEquals(ImmutableMap.of(PuffinFixtures.IMPALA_CREATED_BY, IMPALA),
          props.get(2));
      assertEquals("trino, " + IMPALA,
          reader.fileMetadata().properties().get("created-by"));
    }
  }
}
