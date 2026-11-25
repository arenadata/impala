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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.datasketches.theta.CompactSketch;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.puffin.FileMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.impala.catalog.iceberg.FailingFileIO.Op;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Oracle: the file read back with Iceberg's Puffin reader.
 */
public class IcebergPuffinStatsWriterTest {
  private static final Schema SCHEMA = new Schema(
      optional(1, "a", Types.IntegerType.get()),
      optional(2, "b", Types.IntegerType.get()),
      optional(3, "c", Types.IntegerType.get()));
  private static final String THETA = StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1;
  private static final String OTHER_TYPE = "trino-custom-blob-v1";
  private static final String VERSION = "4.5.0-test";
  private static final String IMPALA = "Impala " + VERSION;
  private static final String TABLE_NAME = "db.t";

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  private final IcebergPuffinStatsWriter writer_ = new IcebergPuffinStatsWriter();
  private final WarningLog log_ = new WarningLog();
  private BackendConfig origConfig_;
  private IcebergTestTables tables_;
  private long s0_;
  private long s1_;

  @Before
  public void setUp() {
    origConfig_ = BackendConfig.INSTANCE;
    TBackendGflags cfg = new TBackendGflags();
    cfg.setImpala_build_version(VERSION);
    BackendConfig.create(cfg, false);
    Logger.getLogger(IcebergPuffinStatsWriter.class).addAppender(log_);
    tables_ = newTables("t", 2);
    s0_ = tables_.append(tables_.file(10, null));
    s1_ = tables_.append(tables_.file(20, null));
  }

  @After
  public void tearDown() {
    BackendConfig.INSTANCE = origConfig_;
    Logger.getLogger(IcebergPuffinStatsWriter.class).removeAppender(log_);
  }

  /** Collects the warnings of the writer. */
  private static final class WarningLog extends AppenderSkeleton {
    private final List<String> warnings_ = new CopyOnWriteArrayList<>();

    @Override
    protected void append(LoggingEvent event) {
      if (event.getLevel().equals(Level.WARN)) warnings_.add(event.getRenderedMessage());
    }

    @Override
    public void close() {}

    @Override
    public boolean requiresLayout() { return false; }
  }

  private IcebergTestTables newTables(String name, int formatVersion) {
    return IcebergTestTables.create(tmp, "db", name, SCHEMA,
        PartitionSpec.unpartitioned(), formatVersion);
  }

  private static CompactSketch sketch(Integer... values) {
    return PuffinFixtures.sketch(Types.IntegerType.get(), Arrays.asList(values));
  }

  /** Blob key that the writer keeps unique in a file: type, fields and snapshot. */
  private static List<Object> key(String type, int field, long snapshotId) {
    return ImmutableList.of(type, ImmutableList.of(field), snapshotId);
  }

  private static List<Object> key(org.apache.iceberg.puffin.BlobMetadata b) {
    return ImmutableList.of(b.type(), b.inputFields(), b.snapshotId());
  }

  /** Content of a Puffin file read with Iceberg's reader. */
  private static final class PuffinContent {
    private final FileMetadata footer_;
    private final Map<List<Object>, org.apache.iceberg.puffin.BlobMetadata> blobs_ =
        new LinkedHashMap<>();
    private final Map<List<Object>, byte[]> data_ = new HashMap<>();

    PuffinContent(Table table, String path) {
      try (PuffinReader reader = Puffin.read(table.io().newInputFile(path)).build()) {
        footer_ = reader.fileMetadata();
        for (Pair<org.apache.iceberg.puffin.BlobMetadata, ByteBuffer> p
            : reader.readAll(footer_.blobs())) {
          List<Object> k = key(p.first());
          assertNull("duplicate blob " + k, blobs_.put(k, p.first()));
          byte[] bytes = new byte[p.second().remaining()];
          p.second().duplicate().get(bytes);
          data_.put(k, bytes);
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  private Map<Integer, byte[]> sketches(Map<Integer, CompactSketch> sketches) {
    Map<Integer, byte[]> result = new HashMap<>();
    sketches.forEach((f, s) -> result.put(f, s.toByteArray()));
    return result;
  }

  private static File localFile(String path) {
    return new File(new Path(path).toUri().getPath());
  }

  /** Names of the Puffin files in the table's metadata directory. */
  private static Set<String> statsFiles(Table table) {
    File[] files = localFile(table.location() + "/metadata").listFiles(
        (dir, name) -> name.endsWith(".stats"));
    return Arrays.stream(files).map(File::getName).collect(Collectors.toSet());
  }

  private static Table withIo(Table table, FailingFileIO io) {
    return new BaseTable(new FailingCommitTables(((BaseTable) table).operations(), io,
        new FailingCommitTables.Rule()), table.name());
  }

  /**
   * Registers for s1 a file of another engine with a theta blob of column 1 that the
   * write replaces, a theta blob of column 3 computed for s0 and a blob of another
   * type over column 1.
   */
  private StatisticsFile registerForeignFile() {
    StatisticsFile file = PuffinFixtures.write(tables_.table(), s1_, ImmutableList.of(
        PuffinFixtures.blob(1, s1_, sketch(7, 8)),
        PuffinFixtures.blob(3, s0_, sketch(1, 2)).ndvProperty("2"),
        PuffinFixtures.blob(1, s1_, sketch(9)).type(OTHER_TYPE).ndvProperty(null)));
    tables_.setStats(s1_, file);
    return file;
  }

  /** File of s1 with Impala blobs of columns 1 and 2 and a foreign blob of column 3. */
  private StatisticsFile registerMixedFile() {
    StatisticsFile file = PuffinFixtures.write(tables_.table(), s1_, ImmutableList.of(
        PuffinFixtures.blob(1, s1_, sketch(1, 2, 3)).impalaCreatedBy(IMPALA),
        PuffinFixtures.blob(2, s1_, sketch(4)).impalaCreatedBy(IMPALA),
        PuffinFixtures.blob(3, s0_, sketch(5, 6))));
    tables_.setStats(s1_, file);
    return file;
  }

  private static Set<BlobMetadata> impalaBlobs(StatisticsFile file) {
    return file.blobMetadata().stream()
        .filter(b -> b.properties().containsKey(PuffinFixtures.IMPALA_CREATED_BY))
        .collect(Collectors.toSet());
  }

  private static void assertImpalaBlob(PuffinContent content, int field, long snapshotId,
      long sequenceNumber, CompactSketch sketch, long ndv) {
    List<Object> k = key(THETA, field, snapshotId);
    org.apache.iceberg.puffin.BlobMetadata blob = content.blobs_.get(k);
    assertNotNull("no blob " + k + " in " + content.blobs_.keySet(), blob);
    assertEquals(sequenceNumber, blob.sequenceNumber());
    assertNull("compressed blob " + k, blob.compressionCodec());
    assertEquals(ImmutableMap.of("ndv", Long.toString(ndv),
        PuffinFixtures.IMPALA_CREATED_BY, IMPALA), blob.properties());
    assertArrayEquals(sketch.toByteArray(), content.data_.get(k));
  }

  private static void assertCopied(PuffinContent source, PuffinContent copy,
      List<Object> k) {
    org.apache.iceberg.puffin.BlobMetadata from = source.blobs_.get(k);
    org.apache.iceberg.puffin.BlobMetadata to = copy.blobs_.get(k);
    assertNotNull("blob " + k + " not copied: " + copy.blobs_.keySet(), to);
    assertEquals(from.sequenceNumber(), to.sequenceNumber());
    assertEquals(from.properties(), to.properties());
    assertNull("compressed blob " + k, to.compressionCodec());
    assertArrayEquals(source.data_.get(k), copy.data_.get(k));
  }

  /** The returned file describes the file on disk; the table does not register it. */
  private static void assertWrittenFile(Table table, StatisticsFile result,
      long snapshotId, PuffinContent content) {
    assertEquals(snapshotId, result.snapshotId());
    assertTrue(result.path(), result.path().startsWith(
        table.location() + "/metadata/" + snapshotId + "-"));
    assertTrue(result.path(), result.path().endsWith(".stats"));
    assertEquals(localFile(result.path()).length(), result.fileSizeInBytes());
    assertTrue(result.fileFooterSizeInBytes() > 0);
    assertEquals(GenericBlobMetadata.from(content.footer_.blobs()),
        result.blobMetadata());
    assertEquals(IMPALA, content.footer_.properties().get("created-by"));
    for (StatisticsFile registered : table.statisticsFiles()) {
      assertFalse("registered " + result.path(),
          registered.path().equals(result.path()));
    }
  }

  @Test
  public void writeCreatesImpalaThetaBlobsOfSnapshot() throws Exception {
    Table table = tables_.table();
    CompactSketch a = sketch(1, 2, 3);
    CompactSketch b = sketch(4, 5);
    StatisticsFile result = writer_.write(table, TABLE_NAME, s1_,
        sketches(ImmutableMap.of(1, a, 2, b)), ImmutableMap.of(1, 3L, 2, 2L));

    PuffinContent content = new PuffinContent(table, result.path());
    assertEquals(2, content.blobs_.size());
    long seq = table.snapshot(s1_).sequenceNumber();
    assertTrue("sequence number of v2 snapshot", seq > 0);
    assertImpalaBlob(content, 1, s1_, seq, a, 3);
    assertImpalaBlob(content, 2, s1_, seq, b, 2);
    assertWrittenFile(tables_.table(), result, s1_, content);
    assertTrue(tables_.table().statisticsFiles().isEmpty());
  }

  @Test
  public void writeCopiesForeignBlobsOfRegisteredFile() throws Exception {
    StatisticsFile registered = registerForeignFile();
    Table table = tables_.table();
    PuffinContent before = new PuffinContent(table, registered.path());
    CompactSketch a = sketch(1, 2, 3);
    CompactSketch b = sketch(4);
    StatisticsFile result = writer_.write(table, TABLE_NAME, s1_,
        sketches(ImmutableMap.of(1, a, 2, b)), ImmutableMap.of(1, 3L, 2, 1L));

    PuffinContent content = new PuffinContent(table, result.path());
    assertEquals(content.blobs_.keySet().toString(), 4, content.blobs_.size());
    long seq = table.snapshot(s1_).sequenceNumber();
    assertImpalaBlob(content, 1, s1_, seq, a, 3);
    assertImpalaBlob(content, 2, s1_, seq, b, 1);
    assertCopied(before, content, key(THETA, 3, s0_));
    assertCopied(before, content, key(OTHER_TYPE, 1, s1_));
    assertWrittenFile(table, result, s1_, content);
    // The registered file stays as it was.
    assertEquals(before.blobs_.keySet(),
        new PuffinContent(table, registered.path()).blobs_.keySet());
    assertEquals(ImmutableList.of(registered), tables_.table().statisticsFiles());
  }

  @Test
  public void writeUsesMetadataLocationOfTableOperationsOnV1Table() throws Exception {
    IcebergTestTables v1 = newTables("v1", 1);
    long s1 = v1.append(v1.file(10, null));
    // Stands for write.metadata.path, which only catalog tables support.
    String metadataDir = new Path(tmp.newFolder("custom-metadata").toURI()).toString();
    Table base = v1.table();
    Table table = new BaseTable(new FailingCommitTables(((BaseTable) base).operations(),
        new FailingCommitTables.Rule()) {
      @Override
      public String metadataFileLocation(String fileName) {
        return metadataDir + "/" + fileName;
      }
    }, base.name());
    CompactSketch a = sketch(1);
    StatisticsFile result = writer_.write(table, TABLE_NAME, s1,
        sketches(ImmutableMap.of(1, a)), ImmutableMap.of(1, 1L));

    assertTrue(result.path(), result.path().startsWith(metadataDir + "/" + s1 + "-"));
    assertTrue(localFile(result.path()).isFile());
    PuffinContent content = new PuffinContent(table, result.path());
    assertEquals(1, content.blobs_.size());
    assertImpalaBlob(content, 1, s1, 0, a, 1);
  }

  @Test
  public void writeWithoutSketchesWritesOnlyBlobsToCopy() throws Exception {
    Table table = tables_.table();
    Set<String> before = statsFiles(table);
    assertNull(writer_.write(table, TABLE_NAME, s1_, ImmutableMap.of(),
        ImmutableMap.of()));
    assertEquals(before, statsFiles(table));

    StatisticsFile registered = registerForeignFile();
    table = tables_.table();
    PuffinContent source = new PuffinContent(table, registered.path());
    StatisticsFile result = writer_.write(table, TABLE_NAME, s1_, ImmutableMap.of(),
        ImmutableMap.of());
    assertNotNull(result);
    PuffinContent copy = new PuffinContent(table, result.path());
    assertEquals(source.blobs_.keySet(), copy.blobs_.keySet());
    for (List<Object> k : source.blobs_.keySet()) assertCopied(source, copy, k);
  }

  @Test
  public void writeOverMissingRegisteredFileKeepsOnlyImpalaBlobs() throws Exception {
    StatisticsFile registered = registerForeignFile();
    assertTrue(localFile(registered.path()).delete());
    Table table = tables_.table();
    CompactSketch a = sketch(1, 2);
    StatisticsFile result = writer_.write(table, TABLE_NAME, s1_,
        sketches(ImmutableMap.of(1, a)), ImmutableMap.of(1, 2L));

    PuffinContent content = new PuffinContent(table, result.path());
    assertEquals(content.blobs_.keySet().toString(), 1, content.blobs_.size());
    assertImpalaBlob(content, 1, s1_, table.snapshot(s1_).sequenceNumber(), a, 2);
    assertEquals(ImmutableList.of("Puffin statistics file " + registered.path()
        + " of table db.t is missing; lost blobs: " + THETA + " [1] " + s1_ + ", "
        + THETA + " [3] " + s0_ + ", " + OTHER_TYPE + " [1] " + s1_), log_.warnings_);
  }

  @Test
  public void writeFailsOnUnreadableRegisteredFile() throws Exception {
    StatisticsFile registered = registerForeignFile();
    FailingFileIO io = new FailingFileIO(tables_.table().io())
        .failPath(Op.READ, p -> p.equals(registered.path()));
    Table table = withIo(tables_.table(), io);
    Set<String> before = statsFiles(table);
    try {
      writer_.write(table, TABLE_NAME, s1_, sketches(ImmutableMap.of(1, sketch(1))),
          ImmutableMap.of(1, 1L));
      fail("write over unreadable file succeeded");
    } catch (ImpalaRuntimeException e) {
      assertEquals("Failed to read Puffin statistics file " + registered.path()
          + " of table db.t: Injected read failure: " + registered.path(),
          e.getMessage());
    }
    assertEquals(before, statsFiles(table));
    assertEquals(0, io.calls(Op.WRITE));
  }

  @Test
  public void writeFailsOnCorruptRegisteredFile() throws Exception {
    StatisticsFile registered = registerForeignFile();
    try (RandomAccessFile f = new RandomAccessFile(localFile(registered.path()), "rw")) {
      f.seek(f.length() - 4);
      f.write(new byte[] {'J', 'U', 'N', 'K'});
    }
    Table table = tables_.table();
    Set<String> before = statsFiles(table);
    try {
      writer_.write(table, TABLE_NAME, s1_, sketches(ImmutableMap.of(1, sketch(1))),
          ImmutableMap.of(1, 1L));
      fail("write over corrupt file succeeded");
    } catch (ImpalaRuntimeException e) {
      assertTrue(e.getMessage(), e.getMessage().startsWith(
          "Failed to read Puffin statistics file " + registered.path()
          + " of table db.t: "));
    }
    assertEquals(before, statsFiles(table));
  }

  @Test
  public void writeFailureDeletesPartialFile() throws Exception {
    FailingFileIO io = new FailingFileIO(tables_.table().io())
        .failPath(Op.WRITE, p -> p.endsWith(".stats"));
    Table table = withIo(tables_.table(), io);
    Set<String> before = statsFiles(table);
    try {
      writer_.write(table, TABLE_NAME, s1_, sketches(ImmutableMap.of(1, sketch(1))),
          ImmutableMap.of(1, 1L));
      fail("failing write succeeded");
    } catch (ImpalaRuntimeException e) {
      Matcher m = Pattern.compile(
          "Failed to write Puffin statistics file (\\S+): Injected write failure: (\\S+)")
          .matcher(e.getMessage());
      assertTrue(e.getMessage(), m.matches());
      assertEquals(m.group(1), m.group(2));
      assertFalse("partial file kept: " + m.group(1), localFile(m.group(1)).exists());
    }
    assertEquals(1, io.calls(Op.WRITE));
    assertEquals(before, statsFiles(table));
  }

  @Test
  public void rewriteWithoutReturnsEmptyWhenNoBlobIsLeft() throws Exception {
    StatisticsFile file = PuffinFixtures.write(tables_.table(), s1_, ImmutableList.of(
        PuffinFixtures.blob(1, s1_, sketch(1)).impalaCreatedBy(IMPALA),
        PuffinFixtures.blob(2, s1_, sketch(2)).impalaCreatedBy(IMPALA)));
    tables_.setStats(s1_, file);
    Table table = tables_.table();
    Set<String> before = statsFiles(table);
    assertEquals(Optional.empty(),
        writer_.rewriteWithout(table, TABLE_NAME, file, impalaBlobs(file)));
    assertEquals(before, statsFiles(table));

    // Dropping a part of the blobs keeps the rest.
    PuffinContent source = new PuffinContent(table, file.path());
    Set<BlobMetadata> first = ImmutableSet.of(file.blobMetadata().get(0));
    Optional<StatisticsFile> result =
        writer_.rewriteWithout(table, TABLE_NAME, file, first);
    assertTrue(result.isPresent());
    PuffinContent copy = new PuffinContent(table, result.get().path());
    assertEquals(ImmutableSet.of(key(THETA, 2, s1_)), copy.blobs_.keySet());
    assertCopied(source, copy, key(THETA, 2, s1_));
  }

  @Test
  public void rewriteWithoutCopiesForeignBlobsOfMixedFile() throws Exception {
    StatisticsFile mixed = registerMixedFile();
    Table table = tables_.table();
    byte[] sourceBytes = Files.readAllBytes(localFile(mixed.path()).toPath());
    PuffinContent source = new PuffinContent(table, mixed.path());

    Optional<StatisticsFile> result =
        writer_.rewriteWithout(table, TABLE_NAME, mixed, impalaBlobs(mixed));

    assertTrue(result.isPresent());
    PuffinContent copy = new PuffinContent(table, result.get().path());
    assertEquals(ImmutableSet.of(key(THETA, 3, s0_)), copy.blobs_.keySet());
    assertCopied(source, copy, key(THETA, 3, s0_));
    assertWrittenFile(table, result.get(), s1_, copy);
    assertArrayEquals("source file changed", sourceBytes,
        Files.readAllBytes(localFile(mixed.path()).toPath()));
    assertEquals(ImmutableList.of(mixed), tables_.table().statisticsFiles());
  }

  @Test
  public void rewriteWithoutKeepsForeignBlobWithKeyOfImpalaBlob() throws Exception {
    CompactSketch foreign = sketch(5, 6);
    StatisticsFile mixed = PuffinFixtures.write(tables_.table(), s1_, ImmutableList.of(
        PuffinFixtures.blob(1, s1_, sketch(1, 2, 3)).impalaCreatedBy(IMPALA),
        PuffinFixtures.blob(1, s1_, foreign)));
    tables_.setStats(s1_, mixed);
    Table table = tables_.table();

    Optional<StatisticsFile> result =
        writer_.rewriteWithout(table, TABLE_NAME, mixed, impalaBlobs(mixed));

    assertTrue(result.isPresent());
    PuffinContent copy = new PuffinContent(table, result.get().path());
    List<Object> k = key(THETA, 1, s1_);
    assertEquals(ImmutableSet.of(k), copy.blobs_.keySet());
    assertArrayEquals(foreign.toByteArray(), copy.data_.get(k));
    assertFalse(copy.blobs_.get(k).properties()
        .containsKey(PuffinFixtures.IMPALA_CREATED_BY));
  }

  @Test
  public void rewriteWithoutMissingFileReturnsEmpty() throws Exception {
    StatisticsFile mixed = registerMixedFile();
    assertTrue(localFile(mixed.path()).delete());
    Table table = tables_.table();
    Set<String> before = statsFiles(table);
    assertEquals(Optional.empty(),
        writer_.rewriteWithout(table, TABLE_NAME, mixed, impalaBlobs(mixed)));
    assertEquals(before, statsFiles(table));
    assertEquals(ImmutableList.of("Puffin statistics file " + mixed.path()
        + " of table db.t is missing; lost blobs: " + THETA + " [1] " + s1_ + ", "
        + THETA + " [2] " + s1_ + ", " + THETA + " [3] " + s0_), log_.warnings_);
  }

  @Test
  public void rewriteWithoutFailsOnUnreadableFile() throws Exception {
    StatisticsFile mixed = registerMixedFile();
    FailingFileIO io = new FailingFileIO(tables_.table().io())
        .failPath(Op.READ, p -> p.equals(mixed.path()));
    Table table = withIo(tables_.table(), io);
    Set<String> before = statsFiles(table);
    try {
      writer_.rewriteWithout(table, TABLE_NAME, mixed, impalaBlobs(mixed));
      fail("rewrite of unreadable file succeeded");
    } catch (ImpalaRuntimeException e) {
      assertEquals("Failed to read Puffin statistics file " + mixed.path()
          + " of table db.t: Injected read failure: " + mixed.path(), e.getMessage());
    }
    assertEquals(before, statsFiles(table));
    assertEquals(0, io.calls(Op.WRITE));
  }

  @Test
  public void deleteQuietlyKeepsFileOnFailure() throws Exception {
    StatisticsFile file = registerMixedFile();
    FailingFileIO io = new FailingFileIO(tables_.table().io()).failCall(Op.DELETE, 1);
    Table table = withIo(tables_.table(), io);
    writer_.deleteQuietly(table, file.path());
    assertTrue(localFile(file.path()).exists());
    writer_.deleteQuietly(table, file.path());
    assertFalse(localFile(file.path()).exists());
    assertEquals(2, io.calls(Op.DELETE));
  }
}
