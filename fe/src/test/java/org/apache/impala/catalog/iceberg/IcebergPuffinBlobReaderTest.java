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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.datasketches.theta.CompactSketch;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.types.Types;
import org.apache.impala.catalog.iceberg.FailingFileIO.Op;
import org.apache.impala.common.ImpalaRuntimeException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Oracle: files written with PuffinFixtures, independently of the Impala writer.
 */
public class IcebergPuffinBlobReaderTest {
  private static final Schema SCHEMA = new Schema(
      optional(1, "a", Types.IntegerType.get()),
      optional(2, "b", Types.IntegerType.get()),
      optional(3, "c", Types.IntegerType.get()));
  private static final String OTHER_TYPE = "trino-custom-blob-v1";

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  private IcebergTestTables tables_;
  private long s0_;
  private long s1_;

  @Before
  public void setUp() {
    tables_ = IcebergTestTables.create(tmp, "db", "t", SCHEMA,
        PartitionSpec.unpartitioned(), 2);
    s0_ = tables_.append(tables_.file(10, null));
    s1_ = tables_.append(tables_.file(20, null));
  }

  private static CompactSketch sketch(Integer... values) {
    return PuffinFixtures.sketch(Types.IntegerType.get(), Arrays.asList(values));
  }

  private static File localFile(String path) {
    return new File(new Path(path).toUri().getPath());
  }

  private static byte[] toBytes(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.duplicate().get(bytes);
    return bytes;
  }

  @Test
  public void readThetaBlobsSelectsBlobOfFieldAndSnapshot() throws Exception {
    CompactSketch a1 = sketch(1, 2);
    CompactSketch a0 = sketch(3);
    CompactSketch c0 = sketch(4, 5, 6);
    StatisticsFile file = PuffinFixtures.write(tables_.table(), s1_, ImmutableList.of(
        PuffinFixtures.blob(1, s1_, a1),
        PuffinFixtures.blob(1, s0_, a0),
        PuffinFixtures.blob(2, s1_, sketch(7)).type(OTHER_TYPE),
        PuffinFixtures.blob(3, s0_, c0)));
    Map<Integer, Long> wanted = new HashMap<>();
    wanted.put(1, s0_);
    wanted.put(2, s1_);
    wanted.put(3, s0_);
    Map<Integer, ByteBuffer> result =
        IcebergPuffinBlobReader.readThetaBlobs(tables_.table().io(), file.path(), wanted);

    assertEquals(ImmutableSet.of(1, 3), result.keySet());
    assertArrayEquals(a0.toByteArray(), toBytes(result.get(1)));
    assertArrayEquals(c0.toByteArray(), toBytes(result.get(3)));
  }

  @Test
  public void readThetaBlobsRejectsSeveralBlobsOfField() throws Exception {
    StatisticsFile file = PuffinFixtures.write(tables_.table(), s1_, ImmutableList.of(
        PuffinFixtures.blob(1, s1_, sketch(1)),
        PuffinFixtures.blob(1, s1_, sketch(2))));
    try {
      IcebergPuffinBlobReader.readThetaBlobs(tables_.table().io(), file.path(),
          ImmutableMap.of(1, s1_));
      fail("ambiguous blobs accepted");
    } catch (ImpalaRuntimeException e) {
      assertEquals("Puffin statistics file " + file.path()
          + " has several theta blobs for field id 1 and snapshot " + s1_,
          e.getMessage());
    }
  }

  @Test
  public void readThetaBlobsReportsUnreadableFile() throws Exception {
    StatisticsFile file = PuffinFixtures.write(tables_.table(), s1_, ImmutableList.of(
        PuffinFixtures.blob(1, s1_, sketch(1))));
    FailingFileIO io = new FailingFileIO(tables_.table().io())
        .failPath(Op.READ, p -> p.equals(file.path()));
    try {
      IcebergPuffinBlobReader.readThetaBlobs(io, file.path(), ImmutableMap.of(1, s1_));
      fail("unreadable file accepted");
    } catch (ImpalaRuntimeException e) {
      assertEquals("Failed to read Puffin statistics file " + file.path()
          + ": Injected read failure: " + file.path(), e.getMessage());
    }
    Files.write(localFile(file.path()).toPath(), "not a puffin file".getBytes());
    try {
      IcebergPuffinBlobReader.readThetaBlobs(tables_.table().io(), file.path(),
          ImmutableMap.of(1, s1_));
      fail("non-Puffin file accepted");
    } catch (ImpalaRuntimeException e) {
      assertTrue(e.getMessage(), e.getMessage().startsWith(
          "Failed to read Puffin statistics file " + file.path() + ": "));
    }
  }

  @Test
  public void readThetaBlobsReportsMissingFile() throws Exception {
    StatisticsFile file = PuffinFixtures.write(tables_.table(), s1_, ImmutableList.of(
        PuffinFixtures.blob(1, s1_, sketch(1))));
    assertTrue(localFile(file.path()).delete());
    try {
      IcebergPuffinBlobReader.readThetaBlobs(tables_.table().io(), file.path(),
          ImmutableMap.of(1, s1_));
      fail("missing file accepted");
    } catch (ImpalaRuntimeException e) {
      assertEquals("Failed to read Puffin statistics file " + file.path()
          + ": File does not exist: " + file.path(), e.getMessage());
    }
  }
}
