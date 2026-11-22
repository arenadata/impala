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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.Pair;

/**
 * Puffin files with theta blobs built the way Iceberg engines build them: the sketch is
 * fed with the Iceberg single-value serialization of each value.
 */
public class PuffinFixtures {
  /** Blob property that marks blobs computed by Impala. */
  public static final String IMPALA_CREATED_BY = "impala.created-by";
  private static final int NOMINAL_ENTRIES = 4096;
  private static final long SEED = 9001;

  private PuffinFixtures() {}

  public static CompactSketch sketch(Type type, List<?> values) {
    UpdateSketch sketch = UpdateSketch.builder()
        .setNominalEntries(NOMINAL_ENTRIES)
        .setSeed(SEED)
        .build();
    for (Object v : values) {
      if (v == null) continue;
      sketch.update(ByteBuffers.toByteArray(Conversions.toByteBuffer(type, v)));
    }
    return sketch.compact(true, null);
  }

  public static BlobSpec blob(int fieldId, long snapshotId, CompactSketch sketch) {
    return new BlobSpec(fieldId, snapshotId, sketch.toByteArray(),
        Long.toString(Math.round(sketch.getEstimate())));
  }

  /**
   * Theta blob whose payload is 'payload' as is, e.g. a sketch serialized by the C++
   * library. The blob has no "ndv" property unless ndvProperty() sets one.
   */
  public static BlobSpec blob(int fieldId, long snapshotId, byte[] payload) {
    return new BlobSpec(fieldId, snapshotId, payload.clone(), null);
  }

  /**
   * Bytes of the one-value sketch of 'values' as the C++ library writes a single-item
   * sketch compacted without ordering: 16 bytes, flags READ_ONLY | COMPACT only.
   */
  public static byte[] cppUnorderedSingleItem(Type type, List<?> values) {
    byte[] bytes = sketch(type, values).toByteArray();
    Preconditions.checkArgument(bytes.length == 16, "not a single-item sketch");
    bytes[5] = 0x0a;
    return bytes;
  }

  /**
   * Writes the blobs to a new Puffin file under the table's metadata directory. The
   * file is not registered in the table.
   */
  public static StatisticsFile write(Table table, long snapshotId, List<BlobSpec> blobs) {
    Preconditions.checkArgument(!blobs.isEmpty(), "no blobs");
    String path = table.location() + "/metadata/" + UUID.randomUUID() + ".stats";
    // 'created-by' is a file property in Puffin, so it lists every blob author.
    Set<String> authors = new LinkedHashSet<>();
    for (BlobSpec b : blobs) authors.add(b.createdBy_);
    PuffinWriter writer = Puffin.write(table.io().newOutputFile(path))
        .createdBy(String.join(", ", authors))
        .build();
    try {
      try {
        for (BlobSpec b : blobs) writer.add(b.toBlob(table));
      } finally {
        writer.close();
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return new GenericStatisticsFile(snapshotId, path, writer.fileSize(),
        writer.footerSize(), GenericBlobMetadata.from(writer.writtenBlobsMetadata()));
  }

  /** Payloads of the Impala theta blobs in 'file' of 'table', by field id. */
  public static Map<Integer, byte[]> impalaThetaPayloads(Table table,
      StatisticsFile file) {
    Map<Integer, byte[]> res = new TreeMap<>();
    try (PuffinReader reader = Puffin.read(table.io().newInputFile(file.path()))
        .withFileSize(file.fileSizeInBytes())
        .build()) {
      for (Pair<BlobMetadata, ByteBuffer> b
          : reader.readAll(reader.fileMetadata().blobs())) {
        if (!b.first().properties().containsKey(IMPALA_CREATED_BY)) continue;
        Preconditions.checkState(b.first().inputFields().size() == 1,
            "Impala blob of fields %s", b.first().inputFields());
        Preconditions.checkState(
            res.put(b.first().inputFields().get(0), ByteBuffers.toByteArray(b.second()))
                == null,
            "two Impala blobs of field %s", b.first().inputFields().get(0));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return res;
  }

  /** Payload of the only blob of 'type' in 'file' of 'table'. */
  public static byte[] payloadOfType(Table table, StatisticsFile file, String type) {
    byte[] res = null;
    try (PuffinReader reader = Puffin.read(table.io().newInputFile(file.path()))
        .withFileSize(file.fileSizeInBytes())
        .build()) {
      for (Pair<BlobMetadata, ByteBuffer> b
          : reader.readAll(reader.fileMetadata().blobs())) {
        if (!b.first().type().equals(type)) continue;
        Preconditions.checkState(res == null, "two blobs of type %s", type);
        res = ByteBuffers.toByteArray(b.second());
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    Preconditions.checkState(res != null, "no blob of type %s", type);
    return res;
  }

  /** Theta blob of one column; by default authored by another engine. */
  public static class BlobSpec {
    private final int fieldId_;
    private final long snapshotId_;
    private final byte[] payload_;
    private String type_ = StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1;
    private String createdBy_ = "trino";
    /** Value of the blob's "ndv" property; null when the blob has none. */
    private String ndv_;
    /** Value of the blob's IMPALA_CREATED_BY property; null when the blob has none. */
    private String impalaCreatedBy_;
    private final Map<String, String> properties_ = new HashMap<>();

    private BlobSpec(int fieldId, long snapshotId, byte[] payload, String ndv) {
      fieldId_ = fieldId;
      snapshotId_ = snapshotId;
      payload_ = payload;
      ndv_ = ndv;
    }

    public BlobSpec createdBy(String createdBy) {
      createdBy_ = Preconditions.checkNotNull(createdBy);
      return this;
    }

    /**
     * Makes this an Impala blob: sets the IMPALA_CREATED_BY blob property and the file
     * author to 'createdBy', e.g. "Impala 4.5.0".
     */
    public BlobSpec impalaCreatedBy(String createdBy) {
      impalaCreatedBy_ = Preconditions.checkNotNull(createdBy);
      createdBy_ = createdBy;
      return this;
    }

    /** Blob type other than theta; the payload stays the same. */
    public BlobSpec type(String type) {
      type_ = Preconditions.checkNotNull(type);
      return this;
    }

    /** Any string, including a non-number; null drops the property. */
    public BlobSpec ndvProperty(String ndv) {
      ndv_ = ndv;
      return this;
    }

    /** Blob property other than "ndv" and IMPALA_CREATED_BY. */
    public BlobSpec property(String key, String value) {
      Preconditions.checkArgument(!key.equals("ndv") && !key.equals(IMPALA_CREATED_BY),
          "%s has its own setter", key);
      properties_.put(key, Preconditions.checkNotNull(value));
      return this;
    }

    private Blob toBlob(Table table) {
      Map<String, String> props = new HashMap<>(properties_);
      if (ndv_ != null) props.put("ndv", ndv_);
      if (impalaCreatedBy_ != null) props.put(IMPALA_CREATED_BY, impalaCreatedBy_);
      Snapshot snapshot = table.snapshot(snapshotId_);
      long sequenceNumber = snapshot == null ? 0 : snapshot.sequenceNumber();
      return new Blob(type_, ImmutableList.of(fieldId_), snapshotId_, sequenceNumber,
          ByteBuffer.wrap(payload_), null, props);
    }
  }
}
