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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.service.BackendConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates the Puffin statistics files of Impala. The files are not registered in the
 * table metadata: the caller registers them in its transaction and calls deleteQuietly()
 * if that fails. Registered files are never modified or deleted.
 *
 * A file registered for a snapshot is read only after io.newInputFile(path).exists():
 * on S3 reading a missing file with a known length throws a raw NoSuchKeyException
 * rather than NotFoundException. A missing file means its blobs are already lost, so
 * they are logged and not copied; a file that exists but cannot be read fails the call
 * before anything is written, so blobs of other engines are never dropped silently.
 */
public class IcebergPuffinStatsWriter {
  private static final Logger LOG =
      LoggerFactory.getLogger(IcebergPuffinStatsWriter.class);

  /** Blob property that marks blobs written by Impala. */
  public static final String IMPALA_CREATED_BY = "impala.created-by";
  /** Blob property with the NDV estimate of the sketch. */
  public static final String NDV = "ndv";

  /**
   * Writes a new Puffin file for snapshot 's1' with a theta blob per key of 'sketches'
   * and copies the blobs of the file registered for 's1' whose type and fields differ
   * from the written ones. Returns null and writes nothing if there are no blobs to
   * write or copy. 'tableName' is the Impala name of the table used in messages.
   */
  public StatisticsFile write(Table t, String tableName, long s1,
      Map<Integer, byte[]> sketches, Map<Integer, Long> ndv)
      throws ImpalaRuntimeException {
    Snapshot snapshot = t.snapshot(s1);
    Preconditions.checkArgument(snapshot != null, "no snapshot %s in %s", s1, tableName);
    String createdBy = createdBy();
    List<Blob> blobs = new ArrayList<>();
    Set<List<Object>> written = new HashSet<>();
    for (Map.Entry<Integer, byte[]> e : new TreeMap<>(sketches).entrySet()) {
      int field = e.getKey();
      Long fieldNdv = ndv.get(field);
      Preconditions.checkArgument(fieldNdv != null, "no NDV of field id %s", field);
      Map<String, String> props = new TreeMap<>();
      props.put(NDV, Long.toString(fieldNdv));
      props.put(IMPALA_CREATED_BY, createdBy);
      Blob blob = new Blob(StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
          ImmutableList.of(field), s1, snapshot.sequenceNumber(),
          ByteBuffer.wrap(e.getValue()), null, props);
      blobs.add(blob);
      written.add(typeAndFields(blob.type(), blob.inputFields()));
    }
    Optional<StatisticsFile> registered = t.statisticsFiles().stream()
        .filter(f -> f.snapshotId() == s1).findFirst();
    if (registered.isPresent()) {
      for (Blob blob : readRegistered(t, tableName, registered.get())) {
        if (!written.contains(typeAndFields(blob.type(), blob.inputFields()))) {
          blobs.add(blob);
        }
      }
    }
    if (blobs.isEmpty()) return null;
    return writeFile(t, s1, blobs);
  }

  /**
   * Writes a copy of 'file' without 'impalaBlobs', the blobs of its registration that
   * Impala wrote. Returns empty if no blob is left or the file is missing: then the
   * caller removes the registration.
   */
  public Optional<StatisticsFile> rewriteWithout(Table t, String tableName,
      StatisticsFile file, Set<BlobMetadata> impalaBlobs) throws ImpalaRuntimeException {
    if (impalaBlobs.containsAll(file.blobMetadata())) return Optional.empty();
    Set<List<Object>> dropped = impalaBlobs.stream()
        .map(b -> blobKey(b.type(), b.fields(), b.sourceSnapshotId(),
            b.sourceSnapshotSequenceNumber()))
        .collect(Collectors.toSet());
    List<Blob> kept = new ArrayList<>();
    for (Blob blob : readRegistered(t, tableName, file)) {
      // A blob of another engine may have the key of an Impala blob.
      boolean impala = blob.properties().containsKey(IMPALA_CREATED_BY);
      if (!impala || !dropped.contains(blobKey(blob.type(), blob.inputFields(),
          blob.snapshotId(), blob.sequenceNumber()))) {
        kept.add(blob);
      }
    }
    if (kept.isEmpty()) return Optional.empty();
    return Optional.of(writeFile(t, file.snapshotId(), kept));
  }

  /** Deletes a file this writer created; failures are only logged. */
  public void deleteQuietly(Table t, String path) {
    try {
      t.io().deleteFile(path);
    } catch (RuntimeException e) {
      LOG.warn("Failed to delete Puffin statistics file {}", path, e);
    }
  }

  /** Whether 'blob' was written by Impala. */
  public static boolean isImpalaBlob(BlobMetadata blob) {
    return blob.properties() != null && blob.properties().containsKey(IMPALA_CREATED_BY);
  }

  /** Integer "ndv" property of 'blob'; null if it has none or it is not an integer. */
  @Nullable
  public static Long ndvOf(BlobMetadata blob) {
    if (blob.properties() == null) return null;
    String ndv = blob.properties().get(NDV);
    if (ndv == null) return null;
    try {
      return Long.parseLong(ndv.trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /**
   * The ndv of the first Impala theta blob of 'fieldId' at 'snapshotId' that has one;
   * null if there is none. Analysis and catalogd both decide by this blob.
   */
  @Nullable
  public static Long impalaNdv(Iterable<StatisticsFile> files, int fieldId,
      long snapshotId) {
    for (StatisticsFile file : files) {
      for (BlobMetadata blob : file.blobMetadata()) {
        if (!StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1.equals(blob.type())
            || blob.fields() == null || blob.fields().size() != 1
            || blob.fields().get(0) != fieldId || blob.sourceSnapshotId() != snapshotId
            || !isImpalaBlob(blob)) {
          continue;
        }
        Long ndv = ndvOf(blob);
        if (ndv != null) return ndv;
      }
    }
    return null;
  }

  private static String createdBy() {
    return "Impala " + BackendConfig.INSTANCE.getImpalaBuildVersion();
  }

  private static List<Object> typeAndFields(String type, List<Integer> fields) {
    return ImmutableList.of(type, fields);
  }

  private static List<Object> blobKey(String type, List<Integer> fields,
      long snapshotId, long sequenceNumber) {
    return ImmutableList.of(type, fields, snapshotId, sequenceNumber);
  }

  /**
   * Blobs of the registered 'file'; empty if the file is missing. Any failure to check
   * or read it is thrown as ImpalaRuntimeException.
   */
  private static List<Blob> readRegistered(Table t, String tableName,
      StatisticsFile file) throws ImpalaRuntimeException {
    try {
      InputFile input = t.io().newInputFile(file.path());
      if (!input.exists()) {
        LOG.warn("Puffin statistics file {} of table {} is missing; lost blobs: {}",
            file.path(), tableName, describeBlobs(file.blobMetadata()));
        return Collections.emptyList();
      }
      return IcebergPuffinBlobReader.readAll(input);
    } catch (IOException | RuntimeException e) {
      throw new ImpalaRuntimeException(String.format(
          "Failed to read Puffin statistics file %s of table %s: %s", file.path(),
          tableName, IcebergPuffinBlobReader.describe(e)), e);
    }
  }

  private static String describeBlobs(List<BlobMetadata> blobs) {
    return blobs.stream()
        .map(b -> b.type() + " " + b.fields() + " " + b.sourceSnapshotId())
        .collect(Collectors.joining(", "));
  }

  /**
   * Writes 'blobs' to a new file in the metadata directory of 't'. On failure the file is
   * deleted before the error is thrown.
   */
  private StatisticsFile writeFile(Table t, long snapshotId, List<Blob> blobs)
      throws ImpalaRuntimeException {
    Preconditions.checkArgument(t instanceof HasTableOperations,
        "table %s has no TableOperations", t.name());
    // The location follows write.metadata.path of catalog tables.
    String path = ((HasTableOperations) t).operations().metadataFileLocation(
        snapshotId + "-" + UUID.randomUUID() + ".stats");
    TrackedOutputFile output = null;
    try {
      output = new TrackedOutputFile(t.io().newOutputFile(path));
      PuffinWriter writer = Puffin.write(output).createdBy(createdBy()).build();
      for (Blob blob : blobs) writer.add(blob);
      writer.close();
      return new GenericStatisticsFile(snapshotId, path, writer.fileSize(),
          writer.footerSize(), GenericBlobMetadata.from(writer.writtenBlobsMetadata()));
    } catch (IOException | RuntimeException e) {
      if (output != null) output.closeQuietly();
      deleteQuietly(t, path);
      throw new ImpalaRuntimeException(String.format(
          "Failed to write Puffin statistics file %s: %s", path,
          IcebergPuffinBlobReader.describe(e)), e);
    }
  }

  /**
   * Remembers the stream it creates. After a failed write the stream is closed directly:
   * closing the PuffinWriter would try to finish the file with a footer.
   */
  private static final class TrackedOutputFile implements OutputFile {
    private final OutputFile delegate_;
    private PositionOutputStream stream_;

    TrackedOutputFile(OutputFile delegate) { delegate_ = delegate; }

    @Override
    public PositionOutputStream create() {
      stream_ = delegate_.create();
      return stream_;
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      stream_ = delegate_.createOrOverwrite();
      return stream_;
    }

    @Override
    public String location() { return delegate_.location(); }

    @Override
    public InputFile toInputFile() { return delegate_.toInputFile(); }

    void closeQuietly() {
      if (stream_ == null) return;
      try {
        stream_.close();
      } catch (IOException | RuntimeException e) {
        LOG.warn("Failed to close Puffin statistics file {}", location(), e);
      }
    }
  }
}
