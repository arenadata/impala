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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.util.Pair;
import org.apache.impala.common.ImpalaRuntimeException;

/**
 * Reads blobs of Puffin statistics files.
 */
public class IcebergPuffinBlobReader {
  private IcebergPuffinBlobReader() {}

  /**
   * Returns the decompressed theta sketch of every field f of 'blobSnapshotIds' whose
   * blob has inputFields [f] and snapshot id blobSnapshotIds[f]. A field without such a
   * blob has no key in the result; the caller decides whether that is an error.
   * Throws if the file cannot be read, is not a Puffin file or has several such blobs
   * for a field.
   */
  public static Map<Integer, ByteBuffer> readThetaBlobs(FileIO io, String path,
      Map<Integer, Long> blobSnapshotIds) throws ImpalaRuntimeException {
    try (PuffinReader reader = Puffin.read(io.newInputFile(path)).build()) {
      List<BlobMetadata> selected = new ArrayList<>();
      Set<Integer> fields = new HashSet<>();
      for (BlobMetadata blob : reader.fileMetadata().blobs()) {
        if (!StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1.equals(blob.type())
            || blob.inputFields().size() != 1) {
          continue;
        }
        int field = blob.inputFields().get(0);
        Long snapshotId = blobSnapshotIds.get(field);
        if (snapshotId == null || snapshotId != blob.snapshotId()) continue;
        if (!fields.add(field)) {
          throw new ImpalaRuntimeException(String.format(
              "Puffin statistics file %s has several theta blobs for field id %d and "
              + "snapshot %d", path, field, snapshotId));
        }
        selected.add(blob);
      }
      Map<Integer, ByteBuffer> result = new HashMap<>();
      for (Pair<BlobMetadata, ByteBuffer> p : reader.readAll(selected)) {
        result.put(p.first().inputFields().get(0), p.second());
      }
      return result;
    } catch (IOException | UncheckedIOException | NotFoundException
        | IllegalStateException | IllegalArgumentException e) {
      // The reader reports a file that is not Puffin with IllegalStateException and
      // IllegalArgumentException; a missing file surfaces as NotFoundException.
      throw new ImpalaRuntimeException(String.format(
          "Failed to read Puffin statistics file %s: %s", path, describe(e)), e);
    }
  }

  /**
   * Reads every blob of 'file', decompressed and with no requested compression, so that
   * a writer stores it unchanged and uncompressed. Read errors are thrown as they are.
   */
  static List<Blob> readAll(InputFile file) throws IOException {
    try (PuffinReader reader = Puffin.read(file).build()) {
      List<Blob> blobs = new ArrayList<>();
      for (Pair<BlobMetadata, ByteBuffer> p
          : reader.readAll(reader.fileMetadata().blobs())) {
        BlobMetadata m = p.first();
        blobs.add(new Blob(m.type(), m.inputFields(), m.snapshotId(),
            m.sequenceNumber(), p.second(), null, m.properties()));
      }
      return blobs;
    }
  }

  /** Message of 'e' without the class name that UncheckedIOException adds. */
  static String describe(Throwable e) {
    Throwable cause = e instanceof UncheckedIOException && e.getCause() != null
        ? e.getCause() : e;
    return cause.getMessage() != null ? cause.getMessage() : cause.toString();
  }
}
