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
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.SnapshotUtil;

/**
 * Data files added in the gap (b, s1] of the parent chain of s1 by non-replace snapshots
 * and still live in s1, and the rows the gap deleted, taken from Iceberg metadata only.
 * Statistics analysis and scan planning both use it, so for the same table metadata,
 * b and s1 they see the same files. The gap is validated before any manifest is read;
 * Iceberg exceptions from reading manifests are not caught.
 */
public final class IcebergAddedFiles {
  /** Summary counters of a non-replace snapshot that mean it deleted rows. */
  private static final List<String> DELETE_COUNTERS = ImmutableList.of(
      SnapshotSummary.DELETED_FILES_PROP, SnapshotSummary.ADDED_DELETE_FILES_PROP,
      SnapshotSummary.DELETED_RECORDS_PROP, SnapshotSummary.ADDED_POS_DELETES_PROP,
      SnapshotSummary.ADDED_EQ_DELETES_PROP);

  private IcebergAddedFiles() {}

  public static final class AddedFiles {
    private static final AddedFiles EMPTY =
        new AddedFiles(Collections.emptyMap(), false, false);

    /** DataFile.location() of the files, as in FileScanTask.file() of the same table. */
    public final Set<String> paths;
    public final long fileCount;
    public final long bytes;
    public final long records;
    /** A replace snapshot rewrote a file that a non-replace gap snapshot added. */
    public final boolean rewrittenByReplace;
    /** A non-replace snapshot deleted rows, according to its summary. */
    public final boolean hasDeletes;

    private AddedFiles(Map<String, FileSizes> files, boolean rewrittenByReplace,
        boolean hasDeletes) {
      long bytes = 0;
      long records = 0;
      for (FileSizes f : files.values()) {
        bytes += f.bytes;
        records += f.records;
      }
      this.paths = Collections.unmodifiableSet(new LinkedHashSet<>(files.keySet()));
      this.fileCount = files.size();
      this.bytes = bytes;
      this.records = records;
      this.rewrittenByReplace = rewrittenByReplace;
      this.hasDeletes = hasDeletes;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof AddedFiles)) return false;
      AddedFiles other = (AddedFiles) o;
      return paths.equals(other.paths) && fileCount == other.fileCount
          && bytes == other.bytes && records == other.records
          && rewrittenByReplace == other.rewrittenByReplace
          && hasDeletes == other.hasDeletes;
    }

    @Override
    public int hashCode() {
      return Objects.hash(paths, fileCount, bytes, records, rewrittenByReplace,
          hasDeletes);
    }

    @Override
    public String toString() {
      return "AddedFiles{files=" + fileCount + ", bytes=" + bytes + ", records="
          + records + ", rewrittenByReplace=" + rewrittenByReplace + ", hasDeletes="
          + hasDeletes + ", paths=" + paths + "}";
    }
  }

  public static final class GapException extends Exception {
    private static final long serialVersionUID = 1L;

    public enum Kind { NOT_ANCESTOR, UNSUPPORTED_SNAPSHOT }

    private final Kind kind_;
    private final long snapshotId_;

    private GapException(Kind kind, long snapshotId, String message) {
      super(message);
      kind_ = kind;
      snapshotId_ = snapshotId;
    }

    public Kind kind() { return kind_; }

    /** The base for NOT_ANCESTOR, the gap snapshot for UNSUPPORTED_SNAPSHOT. */
    public long snapshotId() { return snapshotId_; }
  }

  /** Files of the gap (b, s1] by snapshot, so sub-gaps need no further reading. */
  public static final class Gap {
    private final long b_;
    private final long s1_;
    /** Oldest first. */
    private final List<GapSnapshot> snapshots_;

    private Gap(long b, long s1, List<GapSnapshot> snapshots) {
      b_ = b;
      s1_ = s1;
      snapshots_ = snapshots;
    }

    /**
     * Files of the sub-gap (x, s1], equal to compute(api, x, s1). 'x' must be b or a
     * snapshot of the gap, otherwise it is not known to be an ancestor of s1.
     */
    public AddedFiles after(long x) throws GapException {
      int start = -1;
      if (x == b_) {
        start = 0;
      } else {
        for (int i = 0; i < snapshots_.size(); ++i) {
          if (snapshots_.get(i).id == x) start = i + 1;
        }
      }
      if (start < 0) {
        throw new GapException(GapException.Kind.NOT_ANCESTOR, x, String.format(
            "Snapshot %s is not in the chain of snapshot %s after snapshot %s", x, s1_,
            b_));
      }
      if (start == snapshots_.size()) return AddedFiles.EMPTY;
      Map<String, FileSizes> live = new LinkedHashMap<>();
      boolean rewrittenByReplace = false;
      boolean hasDeletes = false;
      for (GapSnapshot s : snapshots_.subList(start, snapshots_.size())) {
        for (String path : s.removed) {
          // Only files added by non-replace gap snapshots are in 'live'.
          if (live.remove(path) != null && s.replace) rewrittenByReplace = true;
        }
        if (!s.replace) {
          live.putAll(s.added);
          hasDeletes |= s.hasDeletes;
        }
      }
      return new AddedFiles(live, rewrittenByReplace, hasDeletes);
    }
  }

  public static Gap read(Table api, long b, long s1) throws GapException {
    List<Snapshot> gap = gapSnapshots(api, b, s1);
    List<GapSnapshot> snapshots = new ArrayList<>(gap.size());
    for (Snapshot s : gap) snapshots.add(readSnapshot(s, api.io()));
    return new Gap(b, s1, Collections.unmodifiableList(snapshots));
  }

  public static AddedFiles compute(Table api, long b, long s1) throws GapException {
    return read(api, b, s1).after(b);
  }

  /**
   * Rows deleted by non-replace snapshots of the gap: deleted records plus the net
   * position and equality deletes they added. Counters missing from the summary or not
   * numbers count as 0. Reads no manifests.
   */
  public static long estimateDeletedRows(Table api, long b, long s1)
      throws GapException {
    long rows = 0;
    for (Snapshot s : gapSnapshots(api, b, s1)) {
      if (isReplace(s)) continue;
      Map<String, String> summary = s.summary();
      rows += counter(summary, SnapshotSummary.DELETED_RECORDS_PROP)
          + Math.max(0, counter(summary, SnapshotSummary.ADDED_POS_DELETES_PROP)
              - counter(summary, SnapshotSummary.REMOVED_POS_DELETES_PROP))
          + Math.max(0, counter(summary, SnapshotSummary.ADDED_EQ_DELETES_PROP)
              - counter(summary, SnapshotSummary.REMOVED_EQ_DELETES_PROP));
    }
    return rows;
  }

  /**
   * Snapshots of (b, s1], oldest first. The ancestor check must come first:
   * ancestorsBetween() returns the whole chain of s1 when b is not its ancestor.
   */
  private static List<Snapshot> gapSnapshots(Table api, long b, long s1)
      throws GapException {
    if (api.snapshot(s1) == null || (b != s1 && !SnapshotUtil.isAncestorOf(api, s1, b))) {
      throw new GapException(GapException.Kind.NOT_ANCESTOR, b, String.format(
          "Snapshot %s is not an ancestor of snapshot %s", b, s1));
    }
    if (b == s1) return Collections.emptyList();
    List<Snapshot> gap = Lists.newArrayList(SnapshotUtil.ancestorsBetween(api, s1, b));
    Collections.reverse(gap);
    for (Snapshot s : gap) {
      // Without a manifest list a v1 snapshot claims every listed manifest as its own,
      // and without an operation a replace cannot be told apart.
      if (s.manifestListLocation() == null || s.operation() == null) {
        throw new GapException(GapException.Kind.UNSUPPORTED_SNAPSHOT, s.snapshotId(),
            String.format("Snapshot %s has no manifest list or operation",
                s.snapshotId()));
      }
    }
    return gap;
  }

  /**
   * Reads the files of 's' from a detached copy: Snapshot objects of the table cache
   * the files with all their column statistics for the lifetime of the table metadata.
   */
  private static GapSnapshot readSnapshot(Snapshot s, FileIO io) {
    Preconditions.checkState(s.manifestListLocation() != null);
    Snapshot copy = SnapshotParser.fromJson(SnapshotParser.toJson(s));
    Map<String, FileSizes> added = new LinkedHashMap<>();
    for (DataFile f : copy.addedDataFiles(io)) {
      added.put(f.location(), new FileSizes(f.fileSizeInBytes(), f.recordCount()));
    }
    List<String> removed = new ArrayList<>();
    for (DataFile f : copy.removedDataFiles(io)) removed.add(f.location());
    return new GapSnapshot(s.snapshotId(), isReplace(s), hasDeletes(s),
        Collections.unmodifiableMap(added), Collections.unmodifiableList(removed));
  }

  private static boolean isReplace(Snapshot s) {
    return DataOperations.REPLACE.equals(s.operation());
  }

  private static boolean hasDeletes(Snapshot s) {
    for (String key : DELETE_COUNTERS) {
      if (counter(s.summary(), key) > 0) return true;
    }
    return false;
  }

  private static long counter(Map<String, String> summary, String key) {
    String value = summary == null ? null : summary.get(key);
    if (value == null) return 0;
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  private static final class FileSizes {
    final long bytes;
    final long records;

    FileSizes(long bytes, long records) {
      this.bytes = bytes;
      this.records = records;
    }
  }

  private static final class GapSnapshot {
    final long id;
    final boolean replace;
    final boolean hasDeletes;
    final Map<String, FileSizes> added;
    final List<String> removed;

    GapSnapshot(long id, boolean replace, boolean hasDeletes,
        Map<String, FileSizes> added, List<String> removed) {
      this.id = id;
      this.replace = replace;
      this.hasDeletes = hasDeletes;
      this.added = added;
      this.removed = removed;
    }
  }
}
