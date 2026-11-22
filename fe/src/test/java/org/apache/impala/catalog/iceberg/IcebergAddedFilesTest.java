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
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.types.Types;
import org.apache.impala.catalog.iceberg.IcebergAddedFiles.AddedFiles;
import org.apache.impala.catalog.iceberg.IcebergAddedFiles.Gap;
import org.apache.impala.catalog.iceberg.IcebergAddedFiles.GapException;
import org.apache.impala.catalog.iceberg.IcebergAddedFiles.GapException.Kind;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class IcebergAddedFilesTest {
  private static final Schema SCHEMA = new Schema(
      required(1, "id", Types.IntegerType.get()),
      optional(2, "s", Types.StringType.get()));
  /** Snapshot fields where Iceberg caches the files read from manifests. */
  private static final List<String> FILE_CACHES = ImmutableList.of(
      "addedDataFiles", "removedDataFiles", "addedDeleteFiles", "removedDeleteFiles");

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  private interface GapCall { void run() throws GapException; }

  private IcebergTestTables create(int formatVersion) {
    return IcebergTestTables.create(tmp, "db", "t", SCHEMA, PartitionSpec.unpartitioned(),
        formatVersion);
  }

  /** Data file of 'size' bytes and 'rows' records, so sums tell files apart. */
  private static DataFile file(IcebergTestTables t, long size, int rows) {
    return t.file(size, null, ImmutableMap.of("id", Collections.nCopies(rows, 1)));
  }

  private static void assertFiles(AddedFiles actual, DataFile... expected) {
    Set<String> paths = new HashSet<>();
    long bytes = 0;
    long records = 0;
    for (DataFile f : expected) {
      paths.add(f.location());
      bytes += f.fileSizeInBytes();
      records += f.recordCount();
    }
    assertEquals(paths, actual.paths);
    assertEquals(expected.length, actual.fileCount);
    assertEquals(bytes, actual.bytes);
    assertEquals(records, actual.records);
  }

  private static void assertGap(Kind kind, long snapshotId, GapCall call) {
    try {
      call.run();
      fail("expected " + kind);
    } catch (GapException e) {
      assertEquals(kind, e.kind());
      assertEquals(snapshotId, e.snapshotId());
    }
  }

  /** Every way to reach the gap (b, s1] reports the same failure. */
  private static void assertGapAll(Kind kind, long snapshotId, Table api, long b,
      long s1) {
    assertGap(kind, snapshotId, () -> IcebergAddedFiles.read(api, b, s1));
    assertGap(kind, snapshotId, () -> IcebergAddedFiles.compute(api, b, s1));
    assertGap(kind, snapshotId, () -> IcebergAddedFiles.estimateDeletedRows(api, b, s1));
  }

  private static void assertNoFileCaches(Table api) throws Exception {
    for (Snapshot s : api.snapshots()) {
      for (String name : FILE_CACHES) {
        Field field = s.getClass().getDeclaredField(name);
        field.setAccessible(true);
        assertNull(name + " of snapshot " + s.snapshotId(), field.get(s));
      }
    }
  }

  @Test
  public void sameSnapshotGivesEmptyGap() throws Exception {
    IcebergTestTables t = create(2);
    long b = t.append(file(t, 10, 2));
    Table api = t.table();
    AddedFiles files = IcebergAddedFiles.compute(api, b, b);
    assertFiles(files);
    assertFalse(files.rewrittenByReplace);
    assertFalse(files.hasDeletes);
    assertEquals(files, IcebergAddedFiles.read(api, b, b).after(b));
    assertEquals(0, IcebergAddedFiles.estimateDeletedRows(api, b, b));
  }

  @Test
  public void appendChainGivesFilesAddedAfterBase() throws Exception {
    IcebergTestTables t = create(2);
    long b = t.append(file(t, 10, 1), file(t, 11, 2));
    DataFile f1 = file(t, 20, 3);
    DataFile f2 = file(t, 30, 4);
    long s2 = t.append(f1, f2);
    DataFile f3 = file(t, 40, 5);
    long s3 = t.append(f3);
    Table api = t.table();
    AddedFiles files = IcebergAddedFiles.compute(api, b, s3);
    assertFiles(files, f1, f2, f3);
    assertFalse(files.rewrittenByReplace);
    assertFalse(files.hasDeletes);
    assertFiles(IcebergAddedFiles.compute(api, s2, s3), f3);
    assertFiles(IcebergAddedFiles.compute(api, b, s2), f1, f2);
    assertEquals(0, IcebergAddedFiles.estimateDeletedRows(api, b, s3));
  }

  @Test
  public void deleteAndOverwriteDropGapAndBaseFiles() throws Exception {
    IcebergTestTables t = create(2);
    DataFile base0 = file(t, 10, 1);
    DataFile base1 = file(t, 11, 2);
    long b = t.append(base0, base1);
    DataFile g1 = file(t, 20, 3);
    DataFile g2 = file(t, 21, 4);
    t.append(g1, g2);
    DataFile g3 = file(t, 22, 5);
    long ow = t.overwrite(ImmutableList.of(base0, g2), ImmutableList.of(g3));
    long d = t.delete(g1, base1);
    Table api = t.table();
    AddedFiles files = IcebergAddedFiles.compute(api, b, d);
    assertFiles(files, g3);
    assertFalse(files.rewrittenByReplace);
    assertTrue(files.hasDeletes);
    AddedFiles beforeDelete = IcebergAddedFiles.compute(api, b, ow);
    assertFiles(beforeDelete, g1, g3);
    assertTrue(beforeDelete.hasDeletes);
  }

  @Test
  public void replaceIsExcludedAndFlagsRewrittenGapFiles() throws Exception {
    IcebergTestTables t = create(2);
    DataFile base0 = file(t, 10, 1);
    DataFile base1 = file(t, 11, 2);
    long b = t.append(base0, base1);
    DataFile g1 = file(t, 20, 3);
    long s2 = t.append(g1);
    DataFile r1 = file(t, 30, 3);
    long rBase = t.replace(ImmutableList.of(base0, base1), ImmutableList.of(r1));
    DataFile r2 = file(t, 31, 3);
    long rReplaced = t.replace(ImmutableList.of(r1), ImmutableList.of(r2));
    DataFile r3 = file(t, 32, 3);
    long rGap = t.replace(ImmutableList.of(g1), ImmutableList.of(r3));
    Table api = t.table();

    AddedFiles baseRewritten = IcebergAddedFiles.compute(api, b, rBase);
    assertFiles(baseRewritten, g1);
    assertFalse(baseRewritten.rewrittenByReplace);
    assertFalse(baseRewritten.hasDeletes);
    AddedFiles replaceRewritten = IcebergAddedFiles.compute(api, b, rReplaced);
    assertFiles(replaceRewritten, g1);
    assertFalse(replaceRewritten.rewrittenByReplace);
    AddedFiles gapRewritten = IcebergAddedFiles.compute(api, b, rGap);
    assertFiles(gapRewritten);
    assertTrue(gapRewritten.rewrittenByReplace);
    assertFalse(gapRewritten.hasDeletes);
    // g1 is added at or before the base of the sub-gap, so it is a base file there.
    AddedFiles afterG1 = IcebergAddedFiles.compute(api, s2, rGap);
    assertFiles(afterG1);
    assertFalse(afterG1.rewrittenByReplace);
    assertEquals(0, IcebergAddedFiles.estimateDeletedRows(api, b, rGap));
  }

  @Test
  public void branchGapFollowsParentChainOfS1() throws Exception {
    IcebergTestTables t = create(2);
    long b = t.append(file(t, 10, 1));
    DataFile g1 = file(t, 20, 2);
    long br1 = t.appendToBranch("br", g1);
    DataFile m1 = file(t, 30, 3);
    long main1 = t.append(m1);
    DataFile g2 = file(t, 40, 4);
    long br2 = t.appendToBranch("br", g2);
    Table api = t.table();
    assertFiles(IcebergAddedFiles.compute(api, b, br2), g1, g2);
    assertFiles(IcebergAddedFiles.compute(api, b, main1), m1);
    assertGapAll(Kind.NOT_ANCESTOR, br1, api, br1, main1);
    assertGapAll(Kind.NOT_ANCESTOR, main1, api, main1, br2);
  }

  @Test
  public void rollbackDropsSnapshotsOffParentChain() throws Exception {
    IcebergTestTables t = create(2);
    long s0 = t.append(file(t, 10, 1));
    DataFile f1 = file(t, 20, 2);
    long s1 = t.append(f1);
    long s2 = t.append(file(t, 30, 3));
    t.rollbackTo(s1);
    DataFile f3 = file(t, 40, 4);
    long s3 = t.append(f3);
    Table api = t.table();
    assertFiles(IcebergAddedFiles.compute(api, s0, s3), f1, f3);
    assertGapAll(Kind.NOT_ANCESTOR, s2, api, s2, s3);
  }

  @Test
  public void expiredBaseOrAncestorIsNotAncestor() throws Exception {
    IcebergTestTables t = create(2);
    long s0 = t.append(file(t, 10, 1));
    long s1 = t.append(file(t, 20, 2));
    t.append(file(t, 30, 3));
    long s3 = t.append(file(t, 40, 4));
    t.expire(s1);
    Table api = t.table();
    assertNull(api.snapshot(s1));
    assertGapAll(Kind.NOT_ANCESTOR, s1, api, s1, s3);
    // The chain from s3 breaks at the expired s1, so s0 is not reachable either.
    assertGapAll(Kind.NOT_ANCESTOR, s0, api, s0, s3);
  }

  @Test
  public void missingSnapshotIsNotAncestor() throws Exception {
    IcebergTestTables t = create(2);
    long b = t.append(file(t, 10, 1));
    Table api = t.table();
    assertGapAll(Kind.NOT_ANCESTOR, b, api, b, -1);
    assertGapAll(Kind.NOT_ANCESTOR, -1, api, -1, b);
    assertGapAll(Kind.NOT_ANCESTOR, -1, api, -1, -1);
  }

  @Test
  public void snapshotWithoutManifestListOrOperationIsUnsupported() throws Exception {
    IcebergTestTables t = create(1);
    long b = t.append(file(t, 10, 1));
    long noList = t.append(file(t, 20, 2));
    long noOperation = t.append(file(t, 30, 3));
    DataFile f4 = file(t, 40, 4);
    long s4 = t.append(f4);
    t.removeManifestList(noList);
    t.removeSummary(noOperation);
    Table api = t.table();
    assertGapAll(Kind.UNSUPPORTED_SNAPSHOT, noList, api, b, s4);
    assertGapAll(Kind.UNSUPPORTED_SNAPSHOT, noOperation, api, noList, s4);
    assertFiles(IcebergAddedFiles.compute(api, noOperation, s4), f4);
  }

  @Test
  public void gapIsCheckedBeforeManifestsAreRead() throws Exception {
    IcebergTestTables t = create(1);
    long b = t.append(file(t, 10, 1));
    long older = t.append(file(t, 20, 2));
    long noList = t.append(file(t, 30, 3));
    t.removeManifestList(noList);
    Table api = t.table();
    File manifestList =
        new File(new Path(api.snapshot(older).manifestListLocation()).toUri());
    assertTrue(manifestList.delete());
    assertGapAll(Kind.UNSUPPORTED_SNAPSHOT, noList, api, b, noList);
  }

  @Test
  public void missingManifestListOfSupportedGapFails() throws Exception {
    IcebergTestTables t = create(1);
    long b = t.append(file(t, 10, 1));
    long s1 = t.append(file(t, 20, 2));
    Table api = t.table();
    File manifestList =
        new File(new Path(api.snapshot(s1).manifestListLocation()).toUri());
    assertTrue(manifestList.delete());
    try {
      IcebergAddedFiles.compute(api, b, s1);
      fail("missing manifest list was read");
    } catch (NotFoundException e) {
      assertTrue(e.getMessage(), e.getMessage().contains(manifestList.getName()));
    }
  }

  @Test
  public void afterEqualsComputeAndRepeatsEqual() throws Exception {
    IcebergTestTables t = create(2);
    DataFile base0 = file(t, 10, 1);
    long b = t.append(base0);
    DataFile g1 = file(t, 20, 2);
    DataFile g2 = file(t, 21, 3);
    List<Long> gap = new ArrayList<>();
    gap.add(t.append(g1, g2));
    gap.add(t.delete(g2));
    gap.add(t.replace(ImmutableList.of(g1), ImmutableList.of(file(t, 30, 2))));
    gap.add(t.rowDelta(t.positionDeletes(5, 1, null)));
    gap.add(t.overwrite(ImmutableList.of(base0), ImmutableList.of(file(t, 40, 4))));
    long s1 = t.append(file(t, 50, 5));
    gap.add(s1);
    long outside = t.append(file(t, 60, 6));
    Table api = t.table();

    Gap read = IcebergAddedFiles.read(api, b, s1);
    List<Long> bases = new ArrayList<>();
    bases.add(b);
    bases.addAll(gap);
    for (long x : bases) {
      AddedFiles expected = IcebergAddedFiles.compute(api, x, s1);
      assertEquals("after " + x, expected, read.after(x));
      assertEquals("repeated compute " + x, expected,
          IcebergAddedFiles.compute(api, x, s1));
    }
    assertEquals(read.after(b), IcebergAddedFiles.read(api, b, s1).after(b));
    assertTrue(read.after(b).rewrittenByReplace);
    assertTrue(read.after(b).hasDeletes);
    assertGap(Kind.NOT_ANCESTOR, outside, () -> read.after(outside));
    Long parent = api.snapshot(b).parentId();
    long beforeBase = parent == null ? -1 : parent;
    assertGap(Kind.NOT_ANCESTOR, beforeBase, () -> read.after(beforeBase));
    assertNoFileCaches(api);
  }

  @Test
  public void gapReadingLeavesSnapshotFileCachesEmpty() throws Exception {
    IcebergTestTables t = create(2);
    DataFile base0 = file(t, 10, 1);
    long b = t.append(base0);
    DataFile g1 = file(t, 20, 2);
    t.append(g1);
    t.rowDelta(t.positionDeletes(5, 1, null));
    long s1 = t.overwrite(ImmutableList.of(base0, g1), ImmutableList.of(file(t, 30, 3)));
    Table api = t.table();
    IcebergAddedFiles.compute(api, b, s1);
    IcebergAddedFiles.read(api, b, s1).after(b);
    IcebergAddedFiles.estimateDeletedRows(api, b, s1);
    assertNoFileCaches(api);
  }

  @Test
  public void deletedRowsAreEstimatedFromNonReplaceSummaries() throws Exception {
    IcebergTestTables t = create(2);
    DataFile base0 = file(t, 10, 1);
    long b = t.append(base0);
    long rd = t.rowDelta(t.positionDeletes(5, 3, null),
        t.equalityDeletes(6, 4, null, 1));
    t.setSummary(rd, ImmutableMap.<String, String>builder()
        .put("deleted-records", "7")
        .put("added-position-deletes", "3")
        .put("removed-position-deletes", "1")
        .put("added-equality-deletes", "4")
        .put("removed-equality-deletes", "0")
        .build());
    long d = t.delete(base0);
    t.setSummary(d, ImmutableMap.of("deleted-records", "not a number"));
    // Deletes of 'rd' apply to every older data file, so replace a newer one.
    DataFile g1 = file(t, 20, 2);
    long a = t.append(g1);
    long rp = t.replace(ImmutableList.of(g1), ImmutableList.of(file(t, 30, 2)));
    t.setSummary(rp, ImmutableMap.of("deleted-records", "100",
        "added-position-deletes", "5", "deleted-data-files", "1"));
    long rd2 = t.rowDelta(t.positionDeletes(5, 1, null));
    t.setSummary(rd2, ImmutableMap.of("added-position-deletes", "1",
        "removed-position-deletes", "4", "added-equality-deletes", "0",
        "removed-equality-deletes", "2"));
    Table api = t.table();

    assertEquals(7 + 2 + 4, IcebergAddedFiles.estimateDeletedRows(api, b, rd));
    assertEquals(13, IcebergAddedFiles.estimateDeletedRows(api, b, rd2));
    assertEquals(0, IcebergAddedFiles.estimateDeletedRows(api, rd, rd2));
    assertEquals(0, IcebergAddedFiles.estimateDeletedRows(api, d, rp));

    AddedFiles rowDeltaOnly = IcebergAddedFiles.compute(api, b, rd);
    assertFiles(rowDeltaOnly);
    assertTrue(rowDeltaOnly.hasDeletes);
    AddedFiles deleteOnly = IcebergAddedFiles.compute(api, rd, d);
    assertFiles(deleteOnly);
    assertTrue(deleteOnly.hasDeletes);
    AddedFiles replaceOnly = IcebergAddedFiles.compute(api, a, rp);
    assertFiles(replaceOnly);
    assertFalse(replaceOnly.hasDeletes);
    assertFalse(replaceOnly.rewrittenByReplace);
    AddedFiles appendAndReplace = IcebergAddedFiles.compute(api, d, rp);
    assertFiles(appendAndReplace);
    assertFalse(appendAndReplace.hasDeletes);
    assertTrue(appendAndReplace.rewrittenByReplace);
    assertTrue(IcebergAddedFiles.compute(api, rp, rd2).hasDeletes);
  }

  @Test
  public void summaryWithoutPositiveCountersHasNoDeletes() throws Exception {
    IcebergTestTables t = create(2);
    DataFile base0 = file(t, 10, 1);
    long b = t.append(base0);
    long d = t.delete(base0);
    t.setSummary(d, ImmutableMap.of("deleted-data-files", "0", "deleted-records", "x"));
    Table api = t.table();
    AddedFiles files = IcebergAddedFiles.compute(api, b, d);
    assertFiles(files);
    assertFalse(files.hasDeletes);
    assertEquals(0, IcebergAddedFiles.estimateDeletedRows(api, b, d));
  }
}
