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

package org.apache.impala.planner;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.impala.planner.IcebergAddedFilesScanTest.locations;
import static org.apache.impala.planner.IcebergAddedFilesScanTest.scannedPaths;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.analysis.IcebergAnalysisFixture;
import org.apache.impala.analysis.SelectStmt;
import org.apache.impala.catalog.iceberg.IcebergTestTables;
import org.apache.impala.common.FrontendTestBase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

/**
 * Scan planning of the snapshot that the file descriptors of the table were loaded for:
 * the files come from the descriptors, so manifests deleted after the load do not fail
 * the planning unless it has to read them.
 */
public class IcebergLoadedSnapshotScanTest extends FrontendTestBase {
  private static final String DB = "iceberg_loaded_snapshot_scan";
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()));

  @Rule public TemporaryFolder tmp = new TemporaryFolder();
  @Rule public TestName testName = new TestName();

  private static final IcebergAnalysisFixture ifx =
      IcebergAnalysisFixture.create(feFixture_);

  private String fullName() {
    return DB + "." + testName.getMethodName().toLowerCase();
  }

  private IcebergTestTables create() {
    return IcebergTestTables.create(tmp, DB, testName.getMethodName().toLowerCase(),
        SCHEMA, PartitionSpec.unpartitioned(), 2);
  }

  private AnalysisResult analyze(String sql) throws Exception {
    return parseAndAnalyze(sql, feFixture_.createAnalysisCtx());
  }

  private String hinted(long base, Long asOf) {
    return "select id from " + fullName()
        + (asOf == null ? "" : " for system_version as of " + asOf)
        + " /* +ICEBERG_ADDED_FILES_AFTER(" + base + ") */";
  }

  private static void deleteFile(String location) {
    assertTrue(location, new File(new Path(location).toUri()).delete());
  }

  /** Deletes the manifest list of 'snapshotId' and the manifests it added. */
  private static void deleteManifests(IcebergTestTables t, long snapshotId) {
    Snapshot snapshot = t.table().snapshot(snapshotId);
    for (ManifestFile m : snapshot.allManifests(t.table().io())) {
      if (m.snapshotId() == snapshotId) deleteFile(m.path());
    }
    deleteFile(snapshot.manifestListLocation());
  }

  @Test
  public void timeTravelToLoadedSnapshotReadsNoManifests() throws Exception {
    IcebergTestTables t = create();
    DataFile f0 = t.file(100, null);
    DataFile f1 = t.file(100, null);
    long s0 = t.append(f0);
    long s1 = t.append(f1);
    ifx.addPathB(t, Collections.emptyMap());
    deleteManifests(t, s0);
    deleteManifests(t, s1);

    assertEquals(locations(f0, f1), scannedPaths(analyze(
        "select id from " + fullName() + " for system_version as of " + s1)));
  }

  @Test
  public void countStarOfLoadedSnapshotReadsNoManifests() throws Exception {
    IcebergTestTables t = create();
    long s0 = t.append(t.file(100, null));
    ifx.addPathB(t, Collections.emptyMap());
    deleteManifests(t, s0);

    AnalysisResult res = analyze("select count(*) from " + fullName()
        + " for system_version as of " + s0);
    // The count of the snapshot summary replaces the scan.
    assertTrue(((SelectStmt) res.getStmt()).getTableRefs().isEmpty());
  }

  @Test
  public void countStarWithDeletesOfLoadedSnapshotPlansAsWithoutTimeTravel()
      throws Exception {
    IcebergTestTables t = create();
    long s0 = t.append(t.file(100, null));
    DeleteFile deletes = t.positionDeletes(10, 1, null);
    long s1 = t.rowDelta(deletes);
    long s2 = t.append(t.file(100, null));
    ifx.addPathB(t, Collections.emptyMap());
    List<String> expected = scannedPaths(analyze("select count(*) from " + fullName()));
    assertTrue(expected.toString(), expected.contains(deletes.location()));
    for (long s : new long[] {s0, s1, s2}) deleteManifests(t, s);

    assertEquals(expected, scannedPaths(analyze("select count(*) from " + fullName()
        + " for system_version as of " + s2)));
  }

  @Test
  public void hintedScanOfLoadedSnapshotReadsGapManifestsOnly() throws Exception {
    IcebergTestTables t = create();
    DataFile f1 = t.file(100, null);
    DataFile f2 = t.file(100, null);
    long s0 = t.append(t.file(100, null));
    t.append(f1);
    long s2 = t.append(f2);
    ifx.addPathB(t, Collections.emptyMap());
    deleteManifests(t, s0);

    assertEquals(locations(f1, f2), scannedPaths(analyze(hinted(s0, s2))));
    assertEquals(locations(f1, f2), scannedPaths(analyze(hinted(s0, null))));
  }

  @Test
  public void gapFileRemovedBeforeLoadedSnapshotIsNotScanned() throws Exception {
    IcebergTestTables t = create();
    DataFile removed = t.file(100, null);
    DataFile kept = t.file(100, null);
    long s0 = t.append(t.file(100, null));
    t.append(removed, kept);
    long s2 = t.delete(removed);
    ifx.addPathB(t, Collections.emptyMap());
    deleteManifests(t, s0);

    assertEquals(locations(kept), scannedPaths(analyze(hinted(s0, s2))));
  }
}
