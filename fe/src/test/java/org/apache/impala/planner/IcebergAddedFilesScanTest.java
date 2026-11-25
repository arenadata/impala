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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.fs.Path;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.AnalysisContext;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.analysis.IcebergAnalysisFixture;
import org.apache.impala.analysis.InlineViewRef;
import org.apache.impala.analysis.NumericLiteral;
import org.apache.impala.analysis.SelectStmt;
import org.apache.impala.analysis.TableRef;
import org.apache.impala.authorization.AuthorizationChecker;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.iceberg.IcebergAddedFiles;
import org.apache.impala.catalog.iceberg.IcebergTestTables;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TReplicaPreference;
import org.apache.impala.util.EventSequence;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

/** Analysis and scan planning of the internal ICEBERG_ADDED_FILES_AFTER table hint. */
public class IcebergAddedFilesScanTest extends FrontendTestBase {
  private static final String DB = "iceberg_added_files_scan";
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()));
  private static final Schema PART_SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "p", Types.IntegerType.get()));
  private static final String ERR =
      "ICEBERG_ADDED_FILES_AFTER requires an Iceberg table and one snapshot id argument";

  @Rule public TemporaryFolder tmp = new TemporaryFolder();
  @Rule public TestName testName = new TestName();

  private static final IcebergAnalysisFixture ifx =
      IcebergAnalysisFixture.create(feFixture_);

  private String fullName() {
    return DB + "." + testName.getMethodName().toLowerCase();
  }

  /** Table with two snapshots of one data file each; returns the snapshot ids. */
  private long[] table() throws Exception {
    IcebergTestTables t = IcebergTestTables.create(tmp, DB,
        testName.getMethodName().toLowerCase(), SCHEMA, PartitionSpec.unpartitioned(),
        2);
    long s0 = t.append(t.file(100, null));
    long s1 = t.append(t.file(100, null));
    ifx.addPathB(t, Collections.emptyMap());
    return new long[] {s0, s1};
  }

  private AnalysisResult analyze(String sql) throws Exception {
    return parseAndAnalyze(sql, feFixture_.createAnalysisCtx());
  }

  private static TableRef onlyRef(AnalysisResult res) {
    List<TableRef> refs = ((SelectStmt) res.getStmt()).getTableRefs();
    assertEquals(1, refs.size());
    return refs.get(0);
  }

  private void analysisError(String sql, String expected) throws Exception {
    try {
      analyze(sql);
    } catch (AnalysisException e) {
      assertTrue("unexpected message: " + e.getMessage(),
          e.getMessage().startsWith(expected));
      return;
    }
    fail("no AnalysisException for: " + sql);
  }

  @Test
  public void hintOnIcebergTableSetsSnapshotId() throws Exception {
    long[] s = table();
    for (String name : new String[] {"ICEBERG_ADDED_FILES_AFTER",
        "iceberg_added_files_after", "Iceberg_Added_Files_After"}) {
      AnalysisResult res = analyze("select id from " + fullName() + " /* +" + name
          + "(" + s[0] + ") */");
      assertEquals(Long.valueOf(s[0]), onlyRef(res).getIcebergAddedFilesAfter());
      assertTrue(res.getAnalyzer().hasPlanHints());
      assertTrue(res.getAnalyzer().getWarnings().toString(),
          res.getAnalyzer().getWarnings().isEmpty());
    }
  }

  @Test
  public void hintWithTimeTravelSetsSnapshotId() throws Exception {
    long[] s = table();
    AnalysisResult res = analyze("select id from " + fullName()
        + " for system_version as of " + s[1] + " /* +ICEBERG_ADDED_FILES_AFTER("
        + s[0] + ") */");
    assertEquals(Long.valueOf(s[0]), onlyRef(res).getIcebergAddedFilesAfter());
  }

  @Test
  public void noHintLeavesFieldNull() throws Exception {
    table();
    assertNull(onlyRef(analyze("select id from " + fullName()))
        .getIcebergAddedFilesAfter());
  }

  @Test
  public void hintsBesideSnapshotHintAreKept() throws Exception {
    long[] s = table();
    AnalysisResult res = analyze("select id from " + fullName()
        + " /* +SCHEDULE_REMOTE,ICEBERG_ADDED_FILES_AFTER(" + s[0] + ") */");
    assertEquals(Long.valueOf(s[0]), onlyRef(res).getIcebergAddedFilesAfter());
    assertEquals(TReplicaPreference.REMOTE, onlyRef(res).getReplicaPreference());
    assertTrue(res.getAnalyzer().getWarnings().isEmpty());

    // Hints listed after the snapshot hint must not be dropped.
    res = analyze("select id from " + fullName()
        + " /* +ICEBERG_ADDED_FILES_AFTER(" + s[0] + "),SCHEDULE_REMOTE */");
    assertEquals(Long.valueOf(s[0]), onlyRef(res).getIcebergAddedFilesAfter());
    assertEquals(TReplicaPreference.REMOTE, onlyRef(res).getReplicaPreference());
    assertTrue(res.getAnalyzer().getWarnings().isEmpty());
  }

  @Test
  public void hintSurvivesRewriteReanalysis() throws Exception {
    long[] s = table();
    // Constant folding rewrites the statement, which is then reset and analyzed again.
    // The test fixture disables expr rewrites by default.
    AnalysisContext ctx = feFixture_.createAnalysisCtx();
    ctx.getQueryOptions().setEnable_expr_rewrites(true);
    AnalysisResult res = parseAndAnalyze("select id, 1 + 1 from " + fullName()
        + " /* +ICEBERG_ADDED_FILES_AFTER(" + s[0] + ") */", ctx);
    assertEquals(Long.valueOf(s[0]), onlyRef(res).getIcebergAddedFilesAfter());
    assertTrue(((SelectStmt) res.getStmt()).getResultExprs().get(1)
        instanceof NumericLiteral);
  }

  @Test
  public void resetClearsAndCloneKeepsSnapshotId() throws Exception {
    long[] s = table();
    SelectStmt stmt = (SelectStmt) analyze("select id from " + fullName()
        + " /* +ICEBERG_ADDED_FILES_AFTER(" + s[0] + ") */").getStmt();
    SelectStmt clone = (SelectStmt) stmt.clone();
    assertEquals(Long.valueOf(s[0]),
        clone.getTableRefs().get(0).getIcebergAddedFilesAfter());
    stmt.reset();
    assertNull(stmt.getTableRefs().get(0).getIcebergAddedFilesAfter());
    assertEquals(Long.valueOf(s[0]),
        clone.getTableRefs().get(0).getIcebergAddedFilesAfter());
  }

  @Test
  public void secondHintIsError() throws Exception {
    long[] s = table();
    analysisError("select id from " + fullName() + " /* +ICEBERG_ADDED_FILES_AFTER("
        + s[0] + "),ICEBERG_ADDED_FILES_AFTER(" + s[1] + ") */", ERR);
  }

  @Test
  public void wrongArgumentCountIsError() throws Exception {
    table();
    analysisError("select id from " + fullName()
        + " /* +ICEBERG_ADDED_FILES_AFTER */", ERR);
    analysisError("select id from " + fullName()
        + " /* +ICEBERG_ADDED_FILES_AFTER(a, b) */", ERR);
    // Valid hint after the invalid one must not hide the error.
    analysisError("select id from " + fullName()
        + " /* +ICEBERG_ADDED_FILES_AFTER(a,b),SCHEDULE_REMOTE */", ERR);
    analysisError("select id from " + fullName()
        + " /* +SCHEDULE_REMOTE,ICEBERG_ADDED_FILES_AFTER(a,b) */", ERR);
  }

  @Test
  public void notLongArgumentIsError() throws Exception {
    table();
    analysisError("select id from " + fullName()
        + " /* +ICEBERG_ADDED_FILES_AFTER(abc) */", ERR);
    analysisError("select id from " + fullName()
        + " /* +ICEBERG_ADDED_FILES_AFTER(99999999999999999999) */", ERR);
  }

  @Test
  public void viewIsError() throws Exception {
    table();
    feFixture_.addTestView("create view " + fullName() + "_v as select id from "
        + fullName());
    analysisError("select id from " + fullName() + "_v /* +ICEBERG_ADDED_FILES_AFTER(1)"
        + " */", ERR);
  }

  @Test
  public void subqueryIsError() throws Exception {
    table();
    analysisError("select id from (select id from " + fullName()
        + ") v /* +ICEBERG_ADDED_FILES_AFTER(1) */", ERR);
  }

  @Test
  public void hdfsTableIsError() throws Exception {
    table();
    feFixture_.addTestTable("create table " + fullName() + "_h (id int)");
    analysisError("select id from " + fullName() + "_h /* +ICEBERG_ADDED_FILES_AFTER(1)"
        + " */", ERR);
  }

  @Test
  public void icebergMetadataTableIsError() throws Exception {
    table();
    analysisError("select * from " + fullName() + ".snapshots /* "
        + "+ICEBERG_ADDED_FILES_AFTER(1) */", ERR);
  }

  @Test
  public void unknownHintStaysWarning() throws Exception {
    table();
    AnalysisResult res = analyze("select id from " + fullName()
        + " /* +NO_SUCH_HINT(1) */");
    assertNull(onlyRef(res).getIcebergAddedFilesAfter());
    assertEquals(1, res.getAnalyzer().getWarnings().size());
    assertTrue(res.getAnalyzer().getWarnings().get(0),
        res.getAnalyzer().getWarnings().get(0)
            .startsWith("Table hint not recognized for table"));
  }

  @Test
  public void hdfsTableHintsBehaveAsBefore() throws Exception {
    table();
    feFixture_.addTestTable("create table " + fullName() + "_h (id int)");
    AnalysisResult res = analyze("select id from " + fullName()
        + "_h /* +SCHEDULE_REMOTE */");
    assertTrue(res.getAnalyzer().getWarnings().isEmpty());
    assertNull(onlyRef(res).getIcebergAddedFilesAfter());
    res = analyze("select id from " + fullName() + "_h /* +NO_SUCH_HINT */");
    assertEquals(1, res.getAnalyzer().getWarnings().size());
  }

  @Test
  public void countStarWithHintIsNotOptimized() throws Exception {
    long[] s = table();
    AnalysisResult res = analyze("select count(*) from " + fullName()
        + " /* +ICEBERG_ADDED_FILES_AFTER(" + s[0] + ") */");
    assertEquals(Long.valueOf(s[0]), onlyRef(res).getIcebergAddedFilesAfter());
  }

  @Test
  public void countStarWithoutHintIsOptimized() throws Exception {
    table();
    AnalysisResult res = analyze("select count(*) from " + fullName());
    assertTrue(((SelectStmt) res.getStmt()).getTableRefs().isEmpty());
  }

  private IcebergTestTables create(Schema schema, PartitionSpec spec, int formatVersion) {
    return IcebergTestTables.create(tmp, DB, testName.getMethodName().toLowerCase(),
        schema, spec, formatVersion);
  }

  private String scanSql(long base, Long asOf) {
    return "select id from " + fullName()
        + (asOf == null ? "" : " for system_version as of " + asOf)
        + " /* +ICEBERG_ADDED_FILES_AFTER(" + base + ") */";
  }

  /** Sorted locations of the data and delete files read by the scan nodes of 'res'. */
  static List<String> scannedPaths(AnalysisResult res) throws ImpalaException {
    // Set by the coordinator for real queries; the planner requires it.
    res.getAnalyzer().getQueryOptions().setMt_dop(0);
    Planner planner = new Planner(res, res.getAnalyzer().getQueryCtx(),
        new EventSequence("plan"));
    List<String> paths = new ArrayList<>();
    for (PlanFragment fragment : planner.createPlans()) {
      collectPaths(fragment.getPlanRoot(), paths);
    }
    Collections.sort(paths);
    return paths;
  }

  private static void collectPaths(PlanNode node, List<String> paths) {
    if (node instanceof IcebergScanNode) {
      IcebergScanNode scan = (IcebergScanNode) node;
      String location = ((FeFsTable) scan.getTupleDesc().getTable()).getLocation();
      for (FileDescriptor fd : scan.getFileDescriptorsWithLimit(null, false, -1)) {
        paths.add(fd.getAbsolutePath(location));
      }
    }
    for (PlanNode child : node.getChildren()) collectPaths(child, paths);
  }

  static List<String> locations(ContentFile<?>... files) {
    TreeSet<String> paths = new TreeSet<>();
    for (ContentFile<?> f : files) paths.add(f.location());
    return new ArrayList<>(paths);
  }

  private static List<String> sorted(Iterable<String> paths) {
    TreeSet<String> sorted = new TreeSet<>();
    for (String p : paths) sorted.add(p);
    return new ArrayList<>(sorted);
  }

  /**
   * v2 table with partitions p=1 and p=2. Position and equality deletes of both
   * partitions are committed after 'gap'; 'late' comes after them.
   */
  private static final class DeletesTable {
    IcebergTestTables t;
    DataFile base1, base2, gap, late;
    DeleteFile pos1, pos2, eq1, eq2;
    long s0, s1, s2, s3;
  }

  private DeletesTable deletesTable() throws Exception {
    DeletesTable d = new DeletesTable();
    d.t = create(PART_SCHEMA, PartitionSpec.builderFor(PART_SCHEMA).identity("p").build(),
        2);
    d.base1 = d.t.file(100, "p=1");
    d.base2 = d.t.file(100, "p=2");
    d.s0 = d.t.append(d.base1, d.base2);
    d.gap = d.t.file(100, "p=2");
    d.s1 = d.t.append(d.gap);
    d.pos1 = d.t.positionDeletes(10, 1, "p=1");
    d.pos2 = d.t.positionDeletes(10, 1, "p=2");
    d.eq1 = d.t.equalityDeletes(10, 1, "p=1", 1);
    d.eq2 = d.t.equalityDeletes(10, 1, "p=2", 1);
    d.s2 = d.t.rowDelta(d.pos1, d.pos2, d.eq1, d.eq2);
    d.late = d.t.file(100, "p=1");
    d.s3 = d.t.append(d.late);
    ifx.addPathB(d.t, Collections.emptyMap());
    return d;
  }

  @Test
  public void scanReadsGapFilesWithTheirDeletesOnly() throws Exception {
    DeletesTable d = deletesTable();
    List<String> scanned = scannedPaths(analyze(scanSql(d.s0, d.s3)));
    assertEquals(locations(d.gap, d.late, d.pos2, d.eq2), scanned);
    // Same data files as the gap seen by the analysis of the parent statement.
    List<String> gapFiles = new ArrayList<>(
        IcebergAddedFiles.compute(d.t.table(), d.s0, d.s3).paths);
    gapFiles.addAll(locations(d.pos2, d.eq2));
    assertEquals(sorted(gapFiles), scanned);
    // The scan snapshot bounds the gap.
    assertEquals(locations(d.gap), scannedPaths(analyze(scanSql(d.s0, d.s1))));
  }

  @Test
  public void hintWithoutTimeTravelUsesCurrentSnapshot() throws Exception {
    DeletesTable d = deletesTable();
    assertEquals(locations(d.late), scannedPaths(analyze(scanSql(d.s1, null))));
    assertEquals(locations(d.gap, d.late, d.pos2, d.eq2),
        scannedPaths(analyze(scanSql(d.s0, null))));
  }

  @Test
  public void scanWithoutHintIsUnchanged() throws Exception {
    DeletesTable d = deletesTable();
    assertEquals(locations(d.base1, d.base2, d.gap, d.late, d.pos1, d.pos2, d.eq1, d.eq2),
        scannedPaths(analyze("select id from " + fullName())));
    assertEquals(locations(d.base1, d.base2, d.gap),
        scannedPaths(analyze("select id from " + fullName()
            + " for system_version as of " + d.s1)));
  }

  @Test
  public void emptyGapScansNoFiles() throws Exception {
    DeletesTable d = deletesTable();
    // A gap of delete files only adds no data file.
    assertEquals(Collections.emptyList(), scannedPaths(analyze(scanSql(d.s1, d.s2))));
    assertEquals(Collections.emptyList(), scannedPaths(analyze(scanSql(d.s3, d.s3))));
    assertEquals(Collections.emptyList(), scannedPaths(analyze("select count(*) from "
        + fullName() + " for system_version as of " + d.s3
        + " /* +ICEBERG_ADDED_FILES_AFTER(" + d.s3 + ") */")));
  }

  @Test
  public void reanalyzedAndClonedStatementsScanSameGapFiles() throws Exception {
    DeletesTable d = deletesTable();
    List<String> expected = locations(d.gap, d.late, d.pos2, d.eq2);
    // Constant folding resets the statement and analyzes it again.
    String sql = "select id, 1 + 1 from " + fullName() + " for system_version as of "
        + d.s3 + " /* +ICEBERG_ADDED_FILES_AFTER(" + d.s0 + ") */";
    assertEquals(expected, scannedPaths(analyze(sql)));
    // The clone shares the analyzer, which takes one plan of the table only.
    AnalysisResult res = analyze(sql);
    SelectStmt clone = (SelectStmt) res.getStmt().clone();
    TableRef clonedRef = clone.getTableRefs().get(0);
    Planner planner = new Planner(res, res.getAnalyzer().getQueryCtx(),
        new EventSequence("plan"));
    PlanNode scan = new IcebergScanPlanner(res.getAnalyzer(), planner.getPlannerCtx(),
        clonedRef, new ArrayList<>(), null).createIcebergScanPlan();
    List<String> paths = new ArrayList<>();
    collectPaths(scan, paths);
    Collections.sort(paths);
    assertEquals(expected, paths);
  }

  @Test
  public void expiredBaseIsAnalysisError() throws Exception {
    IcebergTestTables t = create(SCHEMA, PartitionSpec.unpartitioned(), 2);
    long s0 = t.append(t.file(100, null));
    t.append(t.file(100, null));
    long s2 = t.append(t.file(100, null));
    t.expire(s0);
    ifx.addPathB(t, Collections.emptyMap());
    planningError(scanSql(s0, s2), AnalysisException.class, "Snapshot " + s0
        + " is not an ancestor of snapshot " + s2 + " of table " + fullName());
  }

  @Test
  public void snapshotWithoutManifestListIsAnalysisError() throws Exception {
    IcebergTestTables t = create(SCHEMA, PartitionSpec.unpartitioned(), 1);
    long s0 = t.append(t.file(100, null));
    long noList = t.append(t.file(100, null));
    long s2 = t.append(t.file(100, null));
    t.removeManifestList(noList);
    ifx.addPathB(t, Collections.emptyMap());
    planningError(scanSql(s0, s2), AnalysisException.class,
        "snapshot " + noList + " has no manifest list or operation");
  }

  @Test
  public void manifestReadErrorIsRuntimeError() throws Exception {
    IcebergTestTables t = create(SCHEMA, PartitionSpec.unpartitioned(), 2);
    long s0 = t.append(t.file(100, null));
    long s1 = t.append(t.file(100, null));
    long s2 = t.append(t.file(100, null));
    ifx.addPathB(t, Collections.emptyMap());
    // Only the gap reads the manifest list of s1; planning s2 does not need it.
    File manifestList =
        new File(new Path(t.table().snapshot(s1).manifestListLocation()).toUri());
    assertTrue(manifestList.delete());
    Throwable e = planningError(scanSql(s0, s2), ImpalaRuntimeException.class,
        "Failed to load data files for Iceberg table: " + fullName());
    assertNotNull(e.getCause());
  }

  private Throwable planningError(String sql, Class<? extends ImpalaException> type,
      String expected) throws Exception {
    try {
      scannedPaths(analyze(sql));
    } catch (ImpalaException e) {
      assertEquals(type, e.getClass());
      assertEquals(expected, e.getMessage());
      return e;
    }
    fail("no error for: " + sql);
    return null;
  }

  @Test
  public void maskingViewScansGapFilesOnly() throws Exception {
    IcebergTestTables t = create(SCHEMA, PartitionSpec.unpartitioned(), 2);
    long b = t.append(t.file(100, null));
    DataFile gap = t.file(100, null);
    long s1 = t.append(gap);
    ifx.addPathB(t, Collections.emptyMap());
    AnalysisContext ctx = createAnalysisCtx(maskingAuthz(createAuthorizationFactory()));
    AnalysisResult res = parseAndAnalyze(scanSql(b, s1), ctx);
    TableRef view = onlyRef(res);
    assertTrue(view.isTableMaskingView());
    assertNull(view.getIcebergAddedFilesAfter());
    assertTrue(view.getTableHints().toString(), view.getTableHints().isEmpty());
    SelectStmt viewStmt = (SelectStmt) ((InlineViewRef) view).getViewStmt();
    assertTrue(viewStmt.toSql(), viewStmt.toSql().contains("id * 2"));
    assertTrue(viewStmt.toSql(), viewStmt.hasWhereClause());
    assertEquals(Long.valueOf(b),
        viewStmt.getTableRefs().get(0).getIcebergAddedFilesAfter());
    assertEquals(locations(gap), scannedPaths(res));
  }

  /** Authorization that masks column id and filters the rows of every table. */
  private static AuthorizationFactory maskingAuthz(AuthorizationFactory base) {
    return (AuthorizationFactory) Proxy.newProxyInstance(
        AuthorizationFactory.class.getClassLoader(),
        new Class<?>[] {AuthorizationFactory.class}, (proxy, method, args) -> {
          if (method.getName().equals("supportsTableMasking")) return true;
          Object result = invoke(base, method, args);
          if (!(result instanceof AuthorizationChecker)) return result;
          AuthorizationChecker checker = (AuthorizationChecker) result;
          return Proxy.newProxyInstance(AuthorizationChecker.class.getClassLoader(),
              new Class<?>[] {AuthorizationChecker.class}, (p, m, a) -> {
                switch (m.getName()) {
                  case "needsMaskingOrFiltering":
                  case "needsRowFiltering": return true;
                  case "createColumnMask": return "id".equals(a[3]) ? "id * 2" : null;
                  case "createRowFilter": return "id > 0";
                  default: return invoke(checker, m, a);
                }
              });
        });
  }

  private static Object invoke(Object target, Method method,
      Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }
}
