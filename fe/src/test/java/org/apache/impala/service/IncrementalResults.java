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

package org.apache.impala.service;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.analysis.ComputeStatsStmt;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.SelectListItem;
import org.apache.impala.analysis.SelectStmt;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.iceberg.IcebergTestTables;
import org.apache.impala.catalog.iceberg.PuffinFixtures;
import org.apache.impala.common.FrontendFixture;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.IcebergScanNode;
import org.apache.impala.planner.PlanFragment;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.Planner;
import org.apache.impala.thrift.TAlterTableUpdateStatsParams;
import org.apache.impala.thrift.TColumnStats;
import org.apache.impala.thrift.TColumnValue;
import org.apache.impala.thrift.TComputeStatsParams;
import org.apache.impala.thrift.TIcebergPuffinStatsParams;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.util.EventSequence;

/**
 * Stands in for the backend running the child queries of an analyzed COMPUTE STATS on
 * an IcebergTestTables table. The queries are planned, not run: the data files of their
 * scan nodes and the values the test declared for these files give the result rows,
 * which become the update request the way CatalogOpExecutor::ExecComputeStats builds it.
 * The row count is that of the live data files of the calculated snapshot minus the
 * record counts of their delete files. Scans of delete files are not supported.
 */
final class IncrementalResults {
  /** Expressions per column in the column stats query. */
  private static final int EXPRS_PER_COLUMN = 8;

  private final TAlterTableUpdateStatsParams params_;
  private final List<String> metricFiles_;
  private final List<String> thetaFiles_;

  private IncrementalResults(TAlterTableUpdateStatsParams params,
      List<String> metricFiles, List<String> thetaFiles) {
    params_ = params;
    metricFiles_ = metricFiles;
    thetaFiles_ = thetaFiles;
  }

  TAlterTableUpdateStatsParams params() { return params_; }

  /** Sorted data files scanned by the column stats query; empty without that query. */
  List<String> metricFiles() { return metricFiles_; }

  /** Sorted data files scanned by the separate theta query; empty without it. */
  List<String> thetaFiles() { return thetaFiles_; }

  static IncrementalResults run(FrontendFixture fx, ComputeStatsStmt stmt,
      IcebergTestTables t) throws ImpalaException {
    TComputeStatsParams compute = stmt.toThrift();
    Preconditions.checkArgument(!compute.isSetNo_child_queries_message(),
        "COMPUTE STATS without child queries: %s",
        compute.getNo_child_queries_message());
    Table api = t.table();
    long s1 = compute.getIceberg_puffin_stats().getCalc_snapshot_id();
    Set<String> deleteFiles = new HashSet<>();

    TAlterTableUpdateStatsParams params = new TAlterTableUpdateStatsParams();
    params.setTable_name(compute.getTable_name());
    params.setExpect_all_partitions(compute.isExpect_all_partitions());
    params.setIs_incremental(compute.isIs_incremental());
    TTableStats tableStats = new TTableStats(liveRows(api, s1, deleteFiles));
    if (compute.isSetTotal_file_bytes()) {
      tableStats.setTotal_file_bytes(compute.getTotal_file_bytes());
    }
    params.setTable_stats(tableStats);
    TIcebergPuffinStatsParams puffin = null;
    if (compute.isSetIceberg_puffin_stats()) {
      puffin = compute.getIceberg_puffin_stats().deepCopy();
      params.setIceberg_puffin_stats(puffin);
    }

    List<String> metricFiles = Collections.emptyList();
    if (compute.isSetCol_stats_query()) {
      AnalysisResult res = analyze(fx, compute.getCol_stats_query());
      metricFiles = scannedDataFiles(res, deleteFiles);
      SelectStmt select = (SelectStmt) res.getStmt();
      // Result exprs refer to the aggregation output; the select list keeps the calls.
      List<Expr> exprs = new ArrayList<>();
      for (SelectListItem item : select.getSelectList().getItems()) {
        exprs.add(item.getExpr());
      }
      List<String> labels = select.getColLabels();
      FeTable tbl = select.getTableRefs().get(0).getTable();
      int thetaCount = 0;
      if (puffin != null && puffin.isSetTheta_field_ids()
          && !compute.isSetIceberg_theta_query()) {
        thetaCount = puffin.getTheta_field_ids().size();
      }
      int metricsEnd = exprs.size() - thetaCount;
      Preconditions.checkState(metricsEnd % EXPRS_PER_COLUMN == 0,
          "unexpected column stats query: %s", compute.getCol_stats_query());
      for (int i = 0; i < metricsEnd; i += EXPRS_PER_COLUMN) {
        Column col = tbl.getColumn(labels.get(i));
        Preconditions.checkState(col != null, "no column %s", labels.get(i));
        String name = labels.get(i);
        if (puffin != null) {
          Integer fieldId = puffin.getColumn_field_ids().get(labels.get(i));
          Preconditions.checkState(fieldId != null, "no field id of %s", name);
          name = api.schema().findField(fieldId).name();
        }
        List<Object> values = values(t, metricFiles, name);
        params.putToColumn_stats(labels.get(i),
            columnStats(col.getType(), values, exprs.subList(i, i + EXPRS_PER_COLUMN)));
      }
      if (thetaCount > 0) putSketches(t, api, puffin, metricFiles);
    }

    List<String> thetaFiles = Collections.emptyList();
    if (compute.isSetIceberg_theta_query()) {
      thetaFiles = scannedDataFiles(analyze(fx, compute.getIceberg_theta_query()),
          deleteFiles);
      putSketches(t, api, puffin, thetaFiles);
    }
    return new IncrementalResults(params, metricFiles, thetaFiles);
  }

  /** Live rows of 's1'; collects the locations of its delete files. */
  private static long liveRows(Table api, long s1, Set<String> deleteFiles) {
    long rows = 0;
    try (CloseableIterable<FileScanTask> tasks =
        api.newScan().useSnapshot(s1).planFiles()) {
      for (FileScanTask task : tasks) {
        rows += task.file().recordCount();
        for (DeleteFile d : task.deletes()) {
          if (deleteFiles.add(d.location())) rows -= d.recordCount();
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return rows;
  }

  private static AnalysisResult analyze(FrontendFixture fx, String sql)
      throws ImpalaException {
    AnalysisResult res = fx.parseAndAnalyze(sql, fx.createAnalysisCtx());
    // Set by the coordinator for real queries; the planner requires it.
    res.getAnalyzer().getQueryOptions().setMt_dop(0);
    return res;
  }

  private static List<String> scannedDataFiles(AnalysisResult res,
      Set<String> deleteFiles) throws ImpalaException {
    Planner planner = new Planner(res, res.getAnalyzer().getQueryCtx(),
        new EventSequence("plan"));
    TreeSet<String> paths = new TreeSet<>();
    for (PlanFragment fragment : planner.createPlans()) {
      collectPaths(fragment.getPlanRoot(), paths);
    }
    for (String p : paths) {
      Preconditions.checkArgument(!deleteFiles.contains(p),
          "scans of delete files are not supported: %s", p);
    }
    return new ArrayList<>(paths);
  }

  private static void collectPaths(PlanNode node, Set<String> paths) {
    if (node instanceof IcebergScanNode) {
      IcebergScanNode scan = (IcebergScanNode) node;
      String location = ((FeFsTable) scan.getTupleDesc().getTable()).getLocation();
      for (FileDescriptor fd : fileDescriptors(scan)) {
        Preconditions.checkState(paths.add(fd.getAbsolutePath(location)),
            "file %s scanned twice", fd.getAbsolutePath(location));
      }
    }
    for (PlanNode child : node.getChildren()) collectPaths(child, paths);
  }

  /** The planner keeps the files of a scan node in a protected method only. */
  private static List<FileDescriptor> fileDescriptors(IcebergScanNode scan) {
    List<?> fds;
    try {
      Method m = IcebergScanNode.class.getDeclaredMethod("getFileDescriptorsWithLimit",
          FeFsPartition.class, boolean.class, long.class);
      m.setAccessible(true);
      fds = (List<?>) m.invoke(scan, null, false, -1L);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
    List<FileDescriptor> res = new ArrayList<>();
    for (Object fd : fds) res.add((FileDescriptor) fd);
    return res;
  }

  private static List<Object> values(IcebergTestTables t, List<String> files,
      String column) {
    List<Object> res = new ArrayList<>();
    for (String f : files) res.addAll(t.columnValues(f, column));
    return res;
  }

  private static void putSketches(IcebergTestTables t, Table api,
      TIcebergPuffinStatsParams puffin, List<String> files) {
    Preconditions.checkState(puffin != null && puffin.isSetTheta_field_ids(),
        "theta sketches without theta field ids");
    for (int fieldId : puffin.getTheta_field_ids()) {
      Types.NestedField field = api.schema().findField(fieldId);
      Preconditions.checkState(field != null, "no field %s", fieldId);
      byte[] sketch =
          PuffinFixtures.sketch(field.type(), values(t, files, field.name())).toByteArray();
      puffin.putToTheta_sketches(fieldId, ByteBuffer.wrap(sketch));
    }
  }

  /**
   * Result row of the eight metric expressions 'exprs' of a column with 'values',
   * decoded as the backend does: it reads a fixed field of the result value, so NULL and
   * values of another type (the TINYINT slot size literal) become 0.
   */
  private static TColumnStats columnStats(Type type, List<Object> values,
      List<Expr> exprs) {
    long count = 0;
    long maxLength = 0;
    long totalLength = 0;
    long trues = 0;
    long falses = 0;
    Object min = null;
    Object max = null;
    Set<Object> distinct = new HashSet<>();
    for (Object v : values) {
      if (v == null) continue;
      ++count;
      distinct.add(v);
      if (v instanceof CharSequence) {
        int length = v.toString().getBytes(StandardCharsets.UTF_8).length;
        maxLength = Math.max(maxLength, length);
        totalLength += length;
      }
      if (Boolean.TRUE.equals(v)) ++trues;
      if (Boolean.FALSE.equals(v)) ++falses;
      if (v instanceof Comparable) {
        if (min == null || compare(v, min) < 0) min = v;
        if (max == null || compare(v, max) > 0) max = v;
      }
    }
    TColumnStats res = new TColumnStats();
    // COUNT outside LEGACY; NDV() in LEGACY, exact on small sets.
    boolean valueCount = exprs.get(0) instanceof FunctionCallExpr
        && ((FunctionCallExpr) exprs.get(0)).getFnName().getFunction()
            .equalsIgnoreCase("count");
    res.setNum_distinct_values(valueCount ? count : distinct.size());
    res.setNum_nulls(values.size() - count);
    // MAX(length()) is INT, the slot size of other types is a TINYINT literal.
    boolean lengthMax = exprs.get(2).getType().isScalarType(PrimitiveType.INT);
    res.setMax_size(lengthMax ? maxLength : 0);
    if (type.isStringType()) {
      res.setAvg_size(count == 0 ? 0 : (double) totalLength / count);
    } else {
      res.setAvg_size(type.getPrimitiveType().getSlotSize());
    }
    // Columns other than BOOLEAN have NULL literals here.
    res.setNum_trues(exprs.get(4).getType().isNull() ? 0 : trues);
    res.setNum_falses(exprs.get(5).getType().isNull() ? 0 : falses);
    if (!exprs.get(6).getType().isNull() && min != null) {
      res.setLow_value(columnValue(type, min));
    }
    if (!exprs.get(7).getType().isNull() && max != null) {
      res.setHigh_value(columnValue(type, max));
    }
    return res;
  }

  // The values of one column are of one Java type.
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static int compare(Object a, Object b) {
    return ((Comparable) a).compareTo(b);
  }

  /** Min or max as ConvertToTColumnValue makes it of the HiveServer2 result value. */
  private static TColumnValue columnValue(Type type, Object v) {
    TColumnValue res = new TColumnValue();
    PrimitiveType p = type.getPrimitiveType();
    switch (p) {
      case TINYINT: res.setByte_val(((Number) v).byteValue()); break;
      case SMALLINT: res.setShort_val(((Number) v).shortValue()); break;
      case INT: res.setInt_val(((Number) v).intValue()); break;
      case BIGINT: res.setLong_val(((Number) v).longValue()); break;
      case FLOAT:
      case DOUBLE: res.setDouble_val(((Number) v).doubleValue()); break;
      case DATE: res.setDate_val(((Number) v).intValue()); break;
      case DECIMAL:
        // The backend prints the value with the scale of the type.
        String ascii = ((BigDecimal) v)
            .setScale(((ScalarType) type).decimalScale(), RoundingMode.UNNECESSARY)
            .toPlainString();
        res.setDecimal_val(ascii.getBytes(StandardCharsets.US_ASCII));
        break;
      default:
        throw new IllegalArgumentException("no min/max of type " + type);
    }
    return res;
  }
}
