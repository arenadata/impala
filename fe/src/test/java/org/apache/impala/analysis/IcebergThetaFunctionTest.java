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

package org.apache.impala.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.impala.catalog.AggregateFunction;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.FrontendTestBase;
import org.junit.Before;
import org.junit.Test;

/**
 * Checks the signatures of ds_theta_sketch_iceberg(): one overload per Iceberg column
 * type with its own Iceberg-encoding update function (BIGINT and STRING share the
 * ds_theta_sketch() ones), a STRING result, and no overload for BINARY. Also pins the
 * user-facing ds_theta_* functions, which must keep their signatures.
 */
public class IcebergThetaFunctionTest extends FrontendTestBase {
  private static final String DB = "iceberg_theta_fn";
  private static final String TBL = DB + ".t";
  private static final String ICEBERG_FN = "ds_theta_sketch_iceberg";
  private static final String ICEBERG_UPDATE = "20DsThetaUpdateIcebergIN10impala_udf";
  private static final String THETA_UPDATE = "13DsThetaUpdateIN10impala_udf";
  private static final String NO_MATCH = "No matching function with signature: ";

  @Before
  public void addTable() {
    feFixture_.addTestDb(DB, null);
    feFixture_.addTestTable("create table " + TBL + " (bo boolean, ti tinyint, "
        + "si smallint, i int, bi bigint, f float, d double, s string, c char(5), "
        + "vc varchar(10), dt date, ts timestamp, d4 decimal(9,2), d8 decimal(18,3), "
        + "d16 decimal(38,5), bin binary)");
  }

  /** Analyzes "select fn(col) from TBL" and returns its only aggregate expression. */
  private FunctionCallExpr aggregate(String fn, String col) {
    SelectStmt stmt = (SelectStmt) AnalyzesOk(
        "select " + fn + "(" + col + ") from " + TBL);
    assertEquals(Type.STRING, stmt.getResultExprs().get(0).getType());
    List<FunctionCallExpr> aggs = stmt.getMultiAggInfo().getAggExprs();
    assertEquals(1, aggs.size());
    return aggs.get(0);
  }

  /** AggregateFunction keeps no intermediate type when it equals the return type. */
  private static Type intermediateType(AggregateFunction fn) {
    return fn.getIntermediateType() == null ?
        fn.getReturnType() : fn.getIntermediateType();
  }

  private AggregateFunction icebergFn(String col, Type argType) {
    FunctionCallExpr expr = aggregate(ICEBERG_FN, col);
    AggregateFunction fn = (AggregateFunction) expr.getFn();
    assertEquals(col, ICEBERG_FN, fn.getFunctionName().getFunction());
    assertEquals(col, Type.STRING, fn.getReturnType());
    assertEquals(col, Type.STRING, intermediateType(fn));
    assertEquals(col, 1, fn.getArgs().length);
    if (argType.isDecimal()) {
      assertTrue(col, fn.getArgs()[0].isWildcardDecimal());
    } else {
      assertEquals(col, argType, fn.getArgs()[0]);
    }
    assertEquals(col, Type.STRING, expr.getType());
    return fn;
  }

  /** Asserts that 'col' resolves to the Iceberg-encoding update function of 'udfVal'. */
  private void assertIcebergUpdate(String col, Type argType, String udfVal) {
    AggregateFunction fn = icebergFn(col, argType);
    String update = fn.getUpdateFnSymbol();
    assertTrue(col + ": " + update,
        update.contains(ICEBERG_UPDATE + udfVal.length() + udfVal + "EE"));
    assertTrue(col + ": " + fn.getInitFnSymbol(),
        fn.getInitFnSymbol().contains("11DsThetaInit"));
    assertTrue(col + ": " + fn.getMergeFnSymbol(),
        fn.getMergeFnSymbol().contains("12DsThetaMerge"));
    assertTrue(col + ": " + fn.getSerializeFnSymbol(),
        fn.getSerializeFnSymbol().contains("16DsThetaSerialize"));
    assertTrue(col + ": " + fn.getFinalizeFnSymbol(),
        fn.getFinalizeFnSymbol().contains("21DsThetaFinalizeSketch"));
  }

  /** Asserts that 'col' reuses the update function of ds_theta_sketch() on the column. */
  private void assertSharedUpdate(String col, Type argType) {
    AggregateFunction fn = icebergFn(col, argType);
    AggregateFunction theta = (AggregateFunction) aggregate("ds_theta_sketch", col)
        .getFn();
    assertEquals(col, theta.getUpdateFnSymbol(), fn.getUpdateFnSymbol());
    assertFalse(col, fn.getUpdateFnSymbol().contains("Iceberg"));
  }

  @Test
  public void eachIcebergTypeHasOwnUpdate() {
    assertIcebergUpdate("bo", Type.BOOLEAN, "BooleanVal");
    assertIcebergUpdate("i", Type.INT, "IntVal");
    assertIcebergUpdate("f", Type.FLOAT, "FloatVal");
    assertIcebergUpdate("d", Type.DOUBLE, "DoubleVal");
    assertIcebergUpdate("dt", Type.DATE, "DateVal");
    assertIcebergUpdate("ts", Type.TIMESTAMP, "TimestampVal");
    for (String col : new String[] {"d4", "d8", "d16"}) {
      assertIcebergUpdate(col, Type.DECIMAL, "DecimalVal");
    }
  }

  @Test
  public void bigintAndStringShareThetaUpdate() {
    assertSharedUpdate("bi", Type.BIGINT);
    assertSharedUpdate("s", Type.STRING);
  }

  @Test
  public void narrowTypesResolveToWiderSignature() {
    for (String col : new String[] {"ti", "si"}) {
      assertIcebergUpdate(col, Type.INT, "IntVal");
    }
    for (String col : new String[] {"c", "vc"}) {
      assertSharedUpdate(col, Type.STRING);
    }
  }

  @Test
  public void binaryHasNoSignature() {
    AnalysisError("select " + ICEBERG_FN + "(bin) from " + TBL,
        NO_MATCH + ICEBERG_FN + "(BINARY).");
  }

  @Test
  public void icebergSketchFeedsThetaEstimate() {
    SelectStmt stmt = (SelectStmt) AnalyzesOk("select ds_theta_estimate(" + ICEBERG_FN
        + "(i)), ds_theta_union_f(" + ICEBERG_FN + "(s), " + ICEBERG_FN + "(dt)) from "
        + TBL);
    assertEquals(Type.BIGINT, stmt.getResultExprs().get(0).getType());
    assertEquals(Type.STRING, stmt.getResultExprs().get(1).getType());
  }

  @Test
  public void thetaSketchSignaturesUnchanged() {
    String[][] supported = {{"ti", "TinyIntVal"}, {"i", "IntVal"}, {"bi", "BigIntVal"},
        {"f", "FloatVal"}, {"d", "DoubleVal"}, {"s", "StringVal"}};
    for (String[] colAndVal : supported) {
      String col = colAndVal[0];
      AggregateFunction fn = (AggregateFunction) aggregate("ds_theta_sketch", col)
          .getFn();
      assertEquals(col, Type.STRING, fn.getReturnType());
      assertEquals(col, Type.STRING, intermediateType(fn));
      String update = fn.getUpdateFnSymbol();
      assertTrue(col + ": " + update, update.contains(
          THETA_UPDATE + colAndVal[1].length() + colAndVal[1] + "EE"));
    }
    String[][] unsupported = {{"bo", "BOOLEAN"}, {"dt", "DATE"},
        {"ts", "TIMESTAMP"}, {"d4", "DECIMAL(9,2)"}, {"bin", "BINARY"}};
    for (String[] colAndType : unsupported) {
      AnalysisError("select ds_theta_sketch(" + colAndType[0] + ") from " + TBL,
          NO_MATCH + "ds_theta_sketch(" + colAndType[1] + ").");
    }
  }

  @Test
  public void thetaScalarSignaturesUnchanged() {
    SelectStmt stmt = (SelectStmt) AnalyzesOk(
        "select ds_theta_estimate(s), ds_theta_union_f(s, vc) from " + TBL);
    assertScalar(stmt.getResultExprs().get(0), "ds_theta_estimate", Type.BIGINT,
        Type.STRING);
    assertScalar(stmt.getResultExprs().get(1), "ds_theta_union_f", Type.STRING,
        Type.STRING, Type.STRING);
  }

  private static void assertScalar(Expr expr, String name, Type ret, Type... args) {
    Function fn = expr.getFn();
    assertEquals(name, fn.getFunctionName().getFunction());
    assertEquals(name, ret, expr.getType());
    assertEquals(name, ret, fn.getReturnType());
    assertEquals(name, args.length, fn.getArgs().length);
    for (int i = 0; i < args.length; ++i) {
      assertEquals(name, args[i], fn.getArgs()[i]);
    }
  }
}
