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

package org.apache.impala.tsql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Test;

/** Regressions from the second review round. Each one is a behaviour, not a dump. */
public class TsqlReviewTest {
  @Test
  public void beginEndDoesNotSwallowErrors() {
    String src = "BEGIN\nINSERT INTO t VALUES (1);\nINSERT INTO t VALUES (2);\nEND\nSELECT 1 AS ok;\n";
    Recording fail = new Recording();
    fail.failSqlContaining = "VALUES (1)";
    try {
      run(src, fail);
      fail("block continued");
    } catch (AssertionError e) {
      assertTrue(e.getMessage(), e.getMessage().contains("boom"));
    }
    assertEquals(1, fail.sql.size());
  }

  @Test
  public void returnStopsOnlyTheCallee() {
    TsqlRuntime.MemoryStore store = define(
        "CREATE PROCEDURE inner_p AS BEGIN RETURN;\nSELECT 1 AS leaked;\nEND\n");
    Recording runner = new Recording();
    TsqlRuntime.Step step = run("DECLARE @n INT = 1;\nEXEC inner_p;\nSELECT @n AS n;\n", runner, store);
    assertTrue(step.sql, step.sql.contains("1"));
    assertFalse(String.join("\n", runner.sql).contains("leaked"));
  }

  @Test
  public void calleeDoesNotClobberCallerVariablesOrLoops() {
    TsqlRuntime.MemoryStore store = define(
        "CREATE PROCEDURE inner_p AS BEGIN\nDECLARE @n INT = 9;\nSET @n = 8;\nEND\n");
    Recording runner = new Recording();
    TsqlRuntime.Step step = run(
        "DECLARE @n INT = 1;\nEXEC inner_p;\nSELECT @n AS n;\n", runner, store);
    assertTrue(step.sql, step.sql.contains("1"));
  }

  @Test
  public void outputAndReturnCode() {
    TsqlRuntime.MemoryStore store = define(
        "CREATE PROCEDURE add_one @x INT, @y INT OUTPUT AS BEGIN\n"
            + "SET @y = @x + 1;\nRETURN 7;\nEND\n");
    Recording runner = new Recording();
    TsqlRuntime.Step step = run(
        "DECLARE @out INT;\nDECLARE @rc INT;\nEXEC @rc = add_one @x = 3, @y = @out OUTPUT;\n"
            + "SELECT @rc AS rc, @out AS out;\n",
        runner, store);
    assertTrue(step.sql, step.sql.contains("7"));
    assertTrue(step.sql, step.sql.contains("4"));
  }

  @Test
  public void qualifiedNameAndDefaultAreNotExecutedAtCreate() {
    Recording runner = new Recording();
    TsqlRuntime.Step step = run(
        "CREATE PROCEDURE dbo.p @a INT = 5 AS BEGIN INSERT INTO t VALUES (@a); END\n", runner);
    assertEquals(TsqlRuntime.Kind.FINISHED, step.kind);
    assertTrue(runner.sql.isEmpty());
  }

  @Test
  public void ddlAndCaseStayIntact() {
    assertTrue(sql("UPDATE t SET a = 1;\nSELECT 1 AS r;\n").contains("SET a = 1"));
    assertTrue(sql("DROP TABLE IF EXISTS t;\nSELECT 1 AS r;\n").startsWith("DROP TABLE IF EXISTS"));
    assertTrue(sql("CREATE TABLE IF NOT EXISTS x (a INT);\nSELECT 1 AS r;\n")
        .contains("IF NOT EXISTS"));
    TsqlRuntime.Step caseStep = run("SELECT CASE WHEN 1 THEN 2 ELSE 3 END AS n;\n", new Recording());
    assertTrue(caseStep.sql, caseStep.sql.contains("ELSE") && caseStep.sql.contains("END"));
  }

  @Test
  public void backslashIsOrdinaryInSource() {
    String inserted = sql("INSERT INTO t VALUES ('C:\\data\\');\nSELECT 1 AS r;\n");
    assertTrue(inserted, inserted.contains("'C:\\\\data\\\\'"));
    String tab = sql("INSERT INTO t VALUES ('\\t');\nSELECT 1 AS r;\n");
    assertTrue(tab, tab.contains("'\\\\t'"));
  }

  @Test
  public void undeclaredVariableIsAnError() {
    try {
      run("SELECT @idd AS id;\n", new Recording());
      fail("undeclared variable ran");
    } catch (AssertionError e) {
      assertTrue(e.getMessage(), e.getMessage().toLowerCase().contains("declare"));
    }
  }

  @Test
  public void nullComparisonsAreUnknown() {
    TsqlRuntime.Step step = run(
        "DECLARE @x INT;\nDECLARE @r STRING = 'yes';\n"
            + "IF @x <> 1 SET @r = 'no';\nIF NOT @x = 1 SET @r = 'no';\nSELECT @r AS r;\n",
        new Recording());
    assertTrue(step.sql, step.sql.contains("'yes'"));
  }

  @Test
  public void errorVariableDoesNotHideTheSystemVariable() {
    TsqlRuntime.Step step = run(
        "DECLARE @error INT = 7;\nSELECT @error AS mine, @@ERROR AS sys;\n", new Recording());
    assertTrue(step.sql, step.sql.contains("7"));
    assertTrue(step.sql, step.sql.contains("0"));
  }

  @Test
  public void stepBudgetAndNestingFailClosed() {
    TsqlRuntime.Step loop = new TsqlRuntime("TSQL\nWHILE 1 = 1 BEGIN END\nSELECT 1 AS r;\n",
        "default", "t", new TsqlRuntime.MemoryStore(), TsqlAccess.OWNER_ONLY, 20, 32).next(null);
    assertEquals(TsqlRuntime.Kind.FAILED, loop.kind);
    assertTrue(loop.errorMessage, loop.errorMessage.contains("exceeded"));
    TsqlRuntime.MemoryStore store = define("CREATE PROCEDURE self_p AS BEGIN EXEC self_p; END\n");
    TsqlRuntime.Step failed = new TsqlRuntime("TSQL\nEXEC self_p;\n", "default", "t", store,
        TsqlAccess.OWNER_ONLY, 1000000L, 4).next(null);
    assertEquals(TsqlRuntime.Kind.FAILED, failed.kind);
    assertTrue(failed.errorMessage, failed.errorMessage.contains("nesting"));
  }

  @Test
  public void throwIsCaughtAndDropProcedureChecksTheOwner() {
    TsqlRuntime.Step caught = run(
        "BEGIN TRY THROW 50001, 'nope', 1; END TRY BEGIN CATCH END CATCH\nSELECT 1 AS ok;\n",
        new Recording());
    assertTrue(caught.sql, caught.sql.contains("1"));
    TsqlRuntime.MemoryStore store = define("CREATE PROCEDURE mine AS BEGIN SELECT 1 AS n; END\n");
    TsqlRuntime.Step denied = new TsqlRuntime("TSQL\nDROP PROCEDURE mine;\n", "default", "bob",
        store).next(null);
    assertEquals(TsqlRuntime.Kind.FAILED, denied.kind);
    TsqlRuntime.Step dropped = new TsqlRuntime("TSQL\nDROP PROCEDURE mine;\n", "default", "tester",
        store).next(null);
    assertEquals(dropped.errorMessage, TsqlRuntime.Kind.FINISHED, dropped.kind);
  }

  @Test
  public void arithmeticOperators() {
    TsqlRuntime.Step step = run(
        "DECLARE @x INT = 2 * 3;\nSET @x = @x / 2;\nSET @x = @x % 2;\nSELECT @x AS x;\n",
        new Recording());
    assertTrue(step.sql, step.sql.contains("1"));
  }

  private static TsqlRuntime.MemoryStore define(String create) {
    TsqlRuntime.MemoryStore store = new TsqlRuntime.MemoryStore();
    TsqlRuntime.Step step = new TsqlRuntime("TSQL\n" + create, "default", "tester", store).next(null);
    if (step.kind == TsqlRuntime.Kind.FAILED) throw new AssertionError(step.errorMessage);
    return store;
  }

  private static String sql(String src) {
    Recording runner = new Recording();
    run(src, runner);
    return runner.sql.isEmpty() ? "" : runner.sql.get(0);
  }

  private static TsqlRuntime.Step run(String src, Recording runner) {
    return run(src, runner, new TsqlRuntime.MemoryStore());
  }

  private static TsqlRuntime.Step run(String src, Recording runner, TsqlRuntime.MemoryStore store) {
    TsqlRuntime rt = new TsqlRuntime("TSQL\n" + src, "default", "tester", store);
    TsqlSqlRunner.Result prev = null;
    for (int i = 0; i < 10000; i++) {
      TsqlRuntime.Step step = rt.next(prev);
      if (step.kind == TsqlRuntime.Kind.FAILED) throw new AssertionError(step.errorMessage);
      if (step.kind == TsqlRuntime.Kind.YIELD) {
        prev = null;
        continue;
      }
      if (step.kind != TsqlRuntime.Kind.SQL) return step;
      prev = runner.execute(step.sql, step.procedureOptions);
    }
    throw new AssertionError("did not finish");
  }

  private static final class Recording implements TsqlSqlRunner {
    final List<String> sql = new ArrayList<String>();
    String failSqlContaining;

    public Result execute(String statement, Map<String, String> options) {
      sql.add(statement);
      if (failSqlContaining != null && statement.contains(failSqlContaining)) {
        return new Result(Collections.<String>emptyList(), Collections.<List<Cell>>emptyList(),
            -1, "boom");
      }
      return new Result(Collections.<String>emptyList(), Collections.<List<Cell>>emptyList(), 0, null);
    }
  }
}
