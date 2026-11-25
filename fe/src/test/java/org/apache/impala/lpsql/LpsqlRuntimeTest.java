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

package org.apache.impala.lpsql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.impala.tsql.TsqlParser;
import org.apache.impala.tsql.TsqlRuntime;
import org.apache.impala.tsql.TsqlSqlRunner;
import org.junit.Test;

/** Behavior of LPSQL lowered onto the shared runtime. */
public final class LpsqlRuntimeTest {
  private static int failed;

  @Test
  public void all() {
    main(new String[0]);
  }

  public static void main(String[] args) {
    rejectedByTsqlAcceptedByLpsql();
    demoProcRewritesCount();
    cursorFieldReadsName();
    fetchSqlcodeStopsTheLoop();
    concatBuildsShowColumnStats();
    exceptionSkipsOneFailure();
    storedLpsqlRoundTrips();
    storedTsqlStaysOnTsqlParser();
    bareSelectInTheMiddleFails();
    aliasKeepsTheVariableName();
    cursorLoopSumsEachRowOnce();
    if (failed > 0) {
      System.err.println(failed + " test(s) failed");
      throw new AssertionError(failed + " test(s) failed");
    }
    System.out.println("LpsqlRuntimeTest OK");
  }

  private static void rejectedByTsqlAcceptedByLpsql() {
    String[] samples = new String[] {
      "EXECUTE IMMEDIATE 'SELECT 1';",
      "FOR i IN 1..3 LOOP\nINSERT INTO t SELECT 1;\nEND LOOP;",
      "SELECT COUNT(*) INTO row_nums FROM t;",
      "LPSQL $$ BEGIN SELECT 1; END $$"
    };
    for (String src : samples) {
      try {
        TsqlParser.parse(src);
        fail("T-SQL parser accepted " + src);
      } catch (TsqlParser.ParseException expected) {
        // still rejected there
      }
      try {
        LpsqlParser.parse(src);
      } catch (TsqlParser.ParseException e) {
        fail("LPSQL parser rejected " + src + ": " + e.getMessage());
      }
    }
  }

  private static void demoProcRewritesCount() {
    String src = lpsql(""
        + "DECLARE row_nums INT;\n"
        + "DECLARE i INT = 1;\n"
        + "DECLARE x INT = 3;\n"
        + "EXECUTE IMMEDIATE 'SET mem_limit = 100m';\n"
        + "DROP TABLE IF EXISTS default.temp_table;\n"
        + "CREATE TABLE default.temp_table (id BIGINT);\n"
        + "WHILE i <= x LOOP\n"
        + "  INSERT INTO default.temp_table SELECT id FROM default.product;\n"
        + "  SET i = i + 1;\n"
        + "END LOOP;\n"
        + "SELECT COUNT(*) INTO row_nums FROM default.temp_table;\n"
        + "SELECT :row_nums AS result;\n");
    RecordingRunner runner = new RecordingRunner();
    TsqlRuntime.Step step = runUntilClient(src, runner);
    check(step.kind == TsqlRuntime.Kind.CLIENT_RESULT, "client result");
    check(step.sql.contains("6"), "rewritten select was " + step.sql);
    check(step.sql.toLowerCase(Locale.ROOT).contains("as result"), "label");
    check("100m".equals(step.procedureOptions.get("mem_limit")), "procedure option");
    boolean sawId = false;
    for (String sql : runner.sql) {
      if (sql.toUpperCase(Locale.ROOT).contains("INSERT")) {
        check(sql.contains("id"), "insert kept id: " + sql);
        sawId = true;
      }
    }
    check(sawId, "saw insert");
  }

  private static void cursorFieldReadsName() {
    String src = lpsql(""
        + "FOR c1 IN ('SHOW TABLES IN default') LOOP\n"
        + "  EXECUTE IMMEDIATE 'INSERT INTO t VALUES (' || QUOTE_LITERAL(c1.name) || ')';\n"
        + "END LOOP;\n"
        + "SELECT 1 AS result;\n");
    RecordingRunner runner = new RecordingRunner();
    runner.showTables = Arrays.asList("alpha", "beta");
    runUntilClient(src, runner);
    String executed = String.join("\n", runner.sql);
    check(executed.contains("'alpha'"), "alpha: " + executed);
    check(executed.contains("'beta'"), "beta: " + executed);
  }

  private static void fetchSqlcodeStopsTheLoop() {
    String src = lpsql(""
        + "DECLARE c CURSOR FOR SELECT n FROM t;\n"
        + "DECLARE name STRING;\n"
        + "OPEN c;\n"
        + "LOOP\n"
        + "  FETCH c INTO name;\n"
        + "  IF SQLCODE <> 0 THEN LEAVE; END IF;\n"
        + "  EXECUTE IMMEDIATE 'MARK ' || name;\n"
        + "END LOOP;\n"
        + "CLOSE c;\n"
        + "SELECT 1 AS result;\n");
    RecordingRunner runner = new RecordingRunner();
    runner.cursorRows = Arrays.asList("a", "b");
    TsqlRuntime.Step step = runUntilClient(src, runner);
    check(step.kind == TsqlRuntime.Kind.CLIENT_RESULT, "loop finished");
    String executed = String.join("\n", runner.sql);
    check(executed.contains("MARK a"), "first row: " + executed);
    check(executed.contains("MARK b"), "second row: " + executed);
    check(!executed.contains("MARK NULL"), "stopped after the last row: " + executed);
  }

  private static void concatBuildsShowColumnStats() {
    String src = lpsql(""
        + "DECLARE name STRING = 't';\n"
        + "EXECUTE IMMEDIATE 'SHOW COLUMN STATS default.' || name;\n"
        + "SELECT 1 AS result;\n");
    RecordingRunner runner = new RecordingRunner();
    runUntilClient(src, runner);
    check(runner.sql.contains("SHOW COLUMN STATS default.t"),
        "concat sql was " + runner.sql);
  }

  private static void exceptionSkipsOneFailure() {
    String src = lpsql(""
        + "DECLARE n INT = 0;\n"
        + "DECLARE i INT = 0;\n"
        + "WHILE i < 2 LOOP\n"
        + "  BEGIN\n"
        + "    EXECUTE IMMEDIATE 'SELECT boom';\n"
        + "  EXCEPTION WHEN OTHERS THEN\n"
        + "    SET n = n + 1;\n"
        + "  END;\n"
        + "  SET i = i + 1;\n"
        + "END LOOP;\n"
        + "SELECT :n AS result;\n");
    RecordingRunner runner = new RecordingRunner();
    runner.failSqlContaining = "boom";
    TsqlRuntime.Step step = runUntilClient(src, runner);
    check(step.kind == TsqlRuntime.Kind.CLIENT_RESULT, "handler continued");
    check(step.sql.contains("2"), "both failures counted: " + step.sql);
  }

  private static void storedLpsqlRoundTrips() {
    String src = ""
        + "LPSQL\n"
        + "CREATE OR REPLACE PROCEDURE p (IN x INT)\n"
        + "BEGIN\n"
        + "  SELECT :x AS result;\n"
        + "END";
    TsqlRuntime.MemoryStore store = new TsqlRuntime.MemoryStore();
    TsqlRuntime create = new TsqlRuntime(src, "default", "owner", store);
    TsqlRuntime.Step created = create.next(null);
    check(created.kind == TsqlRuntime.Kind.FINISHED, "create finished: " + created.errorMessage);
    String stored = store.getSource("default", "p");
    check(stored != null && stored.startsWith("LPSQL"), "stored batch: " + stored);
    check(stored.contains("CREATE OR REPLACE"), "original text kept");
    RecordingRunner runner = new RecordingRunner();
    TsqlRuntime.Step step = runUntilClient("LPSQL\nCALL p(3);", runner, store);
    check(step.kind == TsqlRuntime.Kind.CLIENT_RESULT, "call result");
    check(step.sql.contains("3"), "arg substituted: " + step.sql);
  }

  private static void storedTsqlStaysOnTsqlParser() {
    TsqlRuntime.MemoryStore store = new TsqlRuntime.MemoryStore();
    store.upsert("default", "q", "owner",
        "TSQL\nCREATE PROCEDURE q @x INT AS BEGIN SELECT @x AS n; END");
    RecordingRunner runner = new RecordingRunner();
    TsqlRuntime.Step step = runUntilClient("TSQL\nEXEC q @x = 4;", runner, store);
    check(step.kind == TsqlRuntime.Kind.CLIENT_RESULT, "tsql call");
    check(step.sql.contains("4"), "tsql arg: " + step.sql);
  }

  private static void aliasKeepsTheVariableName() {
    String src = lpsql(""
        + "DECLARE bucket STRING = 'pos';\n"
        + "DECLARE n INT = 4;\n"
        + "SELECT :bucket AS bucket, :n AS n;\n");
    RecordingRunner runner = new RecordingRunner();
    TsqlRuntime.Step step = runUntilClient(src, runner);
    check("SELECT 'pos' AS bucket, 4 AS n".equals(step.sql), "alias sql was " + step.sql);
  }

  private static void cursorLoopSumsEachRowOnce() {
    String src = lpsql(""
        + "DECLARE num INT;\n"
        + "DECLARE s INT = 0;\n"
        + "DECLARE c CURSOR FOR SELECT n FROM lpsql_q.nums WHERE n > 1 ORDER BY n;\n"
        + "OPEN c;\n"
        + "LOOP\n"
        + "  FETCH c INTO num;\n"
        + "  IF SQLCODE <> 0 THEN LEAVE; END IF;\n"
        + "  SET s = s + num;\n"
        + "END LOOP;\n"
        + "CLOSE c;\n"
        + "SELECT :s AS total;\n");
    RecordingRunner runner = new RecordingRunner();
    runner.cursorRows = Arrays.asList("2", "3", "4");
    TsqlRuntime.Step step = runUntilClient(src, runner);
    check("SELECT 9 AS total".equals(step.sql), "cursor sum was " + step.sql);
  }

  private static void bareSelectInTheMiddleFails() {
    try {
      LpsqlParser.parse("SELECT 1 AS a; SELECT 2 AS b;");
      fail("middle select was accepted");
    } catch (TsqlParser.ParseException expected) {
      // ok
    }
  }

  private static String lpsql(String body) {
    return "LPSQL $$\n" + body + "\n$$";
  }

  private static TsqlRuntime.Step runUntilClient(String src, RecordingRunner runner) {
    return runUntilClient(src, runner, new TsqlRuntime.MemoryStore());
  }

  private static TsqlRuntime.Step runUntilClient(String src, RecordingRunner runner,
      TsqlRuntime.MemoryStore store) {
    TsqlRuntime rt = new TsqlRuntime(src, "default", "tester", store);
    TsqlSqlRunner.Result prev = null;
    for (int i = 0; i < 100; i++) {
      TsqlRuntime.Step step = rt.next(prev);
      if (step.kind == TsqlRuntime.Kind.FAILED) {
        throw new AssertionError(step.errorMessage);
      }
      if (step.kind != TsqlRuntime.Kind.SQL) return step;
      prev = runner.execute(step.sql, step.procedureOptions);
    }
    throw new AssertionError("did not finish");
  }

  private static void check(boolean cond, String message) {
    if (!cond) fail(message);
  }

  private static void fail(String message) {
    failed++;
    System.err.println("FAIL " + message);
  }

  private static final class RecordingRunner implements TsqlSqlRunner {
    final List<String> sql = new ArrayList<String>();
    String failSqlContaining;
    List<String> cursorRows = Collections.emptyList();
    List<String> showTables = Collections.emptyList();

    public Result execute(String statement, Map<String, String> procedureOptions) {
      sql.add(statement);
      if (failSqlContaining != null && statement.contains(failSqlContaining)) {
        return new Result(Collections.<String>emptyList(),
            Collections.<List<Cell>>emptyList(), -1, "boom");
      }
      String upper = statement.toUpperCase(Locale.ROOT);
      if (upper.startsWith("SHOW TABLES") && !showTables.isEmpty()) {
        List<List<Cell>> rows = new ArrayList<List<Cell>>();
        for (String name : showTables) {
          rows.add(Collections.singletonList(Cell.of(name)));
        }
        return new Result(Collections.singletonList("name"), rows, 0, null);
      }
      if (upper.contains("COUNT")) {
        return new Result(Collections.singletonList("count"),
            Collections.singletonList(Collections.singletonList(Cell.of("6"))), 0, null);
      }
      if (upper.startsWith("SELECT") && !cursorRows.isEmpty()) {
        List<List<Cell>> rows = new ArrayList<List<Cell>>();
        for (String value : cursorRows) {
          rows.add(Collections.singletonList(Cell.of(value)));
        }
        return new Result(Collections.singletonList("n"), rows, 0, null);
      }
      return new Result(Collections.<String>emptyList(),
          Collections.<List<Cell>>emptyList(), 0, null);
    }
  }
}
