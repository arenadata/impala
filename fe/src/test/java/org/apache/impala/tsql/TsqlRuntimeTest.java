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

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Runs on a JDK with no Impala or Thrift jars. */
public final class TsqlRuntimeTest {
  private static int failed;

  @Test
  public void all() {
    main(new String[0]);
  }

  public static void main(String[] args) {
    demoProcRewritesCountAndKeepsId();
    setOptionStaysInsideTheProcedure();
    rejectsLpsqlSpellings();
    rejectsBareSelectThatIsNotLast();
    rejectsUse();
    quoteLiteralEscapesQuoteAndBackslash();
    substitutionSkipsStringLiterals();
    inlineLiteralUsesImpalaEscapes();
    escapedQuoteIsNotSubstituted();
    tryCatchSwallowsChildFailure();
    uncaughtChildFailureFailsTheScript();
    failedOpenDoesNotConsumeNextStatement();
    nestedExec();
    cursorFetchStatus();
    cursorSumsWhileFetchStatusIsZero();
    activeOrderTotalAssignsFirstAggregateRow();
    materializeAmountIsFourStatements();
    showColumnStatsUsesDistinctCount();
    procedureSetNumNodesIsVisible();
    sessionOptionsAreNotForced();
    castAsTypeConcatenates();
    createRuntimeDictParses();
    negativeArgumentSubstitutes();
    digitStringKeepsLeadingZeros();
    bigintDoesNotLosePrecision();
    infinityStaysQuoted();
    rejectsTextAssignedToInt();
    stringCompareDoesNotCoerce();
    stringPlusConcatenatesDigits();
    ownerCanReplaceAndStrangerCannot();
    if (failed > 0) {
      System.err.println(failed + " test(s) failed");
      throw new AssertionError(failed + " test(s) failed");
    }
    System.out.println("TsqlRuntimeTest OK");
  }

  private static void demoProcRewritesCountAndKeepsId() {
    String src = ""
        + "DECLARE @row_nums INT;\n"
        + "DECLARE @i INT = 1;\n"
        + "DECLARE @x INT = 3;\n"
        + "EXEC('SET mem_limit = 100m');\n"
        + "DROP TABLE IF EXISTS default.temp_table;\n"
        + "CREATE TABLE default.temp_table (id BIGINT);\n"
        + "WHILE @i <= @x\n"
        + "BEGIN\n"
        + "  INSERT INTO default.temp_table SELECT id FROM default.product;\n"
        + "  SET @i = @i + 1;\n"
        + "END\n"
        + "SELECT @row_nums = COUNT(*) FROM default.temp_table;\n"
        + "SELECT @row_nums AS result;\n";
    RecordingRunner runner = new RecordingRunner();
    TsqlRuntime.Step step = runUntilClient(src, runner);
    check(step.kind == TsqlRuntime.Kind.CLIENT_RESULT, "client result");
    check(step.sql.contains("6"), "rewritten select was " + step.sql);
    check(step.sql.contains("AS result") || step.sql.toLowerCase(Locale.ROOT).contains("as result"),
        "label");
    boolean sawId = false;
    for (String sql : runner.sql) {
      if (sql.toUpperCase(Locale.ROOT).contains("INSERT")) {
        check(sql.contains("id"), "insert kept id: " + sql);
        sawId = true;
      }
    }
    check(sawId, "saw insert");
  }

  private static void setOptionStaysInsideTheProcedure() {
    String src = "EXEC('SET mem_limit = 100m');\nSELECT 1 AS result;\n";
    RecordingRunner runner = new RecordingRunner();
    TsqlRuntime.Step step = runUntilClient(src, runner);
    check("100m".equals(step.procedureOptions.get("mem_limit")),
        "option visible on later statement");
    check(runner.sessionOptions.isEmpty(), "session options were not updated");
  }

  private static void rejectsLpsqlSpellings() {
    expectParseFail("EXECUTE IMMEDIATE 'SELECT 1';");
    expectParseFail("FOR i IN 1..3 LOOP\nINSERT INTO t SELECT 1;\nEND LOOP;");
    expectParseFail("SELECT COUNT(*) INTO row_nums FROM t;");
    expectParseFail("LPSQL $$ BEGIN SELECT 1; END $$");
  }

  private static void rejectsBareSelectThatIsNotLast() {
    expectParseFail("SELECT 1 AS a;\nSELECT 2 AS b;\n");
  }

  private static void rejectsUse() {
    expectParseFail("USE otherdb;\nSELECT 1 AS result;\n");
  }

  private static void quoteLiteralEscapesQuoteAndBackslash() {
    check("'a\\'b'".equals(TsqlRuntime.quoteLiteral("a'b")), "quote");
    String quoted = TsqlRuntime.quoteLiteral("a\\b");
    check(quoted.contains("\\\\"), "backslash escaped: " + quoted);
  }

  private static void inlineLiteralUsesImpalaEscapes() {
    RecordingRunner runner = new RecordingRunner();
    runUntilClient("INSERT INTO t VALUES ('O''Brien');\nSELECT 1 AS result;\n", runner);
    check(runner.sql.get(0).contains("'O\\'Brien'"), "inline literal: " + runner.sql);
  }

  private static void escapedQuoteIsNotSubstituted() {
    RecordingRunner runner = new RecordingRunner();
    runUntilClient("DECLARE @x STRING = 'zz';\n"
        + "INSERT INTO t VALUES ('it''s @x');\nSELECT 1 AS r;\n", runner);
    check(runner.sql.get(0).contains("'it\\'s @x'"), "quoted: " + runner.sql);
    check(!String.join("\n", runner.sql).contains("zz"), "substituted inside literal: " + runner.sql);
    RecordingRunner path = new RecordingRunner();
    runUntilClient("INSERT INTO t VALUES ('C:\\data\\');\nSELECT 1 AS r;\n", path);
    check(path.sql.get(0).contains("'C:\\\\data\\\\'"), "backslash literal: " + path.sql);
  }

  private static void substitutionSkipsStringLiterals() {
    String src = "DECLARE @name STRING = 'zz';\n"
        + "EXEC('SELECT ''@name'' AS result');\n";
    RecordingRunner runner = new RecordingRunner();
    runUntilClient(src, runner);
    String executed = String.join("\n", runner.sql);
    check(executed.contains("'@name'"), "string kept @name: " + executed);
    check(!executed.contains("'zz'"), "did not substitute inside quotes: " + executed);
  }

  private static void tryCatchSwallowsChildFailure() {
    String src = "BEGIN TRY\nEXEC('SELECT boom');\nEND TRY\nBEGIN CATCH\nEND CATCH\nSELECT 1 AS result;\n";
    RecordingRunner runner = new RecordingRunner();
    runner.failSqlContaining = "boom";
    TsqlRuntime.Step step = runUntilClient(src, runner);
    check(step.kind == TsqlRuntime.Kind.CLIENT_RESULT, "catch continued");
  }

  private static void uncaughtChildFailureFailsTheScript() {
    String src = "EXEC('SELECT boom');\nSELECT 1 AS result;\n";
    RecordingRunner runner = new RecordingRunner();
    runner.failSqlContaining = "boom";
    TsqlRuntime rt = new TsqlRuntime(batch(src), "default", "tester", new TsqlRuntime.MemoryStore());
    TsqlSqlRunner.Result prev = null;
    TsqlRuntime.Step failed = null;
    for (int i = 0; i < 10; i++) {
      TsqlRuntime.Step step = rt.next(prev);
      if (step.kind == TsqlRuntime.Kind.FAILED) {
        failed = step;
        break;
      }
      if (step.kind != TsqlRuntime.Kind.SQL) break;
      prev = runner.execute(step.sql, step.procedureOptions);
    }
    check(failed != null && failed.errorMessage != null && failed.errorMessage.contains("boom"),
        "uncaught child error was " + (failed == null ? "missing" : failed.errorMessage));
  }

  private static void failedOpenDoesNotConsumeNextStatement() {
    String src = ""
        + "DECLARE @n INT;\n"
        + "BEGIN TRY\n"
        + "  DECLARE c CURSOR FOR SELECT boom FROM t;\n"
        + "  OPEN c;\n"
        + "END TRY\n"
        + "BEGIN CATCH\n"
        + "END CATCH\n"
        + "EXEC('SELECT 9 AS n');\n"
        + "FETCH NEXT FROM c INTO @n;\n"
        + "SELECT @n AS result;\n";
    RecordingRunner runner = new RecordingRunner();
    runner.failSqlContaining = "boom";
    runner.cursorRows = Arrays.asList("9");
    TsqlRuntime.Step step = runUntilClient(src, runner);
    check(step.sql.toUpperCase(Locale.ROOT).contains("NULL"),
        "failed open left @n as " + step.sql);
    check(!step.sql.contains("9"), "next statement was applied to the failed cursor: " + step.sql);
  }

  private static void activeOrderTotalAssignsFirstAggregateRow() {
    String src = ""
        + "CREATE OR ALTER PROCEDURE active_order_total\n"
        + "AS\n"
        + "BEGIN\n"
        + "  DECLARE @cnt INT;\n"
        + "  DECLARE @amt INT;\n"
        + "  SELECT @cnt = COUNT(*), @amt = SUM(amount)\n"
        + "  FROM lpsql_q.orders\n"
        + "  WHERE customer_id IN (SELECT id FROM lpsql_q.customers WHERE active = 1);\n"
        + "  SELECT @cnt AS active_orders, @amt AS active_amount;\n"
        + "END\n"
        + "EXEC active_order_total;\n";
    RecordingRunner runner = new RecordingRunner();
    runner.countRow = Arrays.asList(TsqlSqlRunner.Cell.of("3"), TsqlSqlRunner.Cell.of("32"));
    TsqlRuntime.Step step = runUntilClient(src, runner);
    boolean sawAssign = false;
    for (String sql : runner.sql) {
      String upper = sql.toUpperCase(Locale.ROOT);
      if (upper.contains("COUNT") && upper.contains("SUM") && upper.contains(" IN ")) {
        sawAssign = true;
        check(upper.contains("CUSTOMERS"), "assignment sql was " + sql);
      }
    }
    check(sawAssign, "missing aggregate assignment: " + runner.sql);
    check(step.sql.contains("3") && step.sql.contains("32"), "assigned totals were " + step.sql);
  }

  private static void materializeAmountIsFourStatements() {
    String src = ""
        + "CREATE OR ALTER PROCEDURE materialize_amount\n"
        + "AS\n"
        + "BEGIN\n"
        + "  EXEC('DROP TABLE IF EXISTS lpsql_q.amount_mv');\n"
        + "  CREATE TABLE lpsql_q.amount_mv STORED AS PARQUET AS\n"
        + "    SELECT SUM(v) AS total FROM lpsql_q.amounts;\n"
        + "  INSERT INTO lpsql_q.amounts VALUES (5);\n"
        + "  SELECT m.total AS materialized_total,\n"
        + "    (SELECT SUM(v) FROM lpsql_q.amounts) AS live_total\n"
        + "  FROM lpsql_q.amount_mv m;\n"
        + "END\n"
        + "EXEC materialize_amount;\n";
    RecordingRunner runner = new RecordingRunner();
    TsqlRuntime.Step step = runUntilClient(src, runner);
    check(runner.sql.size() == 3, "child statements were " + runner.sql);
    check(runner.sql.get(0).toUpperCase(Locale.ROOT).contains("DROP TABLE"), runner.sql.get(0));
    String create = runner.sql.get(1).toUpperCase(Locale.ROOT);
    check(create.contains("CREATE TABLE") && create.contains("SUM"), runner.sql.get(1));
    check(runner.sql.get(2).toUpperCase(Locale.ROOT).contains("INSERT"), runner.sql.get(2));
    String client = step.sql.toUpperCase(Locale.ROOT);
    check(client.contains("MATERIALIZED_TOTAL") && client.contains("LIVE_TOTAL"), step.sql);
  }

  private static void showColumnStatsUsesDistinctCount() {
    String src = ""
        + "CREATE OR ALTER PROCEDURE dict\n"
        + "AS\n"
        + "BEGIN\n"
        + "  DECLARE @name STRING;\n"
        + "  DECLARE @col STRING;\n"
        + "  DECLARE @tp STRING;\n"
        + "  DECLARE @ndv INT;\n"
        + "  DECLARE @sql STRING;\n"
        + "  DECLARE c1 CURSOR FOR SHOW TABLES IN default;\n"
        + "  OPEN c1;\n"
        + "  FETCH NEXT FROM c1 INTO @name;\n"
        + "  WHILE @@FETCH_STATUS = 0\n"
        + "  BEGIN\n"
        + "    BEGIN TRY\n"
        + "      SET @sql = 'SHOW COLUMN STATS default.' + @name;\n"
        + "      DECLARE c2 CURSOR FOR @sql;\n"
        + "      OPEN c2;\n"
        + "      FETCH NEXT FROM c2 INTO @col, @tp, @ndv;\n"
        + "      WHILE @@FETCH_STATUS = 0\n"
        + "      BEGIN\n"
        + "        SET @sql = 'INSERT INTO default.cursor_result_table VALUES ('\n"
        + "          + QUOTE_LITERAL(@name) + ', '\n"
        + "          + QUOTE_LITERAL(@col) + ', '\n"
        + "          + CAST(@ndv AS STRING) + ')';\n"
        + "        EXEC(@sql);\n"
        + "        FETCH NEXT FROM c2 INTO @col, @tp, @ndv;\n"
        + "      END\n"
        + "      CLOSE c2;\n"
        + "      DEALLOCATE c2;\n"
        + "    END TRY\n"
        + "    BEGIN CATCH\n"
        + "    END CATCH\n"
        + "    FETCH NEXT FROM c1 INTO @name;\n"
        + "  END\n"
        + "  CLOSE c1;\n"
        + "  DEALLOCATE c1;\n"
        + "  SELECT @sql AS last_insert;\n"
        + "END\n"
        + "EXEC dict;\n";
    RecordingRunner runner = new RecordingRunner();
    runner.failSqlContaining = "v_show_table";
    runner.showTables = Arrays.asList("v_show_table", "lpsql_marker");
    runner.statsCells = Arrays.asList(
        TsqlSqlRunner.Cell.of("id"),
        TsqlSqlRunner.Cell.of("INT"),
        TsqlSqlRunner.Cell.of("-1"),
        TsqlSqlRunner.Cell.of("0"),
        TsqlSqlRunner.Cell.of("4"));
    TsqlRuntime.Step step = runUntilClient(src, runner);
    boolean sawMarker = false;
    for (String sql : runner.sql) {
      if (sql.toUpperCase(Locale.ROOT).contains("INSERT") && sql.contains("v_show_table")) {
        fail("view was inserted: " + sql);
      }
      if (sql.contains("lpsql_marker") && sql.contains("-1")) {
        sawMarker = true;
        check(!sql.contains(", 0)") && !sql.contains(", 4)"), "ndv column was " + sql);
      }
    }
    check(sawMarker, "missing distinct-count insert: " + runner.sql);
    check(step.kind == TsqlRuntime.Kind.CLIENT_RESULT, "dict did not finish");
  }

  private static void sessionOptionsAreNotForced() {
    RecordingRunner runner = new RecordingRunner();
    TsqlRuntime.Step step = runUntilClient("SELECT 1 AS result;\n", runner);
    check(!step.procedureOptions.containsKey("num_nodes"),
        "default num_nodes was " + step.procedureOptions);
    check(!step.procedureOptions.containsKey("sync_ddl"),
        "default sync_ddl was " + step.procedureOptions);
    check(!step.procedureOptions.containsKey("enable_async_ddl_execution"),
        "async ddl was " + step.procedureOptions);
  }

  private static void procedureSetNumNodesIsVisible() {
    String src = "EXEC('SET num_nodes = 0');\nSELECT 1 AS result;\n";
    RecordingRunner runner = new RecordingRunner();
    TsqlRuntime.Step step = runUntilClient(src, runner);
    check("0".equals(step.procedureOptions.get("num_nodes")),
        "num_nodes option was " + step.procedureOptions);
  }

  private static void nestedExec() {
    String src = "CREATE PROCEDURE inner_p @n INT AS BEGIN SELECT @n AS result; END\n"
        + "EXEC inner_p @n = 4;\n";
    RecordingRunner runner = new RecordingRunner();
    TsqlRuntime.Step step = runUntilClient(src, runner);
    check(step.sql.contains("4"), "nested arg substituted: " + step.sql);
  }

  private static void cursorFetchStatus() {
    String src = "DECLARE c1 CURSOR FOR SELECT name FROM t;\n"
        + "OPEN c1;\n"
        + "FETCH NEXT FROM c1 INTO @name;\n"
        + "DECLARE @name STRING;\n"
        + "SELECT @name AS result;\n";
    // DECLARE must come first. Rewrite a valid order.
    src = "DECLARE @name STRING;\n"
        + "DECLARE c1 CURSOR FOR SELECT name FROM t;\n"
        + "OPEN c1;\n"
        + "FETCH NEXT FROM c1 INTO @name;\n"
        + "SELECT @name AS result;\n";
    RecordingRunner runner = new RecordingRunner();
    runner.cursorRows = Arrays.asList("alpha");
    TsqlRuntime.Step step = runUntilClient(src, runner);
    check(step.sql.contains("alpha"), "fetched name: " + step.sql);
  }

  private static void cursorSumsWhileFetchStatusIsZero() {
    String src = ""
        + "DECLARE @n INT;\n"
        + "DECLARE @s INT = 0;\n"
        + "DECLARE c CURSOR FOR SELECT n FROM lpsql_q.nums WHERE n > 1;\n"
        + "OPEN c;\n"
        + "FETCH NEXT FROM c INTO @n;\n"
        + "WHILE @@FETCH_STATUS = 0\n"
        + "BEGIN\n"
        + "  SET @s = @s + @n;\n"
        + "  FETCH NEXT FROM c INTO @n;\n"
        + "END\n"
        + "SELECT @s AS total;\n";
    RecordingRunner runner = new RecordingRunner();
    runner.cursorRows = Arrays.asList("2", "3", "4");
    TsqlRuntime.Step step = runUntilClient(src, runner);
    check(step.kind == TsqlRuntime.Kind.CLIENT_RESULT, "cursor sum reached client select");
    check(step.sql.contains("9"), "cursor sum was " + step.sql);
  }

  private static void castAsTypeConcatenates() {
    String src = ""
        + "DECLARE @ndv INT = 7;\n"
        + "DECLARE @sql STRING;\n"
        + "SET @sql = 'ndv=' + CAST(@ndv AS STRING);\n"
        + "EXEC(@sql);\n";
    RecordingRunner runner = new RecordingRunner();
    runUntilClient(src, runner);
    String executed = String.join("\n", runner.sql);
    check(executed.contains("ndv=7"), "cast concatenation was " + executed);
  }

  private static void createRuntimeDictParses() {
    String src = ""
        + "CREATE OR ALTER PROCEDURE create_runtime_dict\n"
        + "AS\n"
        + "BEGIN\n"
        + "  DECLARE @name STRING;\n"
        + "  DECLARE @col STRING;\n"
        + "  DECLARE @tp STRING;\n"
        + "  DECLARE @ndv INT;\n"
        + "  DECLARE @sql STRING;\n"
        + "  EXEC('CREATE TABLE default.cursor_result_table (dist_rows INT) STORED AS PARQUET');\n"
        + "  DECLARE c1 CURSOR FOR SHOW TABLES IN default;\n"
        + "  OPEN c1;\n"
        + "  FETCH NEXT FROM c1 INTO @name;\n"
        + "  WHILE @@FETCH_STATUS = 0\n"
        + "  BEGIN\n"
        + "    BEGIN TRY\n"
        + "      SET @sql = 'SHOW COLUMN STATS default.' + @name;\n"
        + "      DECLARE c2 CURSOR FOR @sql;\n"
        + "      OPEN c2;\n"
        + "      FETCH NEXT FROM c2 INTO @col, @tp, @ndv;\n"
        + "      WHILE @@FETCH_STATUS = 0\n"
        + "      BEGIN\n"
        + "        SET @sql = 'INSERT INTO default.cursor_result_table VALUES ('\n"
        + "          + QUOTE_LITERAL(@name) + ', '\n"
        + "          + CAST(@ndv AS DECIMAL(10, 2)) + ')';\n"
        + "        EXEC(@sql);\n"
        + "        FETCH NEXT FROM c2 INTO @col, @tp, @ndv;\n"
        + "      END\n"
        + "      CLOSE c2;\n"
        + "      DEALLOCATE c2;\n"
        + "    END TRY\n"
        + "    BEGIN CATCH\n"
        + "    END CATCH\n"
        + "    FETCH NEXT FROM c1 INTO @name;\n"
        + "  END\n"
        + "  CLOSE c1;\n"
        + "  DEALLOCATE c1;\n"
        + "END\n";
    TsqlParser.parse(src);
  }

  private static void digitStringKeepsLeadingZeros() {
    RecordingRunner runner = new RecordingRunner();
    TsqlRuntime.Step step = runUntilClient(
        "DECLARE @acct STRING = '00123';\nSELECT @acct AS acct;\n", runner);
    check(step.sql.contains("'00123'"), "acct was " + step.sql);
  }

  private static void bigintDoesNotLosePrecision() {
    RecordingRunner runner = new RecordingRunner();
    TsqlRuntime.Step step = runUntilClient(
        "DECLARE @id BIGINT = 9007199254740993;\nSELECT @id AS id;\n", runner);
    check(step.sql.contains("9007199254740993"), "id was " + step.sql);
  }

  private static void infinityStaysQuoted() {
    RecordingRunner runner = new RecordingRunner();
    TsqlRuntime.Step step = runUntilClient(
        "DECLARE @s STRING = 'Infinity';\nSELECT @s AS s;\n", runner);
    check(step.sql.contains("'Infinity'"), "infinity was " + step.sql);
  }

  private static void rejectsTextAssignedToInt() {
    RecordingRunner runner = new RecordingRunner();
    try {
      runUntilClient("DECLARE @n INT = 'abc';\nSELECT @n AS n;\n", runner);
      fail("assigned text to int");
    } catch (AssertionError e) {
      check(String.valueOf(e.getMessage()).contains("numeric"), "message was " + e.getMessage());
    }
  }

  private static void stringCompareDoesNotCoerce() {
    RecordingRunner runner = new RecordingRunner();
    TsqlRuntime.Step step = runUntilClient(
        "DECLARE @r STRING = 'diff';\n"
        + "IF '00123' = '123' SET @r = 'same';\n"
        + "SELECT @r AS result;\n", runner);
    check(step.sql.contains("'diff'"), "compare was " + step.sql);
  }

  private static void stringPlusConcatenatesDigits() {
    RecordingRunner runner = new RecordingRunner();
    TsqlRuntime.Step step = runUntilClient(
        "DECLARE @a STRING = '00123';\n"
        + "DECLARE @b STRING = '1';\n"
        + "SET @a = @a + @b;\n"
        + "SELECT @a AS result;\n", runner);
    check(step.sql.contains("'001231'"), "concat was " + step.sql);
  }

  private static void ownerCanReplaceAndStrangerCannot() {
    TsqlRuntime.MemoryStore store = new TsqlRuntime.MemoryStore();
    String create = "CREATE PROCEDURE p AS BEGIN SELECT 1 AS result; END\n";
    TsqlRuntime.Step step = new TsqlRuntime(batch(create), "default", "alice", store).next(null);
    check(step.kind == TsqlRuntime.Kind.FINISHED, "alice create: " + step.errorMessage);

    TsqlAccess noReplace = new TsqlAccess() {
      public boolean canCreate(String database, String user) { return true; }
      public boolean canReplaceAny(String database, String user) { return false; }
    };
    step = new TsqlRuntime(batch(create), "default", "bob", store, noReplace).next(null);
    check(step.kind == TsqlRuntime.Kind.FAILED, "bob should fail");
    check(step.errorMessage.contains("owned by 'alice'"), "bob message was " + step.errorMessage);
    check("alice".equals(store.getOwner("default", "p")), "owner changed to " + store.getOwner("default", "p"));

    TsqlAccess admin = new TsqlAccess() {
      public boolean canCreate(String database, String user) { return true; }
      public boolean canReplaceAny(String database, String user) { return true; }
    };
    String replace = "CREATE OR ALTER PROCEDURE p AS BEGIN SELECT 2 AS result; END\n";
    step = new TsqlRuntime(batch(replace), "default", "carol", store, admin).next(null);
    check(step.kind == TsqlRuntime.Kind.FINISHED, "admin replace: " + step.errorMessage);
    check("alice".equals(store.getOwner("default", "p")),
        "owner stayed " + store.getOwner("default", "p"));

    TsqlAccess noCreate = new TsqlAccess() {
      public boolean canCreate(String database, String user) { return false; }
      public boolean canReplaceAny(String database, String user) { return false; }
    };
    step = new TsqlRuntime(batch("CREATE PROCEDURE q AS BEGIN SELECT 1 AS result; END\n"),
        "default", "dave", store, noCreate).next(null);
    check(step.kind == TsqlRuntime.Kind.FAILED, "create should fail");
    check(step.errorMessage.contains("CREATE"), "dave message was " + step.errorMessage);
  }

  private static void negativeArgumentSubstitutes() {
    String src = ""
        + "CREATE PROCEDURE if_sign @n INT AS BEGIN SELECT @n AS n; END\n"
        + "EXEC if_sign @n = -2;\n";
    RecordingRunner runner = new RecordingRunner();
    TsqlRuntime.Step step = runUntilClient(src, runner);
    check(step.sql.contains("-2"), "negative arg substituted: " + step.sql);
  }

  private static String batch(String src) {
    if (DialectCheck.leadingIs(src, "TSQL") || DialectCheck.leadingIs(src, "LPSQL")) return src;
    return "TSQL\n" + src;
  }

  private static TsqlRuntime.Step runUntilClient(String src, RecordingRunner runner) {
    TsqlRuntime.MemoryStore store = new TsqlRuntime.MemoryStore();
    String text = src;
    int split = text.lastIndexOf("\nEXEC ");
    if (split < 0) split = text.lastIndexOf("\nCALL ");
    if (split > 0 && (text.contains("PROCEDURE") || text.contains("procedure"))) {
      String create = text.substring(0, split);
      TsqlRuntime.Step created = new TsqlRuntime(batch(create), "default", "tester", store).next(null);
      if (created.kind == TsqlRuntime.Kind.FAILED) throw new AssertionError(created.errorMessage);
      text = text.substring(split + 1);
    }
    TsqlRuntime rt = new TsqlRuntime(batch(text), "default", "tester", store);
    TsqlSqlRunner.Result prev = null;
    for (int i = 0; i < 100000; i++) {
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

  private static void expectParseFail(String src) {
    try {
      TsqlParser.parse(src);
      fail("expected parse failure for " + src);
    } catch (TsqlParser.ParseException expected) {
      // ok
    }
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
    final Map<String, String> sessionOptions = new java.util.HashMap<String, String>();
    String failSqlContaining;
    List<String> cursorRows = Collections.emptyList();
    List<String> showTables = Collections.emptyList();
    List<Cell> countRow;
    List<Cell> statsCells;

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
      if (upper.startsWith("SHOW COLUMN STATS") && statsCells != null) {
        return new Result(Arrays.asList("column", "type", "ndv", "nulls", "max"),
            Collections.singletonList(statsCells), 0, null);
      }
      if (upper.contains("COUNT")) {
        if (countRow != null) {
          return new Result(Arrays.asList("cnt", "amt"),
              Collections.singletonList(countRow), 0, null);
        }
        return new Result(Collections.singletonList("count"),
            Collections.singletonList(Collections.singletonList(Cell.of("6"))), 0, null);
      }
      if (upper.startsWith("SELECT") && !cursorRows.isEmpty()) {
        return cursorResult();
      }
      if (upper.startsWith("SELECT NAME") || upper.contains("FROM T")) {
        List<List<Cell>> rows = new ArrayList<List<Cell>>();
        for (String value : cursorRows) {
          rows.add(Collections.singletonList(Cell.of(value)));
        }
        return new Result(Collections.singletonList("name"), rows, 0, null);
      }
      return new Result(Collections.<String>emptyList(),
          Collections.<List<Cell>>emptyList(), 0, null);
    }

    private Result cursorResult() {
      List<List<Cell>> rows = new ArrayList<List<Cell>>();
      for (String value : cursorRows) {
        rows.add(Collections.singletonList(Cell.of(value)));
      }
      return new Result(Collections.singletonList("n"), rows, 0, null);
    }
  }
}
