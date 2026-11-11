package org.apache.impala.tsql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Runs on a JDK with no Impala or Thrift jars. */
public final class TsqlRuntimeTest {
  private static int failed;

  public static void main(String[] args) {
    demoProcRewritesCountAndKeepsId();
    setOptionStaysInsideTheProcedure();
    rejectsLpsqlSpellings();
    rejectsBareSelectThatIsNotLast();
    rejectsUse();
    quoteLiteralEscapesQuoteAndBackslash();
    substitutionSkipsStringLiterals();
    tryCatchSwallowsChildFailure();
    nestedExec();
    cursorFetchStatus();
    cursorSumsWhileFetchStatusIsZero();
    castAsTypeConcatenates();
    createRuntimeDictParses();
    negativeArgumentSubstitutes();
    if (failed > 0) {
      System.err.println(failed + " test(s) failed");
      System.exit(1);
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
    check("'a''b'".equals(TsqlRuntime.quoteLiteral("a'b"))
        || TsqlRuntime.quoteLiteral("a'b").equals("'a''b'"), "quote");
    String quoted = TsqlRuntime.quoteLiteral("a\\b");
    check(quoted.contains("\\\\"), "backslash escaped: " + quoted);
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

  private static void negativeArgumentSubstitutes() {
    String src = ""
        + "CREATE PROCEDURE if_sign @n INT AS BEGIN SELECT @n AS n; END\n"
        + "EXEC if_sign @n = -2;\n";
    RecordingRunner runner = new RecordingRunner();
    TsqlRuntime.Step step = runUntilClient(src, runner);
    check(step.sql.contains("-2"), "negative arg substituted: " + step.sql);
  }

  private static TsqlRuntime.Step runUntilClient(String src, RecordingRunner runner) {
    TsqlRuntime rt = new TsqlRuntime(src, "default", "tester", new TsqlRuntime.MemoryStore());
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

    public Result execute(String statement, Map<String, String> procedureOptions) {
      sql.add(statement);
      if (failSqlContaining != null && statement.contains(failSqlContaining)) {
        return new Result(Collections.<String>emptyList(),
            Collections.<List<Cell>>emptyList(), -1, "boom");
      }
      String upper = statement.toUpperCase(Locale.ROOT);
      if (upper.contains("COUNT")) {
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
