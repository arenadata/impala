package org.apache.impala.lpsql;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import org.apache.impala.tsql.TsqlAst;
import org.apache.impala.tsql.TsqlParser;

/** Keyword to AST. One case per LPSQL form. */
public final class LpsqlAstTest {
  private static int failed;

  public static void main(String[] args) {
    expect("LPSQL $$ BEGIN NULL; END $$", "RawSql");
    expect("LPSQL 'BEGIN NULL; END'", "RawSql");
    expect("LPSQL \"BEGIN NULL; END\"", "RawSql");
    expect("CREATE OR REPLACE PROCEDURE p (IN x INT) BEGIN END",
        "CreateProc p lpsql\n  Param x INT in");
    expect("CREATE PROCEDURE p (x INT, OUT y STRING) BEGIN END",
        "CreateProc p lpsql\n  Param x INT in\n  Param y STRING out");
    expect("DECLARE n INT;", "DeclareVar n INT");
    expect("DECLARE n INT = 1;", "DeclareVar n INT = Literal 1");
    expect("DECLARE s = 'a';", "DeclareVar s STRING = Literal 'a'");
    expect("n = 1;", "SetStmt n = Literal 1");
    expect("IF n > 0 THEN SET n = 1; ELSE SET n = 2; END IF;",
        "IfStmt (Literal n > Literal 0)\n"
            + "  then\n"
            + "    SetStmt n = Literal 1\n"
            + "  else\n"
            + "    SetStmt n = Literal 2");
    expect("WHILE n > 0 LOOP LEAVE; END LOOP;",
        "WhileStmt (Literal n > Literal 0)\n  BreakStmt");
    expect("LOOP LEAVE; END LOOP;",
        "WhileStmt Literal 1\n  BreakStmt");
    expect("FOR i IN 1..n LOOP LEAVE; END LOOP;",
        "SetStmt i = Literal 1\n"
            + "DeclareVar lpsql_for_hi_1 STRING = Literal n\n"
            + "WhileStmt (Var i <= Var lpsql_for_hi_1)\n"
            + "  BreakStmt\n"
            + "  SetStmt i = (Var i + Literal 1)");
    expect("SELECT COUNT(*) INTO n FROM t;",
        "SelectAssign n SELECT COUNT(*) FROM t");
    expect("EXECUTE IMMEDIATE 'SELECT 1';", "ExecString Literal 'SELECT 1'");
    expect("EXECUTE sql;", "ExecString Literal sql");
    expect("n = 'a' || b;", "SetStmt n = (Literal 'a' || Literal b)");
    expect("DECLARE c CURSOR;", "DeclareCursor c");
    expect("DECLARE c CURSOR FOR 'SHOW TABLES';",
        "DeclareCursor c dynamic=Literal 'SHOW TABLES'");
    expect("OPEN c;", "OpenCursor c");
    expect("FETCH c INTO a, b;", "FetchCursor c a, b");
    expect("CLOSE c;", "CloseCursor c");
    expect("FOR c IN ('SHOW TABLES') LOOP EXECUTE IMMEDIATE c.name; END LOOP;",
        "DeclareCursor c dynamic=Literal 'SHOW TABLES'\n"
            + "OpenCursor c\n"
            + "FetchCursor c\n"
            + "WhileStmt (Var fetch_status = Literal 0)\n"
            + "  ExecString CursorField c.name\n"
            + "  FetchCursor c\n"
            + "CloseCursor c");
    expect("BEGIN EXECUTE IMMEDIATE 'SELECT 1'; EXCEPTION WHEN OTHERS THEN n = 0; END;",
        "TryStmt\n"
            + "  body\n"
            + "    ExecString Literal 'SELECT 1'\n"
            + "  handler\n"
            + "    SetStmt n = Literal 0");
    expect("CALL p(1, 2);",
        "ExecProc p\n  Arg Literal 1\n  Arg Literal 2");
    expect("RETURN;", "ReturnStmt");
    expect("IF SQLCODE <> 0 THEN LEAVE; END IF;",
        "IfStmt (Var sqlcode <> Literal 0)\n  then\n    BreakStmt");
    expect("n = QUOTE_LITERAL(s);", "SetStmt n = Call QUOTE_LITERAL(Literal s)");
    expect("DECLARE n INT; SELECT n AS result;",
        "DeclareVar n INT\nBareSelect SELECT @n AS result");
    expectFail("USE other;");
    expectFail("GOTO x;");
    expectFail("n %TYPE;");
    expectFail("SELECT 1 AS a; SELECT 2 AS b;");
    check(LpsqlParser.looksLikeLpsql("LPSQL CREATE OR REPLACE PROCEDURE p (IN x INT) BEGIN END"),
        "leading LPSQL");
    check(LpsqlParser.looksLikeLpsql("LPSQL $$ BEGIN NULL; END $$"), "wrapper");
    check(!LpsqlParser.looksLikeLpsql("CREATE OR REPLACE PROCEDURE p (IN x INT) BEGIN END"),
        "OR REPLACE is not an entry point");
    check(!LpsqlParser.looksLikeLpsql("TSQL CREATE PROCEDURE p @x INT AS BEGIN END"),
        "TSQL is the other dialect");
    expectFail("LPSQL DECLARE @n INT = 1;");
    expectFail("LPSQL BEGIN TRY SET n = 1; END TRY BEGIN CATCH END CATCH;");
    expectFail("LPSQL FETCH NEXT FROM c INTO n;");
    examplesParse();
    if (failed > 0) {
      System.err.println(failed + " test(s) failed");
      System.exit(1);
    }
    System.out.println("LpsqlAstTest OK");
  }

  private static void examplesParse() {
    String[] names = new String[] {
      "lpsql_demo_proc.sql",
      "lpsql_show_table_cursor.sql",
      "lpsql_create_runtime_dict.sql",
      "lpsql_show_tables.sql",
      "lpsql_scd1_load.sql",
      "lpsql_stats_missing_proc.sql"
    };
    for (String name : names) {
      File file = new File("lpsql-dev/examples/" + name);
      if (!file.isFile()) file = new File("examples/" + name);
      if (!file.isFile()) {
        fail("missing example " + name);
        continue;
      }
      try {
        String text = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
        LpsqlParser.parse(text);
      } catch (TsqlParser.ParseException e) {
        fail(name + ": " + e.getMessage());
      } catch (java.io.IOException e) {
        fail(name + ": " + e.getMessage());
      }
    }
  }

  private static void expect(String src, String dump) {
    try {
      String actual = TsqlAst.dump(LpsqlParser.parse(src));
      if (!dump.equals(actual)) {
        fail("source [" + src + "]\nexpected:\n" + dump + "\nactual:\n" + actual);
      }
    } catch (TsqlParser.ParseException e) {
      fail("source [" + src + "] threw " + e.getMessage());
    }
  }

  private static void expectFail(String src) {
    try {
      LpsqlParser.parse(src);
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
}
