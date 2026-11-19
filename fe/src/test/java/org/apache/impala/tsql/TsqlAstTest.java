package org.apache.impala.tsql;

/** Keyword to AST. One case per T-SQL form this parser accepts. */
public final class TsqlAstTest {
  private static int failed;

  public static void main(String[] args) {
    String ret = "CreateProc p tsql\n  Param p INT in\n  ReturnStmt";
    expect("TSQL\nCREATE PROCEDURE p @p INT AS BEGIN RETURN; END", ret);
    expect("CREATE PROCEDURE p @p INT AS BEGIN RETURN; END", ret);
    expect("CREATE PROC p @p INT AS BEGIN RETURN; END", ret);
    expect("CREATE OR ALTER PROCEDURE p @p INT AS BEGIN RETURN; END", ret);
    expect("CREATE PROCEDURE p @p INT OUTPUT WITH EXECUTE AS OWNER AS BEGIN RETURN; END",
        "CreateProc p tsql owner\n  Param p INT out\n  ReturnStmt");
    expect("DECLARE @n INT = 1;", "DeclareVar n INT = Literal 1");
    expect("DECLARE c CURSOR FOR SELECT 1;", "DeclareCursor c sql=SELECT 1");
    expect("SET @n = 1;", "SetStmt n = Literal 1");
    expect("SELECT @n = COUNT(*) FROM t;", "SelectAssign n SELECT COUNT ( * ) FROM t");
    expect("SELECT 1 AS result;", "BareSelect SELECT 1 AS result");
    expect("IF @n = 1 BEGIN SET @n = 2; END ELSE BEGIN SET @n = 3; END",
        "IfStmt (Var n = Literal 1)\n"
            + "  then\n"
            + "    SetStmt n = Literal 2\n"
            + "  else\n"
            + "    SetStmt n = Literal 3");
    expect("WHILE @n > 0 BEGIN BREAK; END",
        "WhileStmt (Var n > Literal 0)\n  BreakStmt");
    expect("WHILE @n > 0 BEGIN CONTINUE; END",
        "WhileStmt (Var n > Literal 0)\n  ContinueStmt");
    expect("BEGIN TRY SET @n = 1; END TRY BEGIN CATCH SET @n = 2; END CATCH",
        "TryStmt\n  body\n    SetStmt n = Literal 1\n  handler\n    SetStmt n = Literal 2");
    expect("EXEC(@sql);", "ExecString Var sql");
    expect("EXECUTE(@sql);", "ExecString Var sql");
    expect("EXEC p @a = 1;", "ExecProc p\n  Arg a = Literal 1");
    expect("CALL p(1);", "ExecProc p\n  Arg Literal 1");
    expect("OPEN c;", "OpenCursor c");
    expect("FETCH NEXT FROM c INTO @a;", "FetchCursor c a");
    expect("CLOSE c;", "CloseCursor c");
    expect("DEALLOCATE c;", "DeallocateCursor c");
    expect("RETURN;", "ReturnStmt");
    expect("SET @n = @@FETCH_STATUS;", "SetStmt n = Var FETCH_STATUS");
    expect("SET @n = @@ERROR;", "SetStmt n = Var ERROR");
    expect("SET @n = @a + @b;", "SetStmt n = (Var a + Var b)");
    expect("SET @n = QUOTE_LITERAL(@s);", "SetStmt n = Call QUOTE_LITERAL(Var s)");
    expect("IF @a = 1 AND NOT @b = 0 OR @c = 1 BEGIN SET @n = 1; END",
        "IfStmt (((Var a = Literal 1) AND (NOT (Var b = Literal 0))) OR (Var c = Literal 1))\n"
            + "  then\n"
            + "    SetStmt n = Literal 1");
    expectFail("USE other;");
    expectFail("FOR i IN 1..3 LOOP INSERT INTO t SELECT 1; END LOOP;");
    expectFail("EXECUTE IMMEDIATE 'SELECT 1';");
    expectFail("LPSQL $$ BEGIN SELECT 1; END $$");
    expectFail("TSQL BEGIN LEAVE; END");
    expectFail("TSQL EXECUTE IMMEDIATE 'SELECT 1';");
    expectFail("TSQL SELECT COUNT(*) INTO n FROM t;");
    check(TsqlParser.looksLikeTsql("TSQL CREATE OR ALTER PROCEDURE p @p INT AS BEGIN END"),
        "leading TSQL");
    check(!TsqlParser.looksLikeTsql("CREATE OR ALTER PROCEDURE p @p INT AS BEGIN END"),
        "CREATE is not an entry point");
    check(!TsqlParser.looksLikeTsql("EXEC p;"), "EXEC is not an entry point");
    check(!TsqlParser.looksLikeTsql("CALL p(1);"), "CALL is not an entry point");
    if (failed > 0) {
      System.err.println(failed + " test(s) failed");
      System.exit(1);
    }
    System.out.println("TsqlAstTest OK");
  }

  private static void expect(String src, String dump) {
    try {
      String actual = TsqlAst.dump(TsqlParser.parse(src));
      if (!dump.equals(actual)) {
        fail("source [" + src + "]\nexpected:\n" + dump + "\nactual:\n" + actual);
      }
    } catch (TsqlParser.ParseException e) {
      fail("source [" + src + "] threw " + e.getMessage());
    }
  }

  private static void expectFail(String src) {
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
}
