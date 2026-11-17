package org.apache.impala.lpsql;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.impala.tsql.DialectCheck;
import org.apache.impala.tsql.TsqlAst;
import org.apache.impala.tsql.TsqlParser;

/**
 * Hand-written parser for the Nova LPSQL subset. It lowers into {@link TsqlAst}
 * and does not import Impala or Thrift types.
 */
public final class LpsqlParser {
  public static final String MARKER = "lpsql\n";

  private final List<Tok> toks;
  private int p;
  private int forSeq;
  private final Set<String> scope = new HashSet<String>();
  private final Set<String> cursors = new HashSet<String>();

  private LpsqlParser(List<Tok> toks) {
    this.toks = toks;
  }

  public static boolean looksLikeLpsql(String sql) {
    return DialectCheck.leadingIs(sql, "LPSQL");
  }

  public static TsqlAst.Script parse(String sql) {
    String body = DialectCheck.stripLeading(sql, "LPSQL");
    DialectCheck.verifyLpsql(body);
    LpsqlParser parser = new LpsqlParser(lex(body));
    TsqlAst.Script script = new TsqlAst.Script();
    parser.skipSemi();
    while (!parser.isEof()) {
      script.statements.addAll(parser.parseStmt());
      parser.skipSemi();
    }
    TsqlParser.validateBareSelect(script.statements, true);
    return script;
  }

  private List<TsqlAst.Stmt> parseStmt() {
    if (check("%")) rejectTypeAttribute();
    if (check("USE")) {
      throw new TsqlParser.ParseException("USE is not allowed inside a procedure or batch");
    }
    if (check("GOTO")) {
      throw new TsqlParser.ParseException("GOTO is not supported");
    }
    if (check("BULK")) {
      throw new TsqlParser.ParseException("BULK COLLECT is not supported");
    }
    if (check("END") || check("ELSE") || check("EXCEPTION") || check("THEN")) {
      throw new TsqlParser.ParseException("Unexpected " + peekText());
    }
    if (check("NULL") && (lookaheadIs(1, ";") || lookaheadIs(1, "GO") || lookaheadIs(1, "END")
        || isEofAfter(1))) {
      bump();
      return one(new TsqlAst.RawSql(""));
    }
    if (check("CREATE")) return parseCreateOrSql();
    if (check("DECLARE")) return one(parseDeclare());
    if (check("SET")) return one(parseSet());
    if (check("SELECT")) return one(parseSelect());
    if (check("IF")) return one(parseIf());
    if (check("WHILE")) return one(parseWhile());
    if (check("LOOP")) return one(parseLoop());
    if (check("FOR")) return parseFor();
    if (check("BEGIN")) return parseBegin();
    if (check("EXEC") || check("EXECUTE")) return one(parseExec());
    if (check("CALL")) return one(parseCall());
    if (check("RETURN")) {
      bump();
      return one(new TsqlAst.ReturnStmt());
    }
    if (check("LEAVE")) {
      bump();
      return one(new TsqlAst.BreakStmt());
    }
    if (check("OPEN")) {
      bump();
      return one(new TsqlAst.OpenCursor(ident()));
    }
    if (check("FETCH")) return one(parseFetch());
    if (check("CLOSE")) {
      bump();
      return one(new TsqlAst.CloseCursor(ident()));
    }
    if (check("DEALLOCATE")) {
      bump();
      return one(new TsqlAst.DeallocateCursor(ident()));
    }
    if (checkIdent() && lookaheadIs(1, "=")) return one(parseAssign());
    return one(new TsqlAst.RawSql(rewriteSql(readRawSql())));
  }

  private List<TsqlAst.Stmt> parseCreateOrSql() {
    if (lookaheadIs(1, "PACKAGE") || (lookaheadIs(1, "OR") && lookaheadIs(3, "PACKAGE"))) {
      throw new TsqlParser.ParseException("Packages are not supported");
    }
    if (!isCreateProcedure()) return one(new TsqlAst.RawSql(rewriteSql(readRawSql())));
    return one(parseCreate());
  }

  private TsqlAst.Stmt parseCreate() {
    Set<String> outerScope = new HashSet<String>(scope);
    Set<String> outerCursors = new HashSet<String>(cursors);
    scope.clear();
    cursors.clear();
    try {
      bump(); // CREATE
      if (check("OR")) {
        bump();
        if (check("ALTER")) {
          throw new TsqlParser.ParseException("CREATE OR ALTER is T-SQL. Use CREATE OR REPLACE");
        }
        expect("REPLACE");
      }
      if (!(check("PROCEDURE") || check("PROC"))) {
        throw new TsqlParser.ParseException("Only CREATE PROCEDURE is an LPSQL statement");
      }
      bump();
      TsqlAst.CreateProc proc = new TsqlAst.CreateProc();
      proc.name = ident();
      proc.source = MARKER;
      if (check("(")) parseParams(proc);
      while (check("DECLARE")) {
        proc.body.add(parseDeclare());
        skipSemi();
      }
      if (check("BEGIN")) proc.body.addAll(parseBegin());
      else {
        while (!isEof() && !check("GO")) {
          proc.body.addAll(parseStmt());
          skipSemi();
        }
      }
      return proc;
    } finally {
      scope.clear();
      scope.addAll(outerScope);
      cursors.clear();
      cursors.addAll(outerCursors);
    }
  }

  private void parseParams(TsqlAst.CreateProc proc) {
    expect("(");
    if (check(")")) {
      bump();
      return;
    }
    do {
      if (check(",")) bump();
      boolean output = false;
      if (check("IN")) bump();
      if (check("OUT") || check("OUTPUT")) {
        bump();
        output = true;
      }
      String name = ident();
      if (check("%")) rejectTypeAttribute();
      String type = ident();
      if (check("(")) {
        bump();
        readRawBalanced();
      }
      if (check("%")) rejectTypeAttribute();
      proc.params.add(new TsqlAst.Param(name, type, output));
      scope.add(name.toLowerCase(Locale.ROOT));
    } while (check(","));
    expect(")");
  }

  private TsqlAst.Stmt parseDeclare() {
    bump();
    String name = ident();
    if (check("CURSOR")) {
      bump();
      cursors.add(name.toLowerCase(Locale.ROOT));
      if (!check("FOR")) return new TsqlAst.DeclareCursor(name, "", null);
      bump();
      if (dynamicQuery()) return new TsqlAst.DeclareCursor(name, null, parseExpr());
      return new TsqlAst.DeclareCursor(name, rewriteSql(readRawSql()), null);
    }
    scope.add(name.toLowerCase(Locale.ROOT));
    if (check("=")) {
      bump();
      return new TsqlAst.DeclareVar(name, "STRING", parseExpr());
    }
    if (check("%")) rejectTypeAttribute();
    String type = ident();
    if (check("(")) {
      bump();
      readRawBalanced();
    }
    if (check("%")) rejectTypeAttribute();
    TsqlAst.Expr init = null;
    if (check("=")) {
      bump();
      init = parseExpr();
    }
    return new TsqlAst.DeclareVar(name, type, init);
  }

  private boolean dynamicQuery() {
    if (checkString() || checkNumber() || check("(")) return true;
    if (!checkIdent()) return false;
    String name = peekText();
    return !(name.equalsIgnoreCase("SELECT") || name.equalsIgnoreCase("SHOW")
        || name.equalsIgnoreCase("WITH") || name.equalsIgnoreCase("VALUES"));
  }

  private TsqlAst.Stmt parseSet() {
    bump();
    return parseAssign();
  }

  private TsqlAst.Stmt parseAssign() {
    String name = ident();
    scope.add(name.toLowerCase(Locale.ROOT));
    expect("=");
    return new TsqlAst.SetStmt(name, parseExpr());
  }

  private TsqlAst.Stmt parseSelect() {
    int start = p;
    bump();
    int depth = 0;
    int intoAt = -1;
    for (int i = p; i < toks.size(); i++) {
      Tok t = toks.get(i);
      if (t.text.equals("(")) depth++;
      else if (t.text.equals(")")) depth = Math.max(0, depth - 1);
      else if (depth == 0 && (t.text.equals(";") || t.text.equalsIgnoreCase("GO"))) break;
      else if (depth == 0 && t.text.equalsIgnoreCase("INTO")) {
        intoAt = i;
        break;
      } else if (depth == 0 && isBoundary(t) && i > p) break;
    }
    if (intoAt < 0) {
      p = start;
      return new TsqlAst.BareSelect(rewriteSql(readRawSql()));
    }
    String list = joinTokens(p, intoAt);
    p = intoAt + 1;
    TsqlAst.SelectAssign assign = new TsqlAst.SelectAssign();
    do {
      if (check(",")) bump();
      assign.targets.add(ident());
    } while (check(","));
    String rest = "";
    if (!isEof() && !check(";") && !check("GO") && !isBoundary(peek())) rest = readRawSql();
    assign.sql = rewriteSql("SELECT " + list + (rest.isEmpty() ? "" : " " + rest));
    for (String target : assign.targets) scope.add(target.toLowerCase(Locale.ROOT));
    return assign;
  }

  private TsqlAst.Stmt parseIf() {
    bump();
    TsqlAst.IfStmt iff = new TsqlAst.IfStmt();
    iff.condition = parseExpr();
    expect("THEN");
    iff.thenBody.addAll(parseUntil("ELSE", "END"));
    if (check("ELSE")) {
      bump();
      iff.elseBody.addAll(parseUntil("END"));
    }
    expect("END");
    expect("IF");
    return iff;
  }

  private TsqlAst.Stmt parseWhile() {
    bump();
    TsqlAst.WhileStmt loop = new TsqlAst.WhileStmt();
    loop.condition = parseExpr();
    expect("LOOP");
    loop.body.addAll(parseUntil("END"));
    expect("END");
    expect("LOOP");
    return loop;
  }

  private TsqlAst.Stmt parseLoop() {
    bump();
    TsqlAst.WhileStmt loop = new TsqlAst.WhileStmt();
    loop.condition = new TsqlAst.Literal("1", false);
    loop.body.addAll(parseUntil("END"));
    expect("END");
    expect("LOOP");
    return loop;
  }

  private List<TsqlAst.Stmt> parseFor() {
    bump();
    String name = ident();
    expect("IN");
    if (check("(")) return parseCursorFor(name);
    scope.add(name.toLowerCase(Locale.ROOT));
    TsqlAst.Expr lo = parseExpr();
    expectDots();
    TsqlAst.Expr hi = parseExpr();
    expect("LOOP");
    List<TsqlAst.Stmt> body = parseUntil("END");
    expect("END");
    expect("LOOP");
    String hiName = "lpsql_for_hi_" + (++forSeq);
    scope.add(hiName);
    List<TsqlAst.Stmt> out = new ArrayList<TsqlAst.Stmt>();
    out.add(new TsqlAst.SetStmt(name, lo));
    out.add(new TsqlAst.DeclareVar(hiName, "STRING", hi));
    TsqlAst.WhileStmt loop = new TsqlAst.WhileStmt();
    loop.condition = new TsqlAst.Binary("<=", new TsqlAst.Var(name), new TsqlAst.Var(hiName));
    loop.body.addAll(body);
    loop.body.add(new TsqlAst.SetStmt(name,
        new TsqlAst.Binary("+", new TsqlAst.Var(name), new TsqlAst.Literal("1", false))));
    out.add(loop);
    return out;
  }

  private List<TsqlAst.Stmt> parseCursorFor(String name) {
    expect("(");
    TsqlAst.Expr dynamic = null;
    String sql = null;
    if (check("SELECT") || check("SHOW") || check("WITH") || check("VALUES")) {
      sql = readUntilCloseParen();
    } else {
      dynamic = parseExpr();
    }
    expect(")");
    expect("LOOP");
    cursors.add(name.toLowerCase(Locale.ROOT));
    List<TsqlAst.Stmt> body = parseUntil("END");
    expect("END");
    expect("LOOP");
    List<TsqlAst.Stmt> out = new ArrayList<TsqlAst.Stmt>();
    if (dynamic != null) out.add(new TsqlAst.DeclareCursor(name, null, dynamic));
    else out.add(new TsqlAst.DeclareCursor(name, rewriteSql(sql), null));
    out.add(new TsqlAst.OpenCursor(name));
    TsqlAst.FetchCursor first = new TsqlAst.FetchCursor();
    first.name = name;
    out.add(first);
    TsqlAst.WhileStmt loop = new TsqlAst.WhileStmt();
    loop.condition = new TsqlAst.Binary("=", new TsqlAst.Var("fetch_status"),
        new TsqlAst.Literal("0", false));
    loop.body.addAll(body);
    TsqlAst.FetchCursor again = new TsqlAst.FetchCursor();
    again.name = name;
    loop.body.add(again);
    out.add(loop);
    out.add(new TsqlAst.CloseCursor(name));
    return out;
  }

  private List<TsqlAst.Stmt> parseBegin() {
    bump();
    List<TsqlAst.Stmt> body = parseUntil("EXCEPTION", "END");
    if (check("EXCEPTION")) {
      bump();
      expect("WHEN");
      if (!check("OTHERS")) {
        throw new TsqlParser.ParseException("Only EXCEPTION WHEN OTHERS is supported");
      }
      bump();
      expect("THEN");
      List<TsqlAst.Stmt> handler = parseUntil("END");
      expect("END");
      TsqlAst.TryStmt block = new TsqlAst.TryStmt();
      block.body.addAll(body);
      block.handler.addAll(handler);
      return one(block);
    }
    expect("END");
    return body;
  }

  private TsqlAst.Stmt parseExec() {
    bump();
    if (check("IMMEDIATE")) bump();
    return new TsqlAst.ExecString(parseExpr());
  }

  private TsqlAst.Stmt parseCall() {
    bump();
    TsqlAst.ExecProc call = new TsqlAst.ExecProc();
    call.name = ident();
    if (check("(")) {
      bump();
      if (!check(")")) {
        do {
          if (check(",")) bump();
          call.args.add(new TsqlAst.Arg(null, parseExpr(), false));
        } while (check(","));
      }
      expect(")");
    }
    return call;
  }

  private TsqlAst.Stmt parseFetch() {
    bump();
    if (check("NEXT")) bump();
    if (check("FROM")) bump();
    TsqlAst.FetchCursor fetch = new TsqlAst.FetchCursor();
    fetch.name = ident();
    expect("INTO");
    do {
      if (check(",")) bump();
      String target = ident();
      fetch.targets.add(target);
      scope.add(target.toLowerCase(Locale.ROOT));
    } while (check(","));
    return fetch;
  }

  private List<TsqlAst.Stmt> parseUntil(String... words) {
    List<TsqlAst.Stmt> out = new ArrayList<TsqlAst.Stmt>();
    skipSemi();
    while (!isEof() && !isOneOf(words)) {
      out.addAll(parseStmt());
      skipSemi();
    }
    return out;
  }

  private boolean isOneOf(String[] words) {
    for (String word : words) {
      if (check(word)) return true;
    }
    return false;
  }

  private TsqlAst.Expr parseExpr() {
    return parseOr();
  }

  private TsqlAst.Expr parseOr() {
    TsqlAst.Expr e = parseAnd();
    while (check("OR")) {
      bump();
      e = new TsqlAst.Binary("OR", e, parseAnd());
    }
    return e;
  }

  private TsqlAst.Expr parseAnd() {
    TsqlAst.Expr e = parseNot();
    while (check("AND")) {
      bump();
      e = new TsqlAst.Binary("AND", e, parseNot());
    }
    return e;
  }

  private TsqlAst.Expr parseNot() {
    if (check("NOT")) {
      bump();
      return new TsqlAst.Unary("NOT", parseNot());
    }
    return parseCmp();
  }

  private TsqlAst.Expr parseCmp() {
    TsqlAst.Expr e = parseAdd();
    if (check("=") || check("!=") || check("<>") || check("<") || check(">")
        || check("<=") || check(">=")) {
      String op = bump().text;
      e = new TsqlAst.Binary(op, e, parseAdd());
    }
    if (check("IS")) {
      bump();
      boolean neg = false;
      if (check("NOT")) {
        bump();
        neg = true;
      }
      expect("NULL");
      e = new TsqlAst.IsNull(e, neg);
    }
    return e;
  }

  private TsqlAst.Expr parseAdd() {
    TsqlAst.Expr e = parsePrimary();
    while (check("+") || check("-") || check("||")) {
      String op = bump().text;
      e = new TsqlAst.Binary(op, e, parsePrimary());
    }
    return e;
  }

  private TsqlAst.Expr parsePrimary() {
    if (check("%")) rejectTypeAttribute();
    if (check("-")) {
      bump();
      return new TsqlAst.Binary("-", new TsqlAst.Literal("0", false), parsePrimary());
    }
    if (check("(")) {
      bump();
      TsqlAst.Expr e = parseExpr();
      expect(")");
      return e;
    }
    if (checkString()) return new TsqlAst.Literal(unescape(bump().text), true);
    if (checkNumber()) return new TsqlAst.Literal(bump().text, false);
    if (check("NULL")) {
      bump();
      return new TsqlAst.Literal(null, false);
    }
    if (checkIdent()) {
      String name = bump().text;
      if (check(".") && cursors.contains(name.toLowerCase(Locale.ROOT))) {
        bump();
        return new TsqlAst.CursorField(name, ident());
      }
      if (check("(")) {
        bump();
        TsqlAst.Call call = new TsqlAst.Call();
        call.name = name;
        if (!check(")")) {
          do {
            if (check(",")) bump();
            call.args.add(parseExpr());
            if (check("AS")) {
              bump();
              ident();
              if (check("(")) {
                bump();
                readRawBalanced();
              }
            }
          } while (check(","));
        }
        expect(")");
        return call;
      }
      if (name.equalsIgnoreCase("SQLCODE")) return new TsqlAst.Var("sqlcode");
      if (scope.contains(name.toLowerCase(Locale.ROOT))) return new TsqlAst.Var(name);
      return new TsqlAst.Literal(name, false);
    }
    throw new TsqlParser.ParseException("Expected expression at " + peekText());
  }

  private String rewriteSql(String sql) {
    if (sql == null || sql.isEmpty() || scope.isEmpty()) return sql;
    StringBuilder out = new StringBuilder();
    int i = 0;
    while (i < sql.length()) {
      char c = sql.charAt(i);
      if (c == '-' && i + 1 < sql.length() && sql.charAt(i + 1) == '-') {
        int j = sql.indexOf('\n', i);
        if (j < 0) j = sql.length();
        out.append(sql, i, j);
        i = j;
        continue;
      }
      if (c == '/' && i + 1 < sql.length() && sql.charAt(i + 1) == '*') {
        int j = sql.indexOf("*/", i + 2);
        j = j < 0 ? sql.length() : j + 2;
        out.append(sql, i, j);
        i = j;
        continue;
      }
      if (c == '\'' || c == '"') {
        int j = skipQuoted(sql, i, c);
        out.append(sql, i, j);
        i = j;
        continue;
      }
      if (c == '`') {
        int j = sql.indexOf('`', i + 1);
        j = j < 0 ? sql.length() : j + 1;
        out.append(sql, i, j);
        i = j;
        continue;
      }
      if (c == '@') {
        int j = i + 1;
        while (j < sql.length() && isNameChar(sql.charAt(j))) j++;
        out.append(sql, i, j);
        i = j;
        continue;
      }
      if (Character.isLetter(c) || c == '_') {
        int j = i + 1;
        while (j < sql.length() && isNameChar(sql.charAt(j))) j++;
        String ident = sql.substring(i, j);
        if (scope.contains(ident.toLowerCase(Locale.ROOT)) && !sqlKeyword(ident)
            && !precededByDot(out)) {
          out.append('@').append(ident);
        } else {
          out.append(ident);
        }
        i = j;
        continue;
      }
      out.append(c);
      i++;
    }
    return out.toString();
  }

  private static boolean precededByDot(StringBuilder out) {
    int k = out.length() - 1;
    while (k >= 0 && Character.isWhitespace(out.charAt(k))) k--;
    return k >= 0 && out.charAt(k) == '.';
  }

  private static boolean sqlKeyword(String name) {
    String upper = name.toUpperCase(Locale.ROOT);
    return upper.equals("AS") || upper.equals("FROM") || upper.equals("WHERE")
        || upper.equals("SELECT") || upper.equals("AND") || upper.equals("OR")
        || upper.equals("ON") || upper.equals("JOIN") || upper.equals("INTO")
        || upper.equals("SET") || upper.equals("IN") || upper.equals("IS")
        || upper.equals("NULL") || upper.equals("NOT") || upper.equals("BY")
        || upper.equals("ORDER") || upper.equals("GROUP") || upper.equals("LIMIT")
        || upper.equals("INSERT") || upper.equals("VALUES") || upper.equals("TABLE");
  }

  private String readRawSql() {
    StringBuilder sb = new StringBuilder();
    int depth = 0;
    while (!isEof()) {
      Tok t = peek();
      if (depth == 0 && (t.text.equals(";") || t.text.equalsIgnoreCase("GO"))) break;
      if (depth == 0 && isBoundary(t) && sb.length() > 0 && !isIfExists(t)) break;
      if (t.text.equals("(")) depth++;
      if (t.text.equals(")")) depth--;
      appendTok(sb, t);
      bump();
    }
    return sb.toString().trim();
  }

  private String readUntilCloseParen() {
    StringBuilder sb = new StringBuilder();
    int depth = 1;
    while (!isEof() && depth > 0) {
      Tok t = peek();
      if (t.text.equals("(")) depth++;
      if (t.text.equals(")")) {
        depth--;
        if (depth == 0) break;
      }
      appendTok(sb, t);
      bump();
    }
    return sb.toString().trim();
  }

  private void readRawBalanced() {
    int depth = 1;
    while (!isEof() && depth > 0) {
      Tok t = bump();
      if (t.text.equals("(")) depth++;
      if (t.text.equals(")")) depth--;
    }
  }

  private static void appendTok(StringBuilder sb, Tok t) {
    if (sb.length() > 0 && t.leadingSpace) sb.append(' ');
    if (t.kind == Kind.STRING) sb.append('\'').append(t.text).append('\'');
    else sb.append(t.text);
  }

  private String joinTokens(int from, int to) {
    StringBuilder sb = new StringBuilder();
    for (int i = from; i < to; i++) appendTok(sb, toks.get(i));
    return sb.toString().trim();
  }

  /** {@code IF EXISTS} and {@code IF NOT EXISTS} belong to the SQL statement. */
  private boolean isIfExists(Tok t) {
    if (!t.text.equalsIgnoreCase("IF")) return false;
    return lookaheadIs(1, "EXISTS") || (lookaheadIs(1, "NOT") && lookaheadIs(2, "EXISTS"));
  }

  private boolean isBoundary(Tok t) {
    String text = t.text;
    return text.equalsIgnoreCase("BEGIN") || text.equalsIgnoreCase("END")
        || text.equalsIgnoreCase("IF") || text.equalsIgnoreCase("ELSE")
        || text.equalsIgnoreCase("WHILE") || text.equalsIgnoreCase("LOOP")
        || text.equalsIgnoreCase("FOR") || text.equalsIgnoreCase("DECLARE")
        || text.equalsIgnoreCase("SET") || text.equalsIgnoreCase("EXEC")
        || text.equalsIgnoreCase("EXECUTE") || text.equalsIgnoreCase("RETURN")
        || text.equalsIgnoreCase("LEAVE") || text.equalsIgnoreCase("OPEN")
        || text.equalsIgnoreCase("FETCH") || text.equalsIgnoreCase("CLOSE")
        || text.equalsIgnoreCase("CALL") || text.equalsIgnoreCase("EXCEPTION")
        || text.equalsIgnoreCase("CREATE") || text.equalsIgnoreCase("GO");
  }

  private boolean isCreateProcedure() {
    int i = 1;
    if (p + i < toks.size() && toks.get(p + i).text.equalsIgnoreCase("OR")) i += 2;
    return p + i < toks.size()
        && (toks.get(p + i).text.equalsIgnoreCase("PROCEDURE")
            || toks.get(p + i).text.equalsIgnoreCase("PROC"));
  }

  private void expectDots() {
    if (check("..")) {
      bump();
      return;
    }
    if (check(".") && lookaheadIs(1, ".")) {
      bump();
      bump();
      return;
    }
    throw new TsqlParser.ParseException("Expected .. at " + peekText());
  }

  private void skipSemi() {
    while (check(";") || check("GO")) bump();
  }

  private static void rejectTypeAttribute() {
    throw new TsqlParser.ParseException("%TYPE and %ROWTYPE are not supported");
  }

  private static List<TsqlAst.Stmt> one(TsqlAst.Stmt stmt) {
    List<TsqlAst.Stmt> out = new ArrayList<TsqlAst.Stmt>();
    out.add(stmt);
    return out;
  }

  private boolean isEof() {
    return p >= toks.size();
  }

  private boolean isEofAfter(int n) {
    return p + n >= toks.size();
  }

  private Tok peek() {
    if (isEof()) return new Tok(Kind.EOF, "", false);
    return toks.get(p);
  }

  private String peekText() {
    return peek().text;
  }

  private boolean check(String text) {
    return !isEof() && toks.get(p).text.equalsIgnoreCase(text);
  }

  private boolean lookaheadIs(int n, String text) {
    return p + n < toks.size() && toks.get(p + n).text.equalsIgnoreCase(text);
  }

  private boolean checkString() {
    return !isEof() && toks.get(p).kind == Kind.STRING;
  }

  private boolean checkNumber() {
    return !isEof() && toks.get(p).kind == Kind.NUMBER;
  }

  private boolean checkIdent() {
    return !isEof() && toks.get(p).kind == Kind.IDENT;
  }

  private Tok bump() {
    if (isEof()) throw new TsqlParser.ParseException("Unexpected end of script");
    return toks.get(p++);
  }

  private void expect(String text) {
    if (!check(text)) throw new TsqlParser.ParseException("Expected " + text + " at " + peekText());
    bump();
  }

  private String ident() {
    if (!checkIdent()) throw new TsqlParser.ParseException("Expected identifier at " + peekText());
    return bump().text;
  }

  private static String unescape(String quotedBody) {
    return quotedBody.replace("''", "'");
  }

  private enum Kind { IDENT, STRING, NUMBER, SYMBOL, EOF }

  private static final class Tok {
    final Kind kind;
    final String text;
    final boolean leadingSpace;

    Tok(Kind kind, String text, boolean leadingSpace) {
      this.kind = kind;
      this.text = text;
      this.leadingSpace = leadingSpace;
    }
  }

  static List<Tok> lex(String sql) {
    List<Tok> out = new ArrayList<Tok>();
    int i = 0;
    boolean space = false;
    while (i < sql.length()) {
      char c = sql.charAt(i);
      if (Character.isWhitespace(c)) {
        space = true;
        i++;
        continue;
      }
      if (c == '-' && i + 1 < sql.length() && sql.charAt(i + 1) == '-') {
        while (i < sql.length() && sql.charAt(i) != '\n') i++;
        space = true;
        continue;
      }
      if (c == '/' && i + 1 < sql.length() && sql.charAt(i + 1) == '*') {
        i += 2;
        while (i + 1 < sql.length() && !(sql.charAt(i) == '*' && sql.charAt(i + 1) == '/')) i++;
        i = Math.min(sql.length(), i + 2);
        space = true;
        continue;
      }
      if (c == '\'' || c == '"') {
        int j = i + 1;
        StringBuilder sb = new StringBuilder();
        while (j < sql.length()) {
          if (sql.charAt(j) == c && j + 1 < sql.length() && sql.charAt(j + 1) == c) {
            sb.append(c).append(c);
            j += 2;
            continue;
          }
          if (sql.charAt(j) == c) break;
          sb.append(sql.charAt(j));
          j++;
        }
        out.add(new Tok(Kind.STRING, sb.toString(), space));
        space = false;
        i = j + 1;
        continue;
      }
      if (Character.isDigit(c)) {
        int j = i + 1;
        while (j < sql.length() && Character.isDigit(sql.charAt(j))) j++;
        if (j < sql.length() && sql.charAt(j) == '.'
            && !(j + 1 < sql.length() && sql.charAt(j + 1) == '.')) {
          j++;
          while (j < sql.length() && Character.isDigit(sql.charAt(j))) j++;
        }
        out.add(new Tok(Kind.NUMBER, sql.substring(i, j), space));
        space = false;
        i = j;
        continue;
      }
      if (Character.isLetter(c) || c == '_') {
        int j = i + 1;
        while (j < sql.length() && isNameChar(sql.charAt(j))) j++;
        out.add(new Tok(Kind.IDENT, sql.substring(i, j), space));
        space = false;
        i = j;
        continue;
      }
      if (i + 1 < sql.length()) {
        String two = sql.substring(i, i + 2);
        if (two.equals("||") || two.equals("..") || two.equals("!=") || two.equals("<>")
            || two.equals("<=") || two.equals(">=")) {
          out.add(new Tok(Kind.SYMBOL, two, space));
          space = false;
          i += 2;
          continue;
        }
      }
      if (c == '%') {
        throw new TsqlParser.ParseException("%TYPE and %ROWTYPE are not supported");
      }
      out.add(new Tok(Kind.SYMBOL, String.valueOf(c), space));
      space = false;
      i++;
    }
    return out;
  }

  private static boolean isNameChar(char c) {
    return Character.isLetterOrDigit(c) || c == '_';
  }

  private static int skipQuoted(String sql, int i, char q) {
    int j = i + 1;
    while (j < sql.length()) {
      if (sql.charAt(j) == q && j + 1 < sql.length() && sql.charAt(j + 1) == q) {
        j += 2;
        continue;
      }
      if (sql.charAt(j) == q) return j + 1;
      j++;
    }
    return sql.length();
  }

}
