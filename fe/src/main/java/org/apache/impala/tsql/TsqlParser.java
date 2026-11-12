package org.apache.impala.tsql;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Recursive-descent parser for the T-SQL subset accepted by this build.
 * LPSQL spellings (EXECUTE IMMEDIATE, FOR i IN, SELECT INTO, a leading LPSQL
 * wrapper) are syntax errors.
 */
public final class TsqlParser {
  public static final class ParseException extends RuntimeException {
    public ParseException(String message) {
      super(message);
    }
  }

  private final List<Tok> toks;
  private int p;

  private TsqlParser(List<Tok> toks) {
    this.toks = toks;
  }

  public static boolean looksLikeTsql(String sql) {
    if (sql == null) return false;
    String head = firstKeyword(sql);
    if (head == null) return false;
    if (head.equals("DECLARE") || head.equals("BEGIN") || head.equals("EXEC")
        || head.equals("EXECUTE") || head.equals("CALL")) {
      return true;
    }
    if (head.equals("SHOW")) {
      String rest = sql.replaceAll("(?s)/\\*.*?\\*/", " ").replaceAll("--.*?(\\n|$)", " ");
      return rest.toUpperCase(Locale.ROOT).contains("PROCEDURE");
    }
    if (!head.equals("CREATE")) return false;
    String upper = stripComments(sql).trim().toUpperCase(Locale.ROOT);
    return upper.startsWith("CREATE PROC") || upper.startsWith("CREATE OR ALTER PROC");
  }

  public static TsqlAst.Script parse(String sql) {
    if (sql != null && stripComments(sql).trim().toUpperCase(Locale.ROOT).startsWith("LPSQL")) {
      throw new ParseException("LPSQL spelling is not accepted");
    }
    List<Tok> toks = lex(sql == null ? "" : sql);
    TsqlParser parser = new TsqlParser(toks);
    TsqlAst.Script script = new TsqlAst.Script();
    parser.skipSemi();
    while (!parser.isEof()) {
      script.statements.add(parser.parseStmt());
      parser.skipSemi();
    }
    validateBareSelect(script.statements, true);
    return script;
  }

  private static void validateBareSelect(List<TsqlAst.Stmt> stmts, boolean outermost) {
    for (int i = 0; i < stmts.size(); i++) {
      TsqlAst.Stmt s = stmts.get(i);
      if (s instanceof TsqlAst.BareSelect) {
        if (!outermost || i != stmts.size() - 1) {
          throw new ParseException(
              "A bare SELECT is allowed only as the last statement of the outermost body");
        }
      } else if (s instanceof TsqlAst.IfStmt) {
        TsqlAst.IfStmt iff = (TsqlAst.IfStmt) s;
        validateBareSelect(iff.thenBody, false);
        validateBareSelect(iff.elseBody, false);
      } else if (s instanceof TsqlAst.WhileStmt) {
        validateBareSelect(((TsqlAst.WhileStmt) s).body, false);
      } else if (s instanceof TsqlAst.TryStmt) {
        TsqlAst.TryStmt t = (TsqlAst.TryStmt) s;
        validateBareSelect(t.body, false);
        validateBareSelect(t.handler, false);
      } else if (s instanceof TsqlAst.CreateProc) {
        validateBareSelect(((TsqlAst.CreateProc) s).body, true);
      }
    }
  }

  private TsqlAst.Stmt parseStmt() {
    if (check("USE")) {
      throw new ParseException("USE is not allowed inside a procedure or batch");
    }
    if (check("CREATE")) {
      if (isCreateProcedure()) return parseCreate();
      return new TsqlAst.RawSql(readRawSql());
    }
    if (check("DECLARE")) return parseDeclare();
    if (check("SET")) return parseSet();
    if (check("SELECT")) return parseSelect();
    if (check("IF")) return parseIf();
    if (check("WHILE")) return parseWhile();
    if (check("BEGIN")) return parseBegin();
    if (check("EXEC") || check("EXECUTE")) return parseExec();
    if (check("CALL")) return parseCall();
    if (check("RETURN")) {
      bump();
      return new TsqlAst.ReturnStmt();
    }
    if (check("BREAK")) {
      bump();
      return new TsqlAst.BreakStmt();
    }
    if (check("CONTINUE")) {
      bump();
      return new TsqlAst.ContinueStmt();
    }
    if (check("OPEN")) {
      bump();
      return new TsqlAst.OpenCursor(plainName());
    }
    if (check("FETCH")) return parseFetch();
    if (check("CLOSE")) {
      bump();
      return new TsqlAst.CloseCursor(plainName());
    }
    if (check("DEALLOCATE")) {
      bump();
      return new TsqlAst.DeallocateCursor(plainName());
    }
    if (check("FOR")) {
      throw new ParseException("FOR loops are not T-SQL");
    }
    if (check("SHOW")) return new TsqlAst.RawSql(readRawSql());
    return new TsqlAst.RawSql(readRawSql());
  }

  private TsqlAst.Stmt parseCreate() {
    bump(); // CREATE
    boolean alter = false;
    if (check("OR")) {
      bump();
      expect("ALTER");
      alter = true;
    }
    if (!(check("PROCEDURE") || check("PROC"))) {
      // CREATE TABLE and the rest of Impala DDL are not T-SQL batches.
      throw new ParseException("Only CREATE PROCEDURE is accepted as a T-SQL statement");
    }
    bump();
    TsqlAst.CreateProc proc = new TsqlAst.CreateProc();
    proc.name = ident();
    if (check("(")) {
      bump();
      if (!check(")")) parseParams(proc);
      expect(")");
    } else if (checkAt()) {
      parseParams(proc);
    }
    if (check("AS")) bump();
    if (check("WITH")) {
      bump();
      expect("EXECUTE");
      expect("AS");
      expect("OWNER");
      proc.executeAsOwner = true;
    }
    if (check("AS")) bump();
    proc.body.addAll(parseBody());
    proc.source = "tsql\n";
    if (!alter) {
      // source text is attached by the caller when it has the original string
    }
    return proc;
  }

  private void parseParams(TsqlAst.CreateProc proc) {
    do {
      if (check(",")) bump();
      String name = atName();
      String type = ident();
      if (check("(")) {
        bump();
        readRawBalanced();
      }
      boolean output = false;
      if (check("OUTPUT") || check("OUT")) {
        bump();
        output = true;
      }
      proc.params.add(new TsqlAst.Param(name, type, output));
    } while (check(",") || checkAt());
  }

  private TsqlAst.Stmt parseDeclare() {
    bump();
    if (checkAt()) {
      String name = atName();
      if (check("CURSOR")) {
        throw new ParseException("Declare the cursor name before CURSOR: DECLARE c CURSOR FOR ...");
      }
      String type = ident();
      if (check("(")) {
        bump();
        readRawBalanced();
      }
      TsqlAst.Expr init = null;
      if (check("=")) {
        bump();
        init = parseExpr();
      }
      return new TsqlAst.DeclareVar(name, type, init);
    }
    String cursor = plainName();
    expect("CURSOR");
    expect("FOR");
    if (checkAt() || checkString()) {
      return new TsqlAst.DeclareCursor(cursor, null, parseExpr());
    }
    return new TsqlAst.DeclareCursor(cursor, readRawSql(), null);
  }

  private TsqlAst.Stmt parseSet() {
    bump();
    String name = atName();
    expect("=");
    return new TsqlAst.SetStmt(name, parseExpr());
  }

  private TsqlAst.Stmt parseSelect() {
    int start = p;
    bump(); // SELECT
    if (checkAt() && lookaheadIs(1, "=")) {
      TsqlAst.SelectAssign assign = new TsqlAst.SelectAssign();
      StringBuilder sql = new StringBuilder("SELECT ");
      boolean first = true;
      do {
        if (check(",")) bump();
        String target = atName();
        expect("=");
        assign.targets.add(target);
        String piece = readSelectExpr();
        if (!first) sql.append(", ");
        sql.append(piece);
        first = false;
      } while (check(","));
      if (!isEof() && !check(";") && !check("GO") && !boundaryKeyword()) {
        sql.append(' ').append(readRawSql());
      }
      assign.sql = sql.toString().trim();
      return assign;
    }
    p = start;
    String sql = readRawSql();
    String upper = sql.toUpperCase(Locale.ROOT);
    if (upper.contains(" INTO ")) {
      throw new ParseException("SELECT INTO is not T-SQL. Use SELECT @var = ...");
    }
    return new TsqlAst.BareSelect(sql);
  }

  private TsqlAst.Stmt parseIf() {
    bump();
    TsqlAst.IfStmt iff = new TsqlAst.IfStmt();
    iff.condition = parseExpr();
    iff.thenBody.addAll(parseBody());
    if (check("ELSE")) {
      bump();
      iff.elseBody.addAll(parseBody());
    }
    return iff;
  }

  private TsqlAst.Stmt parseWhile() {
    bump();
    TsqlAst.WhileStmt w = new TsqlAst.WhileStmt();
    w.condition = parseExpr();
    w.body.addAll(parseBody());
    return w;
  }

  private TsqlAst.Stmt parseBegin() {
    bump();
    if (check("TRY")) {
      bump();
      TsqlAst.TryStmt t = new TsqlAst.TryStmt();
      t.body.addAll(parseStmtsUntil("END"));
      expect("END");
      expect("TRY");
      expect("BEGIN");
      expect("CATCH");
      t.handler.addAll(parseStmtsUntil("END"));
      expect("END");
      expect("CATCH");
      return t;
    }
    List<TsqlAst.Stmt> body = parseStmtsUntil("END");
    expect("END");
    if (body.size() == 1) return body.get(0);
    TsqlAst.TryStmt block = new TsqlAst.TryStmt();
    block.body.addAll(body);
    return block;
  }

  private List<TsqlAst.Stmt> parseBody() {
    if (check("BEGIN")) {
      TsqlAst.Stmt s = parseBegin();
      if (s instanceof TsqlAst.TryStmt && ((TsqlAst.TryStmt) s).handler.isEmpty()) {
        return ((TsqlAst.TryStmt) s).body;
      }
      List<TsqlAst.Stmt> one = new ArrayList<TsqlAst.Stmt>();
      one.add(s);
      return one;
    }
    List<TsqlAst.Stmt> one = new ArrayList<TsqlAst.Stmt>();
    one.add(parseStmt());
    skipSemi();
    return one;
  }

  private List<TsqlAst.Stmt> parseStmtsUntil(String endKw) {
    List<TsqlAst.Stmt> out = new ArrayList<TsqlAst.Stmt>();
    skipSemi();
    while (!isEof() && !check(endKw)) {
      out.add(parseStmt());
      skipSemi();
    }
    return out;
  }

  private TsqlAst.Stmt parseExec() {
    String kw = bump().text;
    if (check("IMMEDIATE")) {
      throw new ParseException("EXECUTE IMMEDIATE is not T-SQL. Use EXEC(@sql)");
    }
    if (check("(") || checkString() || checkAt()) {
      if (check("(")) bump();
      TsqlAst.Expr text = parseExpr();
      if (check(")")) bump();
      return new TsqlAst.ExecString(text);
    }
    return finishExecProc(ident());
  }

  private TsqlAst.Stmt parseCall() {
    bump();
    String name = ident();
    TsqlAst.ExecProc call = new TsqlAst.ExecProc();
    call.name = name;
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

  private TsqlAst.Stmt finishExecProc(String name) {
    TsqlAst.ExecProc call = new TsqlAst.ExecProc();
    call.name = name;
    while (!isEof() && !check(";") && !check("GO") && !boundaryKeyword()) {
      if (check(",")) bump();
      String param = null;
      TsqlAst.Expr value;
      if (checkAt() && lookaheadIs(1, "=")) {
        param = atName();
        expect("=");
        value = parseExpr();
      } else {
        value = parseExpr();
      }
      boolean output = false;
      if (check("OUTPUT") || check("OUT")) {
        bump();
        output = true;
      }
      call.args.add(new TsqlAst.Arg(param, value, output));
      if (!check(",") && !(checkAt() && lookaheadIs(1, "=")) && !checkString()
          && !checkNumber() && !checkAt()) {
        break;
      }
    }
    return call;
  }

  private TsqlAst.Stmt parseFetch() {
    bump();
    if (check("NEXT")) bump();
    if (check("FROM")) bump();
    TsqlAst.FetchCursor fetch = new TsqlAst.FetchCursor();
    fetch.name = plainName();
    expect("INTO");
    do {
      if (check(",")) bump();
      fetch.targets.add(atName());
    } while (check(","));
    return fetch;
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
    while (check("+") || check("-")) {
      String op = bump().text;
      e = new TsqlAst.Binary(op, e, parsePrimary());
    }
    return e;
  }

  private TsqlAst.Expr parsePrimary() {
    if (check("-")) {
      // Leading minus is numeric negation. Unary is reserved for NOT.
      bump();
      return new TsqlAst.Binary("-", new TsqlAst.Literal("0", false), parsePrimary());
    }
    if (check("(")) {
      bump();
      TsqlAst.Expr e = parseExpr();
      expect(")");
      return e;
    }
    if (checkString()) {
      return new TsqlAst.Literal(unescape(bump().text), true);
    }
    if (checkNumber()) {
      return new TsqlAst.Literal(bump().text, false);
    }
    if (check("NULL")) {
      bump();
      return new TsqlAst.Literal(null, false);
    }
    if (checkAt()) {
      return new TsqlAst.Var(atName());
    }
    if (checkIdent()) {
      String name = ident();
      if (check("(")) {
        bump();
        TsqlAst.Call call = new TsqlAst.Call();
        call.name = name;
        if (!check(")")) {
          do {
            if (check(",")) bump();
            // CAST(expr AS type). The value stays argument 0; the type is not evaluated.
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
      return new TsqlAst.Literal(name, false);
    }
    throw new ParseException("Expected expression at " + peekText());
  }

  private String readRawSql() {
    StringBuilder sb = new StringBuilder();
    int depth = 0;
    while (!isEof()) {
      Tok t = peek();
      if (depth == 0 && (t.text.equals(";") || t.text.equalsIgnoreCase("GO"))) break;
      if (depth == 0 && boundaryKeyword() && sb.length() > 0) break;
      if (t.text.equals("(")) depth++;
      if (t.text.equals(")")) depth--;
      if (sb.length() > 0) sb.append(t.leadingSpace ? ' ' : "");
      if (t.kind == Kind.STRING) sb.append('\'').append(t.text).append('\'');
      else sb.append(t.text);
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

  private String readSelectExpr() {
    StringBuilder sb = new StringBuilder();
    int depth = 0;
    while (!isEof()) {
      if (depth == 0 && (check(",") || check(";") || check("GO") || check("FROM")
          || check("WHERE") || check("GROUP") || check("ORDER") || check("LIMIT")
          || check("HAVING"))) {
        break;
      }
      Tok t = peek();
      if (t.text.equals("(")) depth++;
      if (t.text.equals(")")) depth = Math.max(0, depth - 1);
      if (sb.length() > 0) sb.append(' ');
      if (t.kind == Kind.STRING) sb.append('\'').append(t.text).append('\'');
      else sb.append(t.text);
      bump();
    }
    return sb.toString();
  }

  private String textFrom(int from, int to) {
    StringBuilder sb = new StringBuilder();
    for (int i = from; i < to; i++) {
      Tok t = toks.get(i);
      if (sb.length() > 0) sb.append(' ');
      if (t.kind == Kind.STRING) sb.append('\'').append(t.text).append('\'');
      else sb.append(t.text);
    }
    return sb.toString();
  }

  private boolean boundaryKeyword() {
    return check("BEGIN") || check("END") || check("IF") || check("ELSE") || check("WHILE")
        || check("DECLARE") || check("SET") || check("EXEC") || check("EXECUTE")
        || check("RETURN") || check("BREAK") || check("CONTINUE") || check("OPEN")
        || check("FETCH") || check("CLOSE") || check("DEALLOCATE") || check("CREATE")
        || check("GO");
  }

  private void skipSemi() {
    while (check(";") || check("GO")) bump();
  }

  private boolean isEof() {
    return p >= toks.size();
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

  private boolean checkAt() {
    return !isEof() && toks.get(p).kind == Kind.AT;
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
    if (isEof()) throw new ParseException("Unexpected end of script");
    return toks.get(p++);
  }

  private void expect(String text) {
    if (!check(text)) throw new ParseException("Expected " + text + " at " + peekText());
    bump();
  }

  private String ident() {
    if (!checkIdent() && !isKeyword(peekText())) {
      throw new ParseException("Expected identifier at " + peekText());
    }
    return bump().text;
  }

  private boolean isCreateProcedure() {
    int i = 1;
    if (p + i < toks.size() && toks.get(p + i).text.equalsIgnoreCase("OR")) {
      i += 2; // ALTER
    }
    return p + i < toks.size()
        && (toks.get(p + i).text.equalsIgnoreCase("PROCEDURE")
            || toks.get(p + i).text.equalsIgnoreCase("PROC"));
  }

  private String plainName() {
    if (checkAt()) return atName();
    return ident();
  }

  private String atName() {
    if (!checkAt()) throw new ParseException("Expected @name at " + peekText());
    String raw = bump().text;
    while (raw.startsWith("@")) raw = raw.substring(1);
    return raw;
  }

  private static String unescape(String quotedBody) {
    return quotedBody.replace("''", "'");
  }

  private enum Kind { IDENT, AT, STRING, NUMBER, SYMBOL, EOF }

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
      if (c == '\'') {
        int j = i + 1;
        StringBuilder sb = new StringBuilder();
        while (j < sql.length()) {
          if (sql.charAt(j) == '\'' && j + 1 < sql.length() && sql.charAt(j + 1) == '\'') {
            sb.append("''");
            j += 2;
            continue;
          }
          if (sql.charAt(j) == '\'') break;
          sb.append(sql.charAt(j));
          j++;
        }
        out.add(new Tok(Kind.STRING, sb.toString(), space));
        space = false;
        i = j + 1;
        continue;
      }
      if (c == '@') {
        int j = i + 1;
        while (j < sql.length() && (Character.isLetterOrDigit(sql.charAt(j)) || sql.charAt(j) == '_')) j++;
        String name = "@" + sql.substring(i + 1, j);
        if (name.equals("@")) name = "@@";
        if (sql.startsWith("@@", i)) {
          j = i + 2;
          while (j < sql.length() && (Character.isLetterOrDigit(sql.charAt(j)) || sql.charAt(j) == '_')) j++;
          name = sql.substring(i, j);
        }
        out.add(new Tok(Kind.AT, name, space));
        space = false;
        i = j;
        continue;
      }
      if (Character.isDigit(c)) {
        int j = i + 1;
        while (j < sql.length() && (Character.isDigit(sql.charAt(j)) || sql.charAt(j) == '.')) j++;
        out.add(new Tok(Kind.NUMBER, sql.substring(i, j), space));
        space = false;
        i = j;
        continue;
      }
      if (Character.isLetter(c) || c == '_') {
        int j = i + 1;
        while (j < sql.length() && (Character.isLetterOrDigit(sql.charAt(j)) || sql.charAt(j) == '_')) j++;
        out.add(new Tok(Kind.IDENT, sql.substring(i, j), space));
        space = false;
        i = j;
        continue;
      }
      if (i + 1 < sql.length()) {
        String two = sql.substring(i, i + 2);
        if (two.equals("!=") || two.equals("<>") || two.equals("<=") || two.equals(">=")) {
          out.add(new Tok(Kind.SYMBOL, two, space));
          space = false;
          i += 2;
          continue;
        }
      }
      out.add(new Tok(Kind.SYMBOL, String.valueOf(c), space));
      space = false;
      i++;
    }
    return out;
  }

  private static boolean isKeyword(String text) {
    return text != null && text.length() > 0;
  }

  private static String firstKeyword(String sql) {
    String stripped = stripComments(sql).trim();
    if (stripped.isEmpty()) return null;
    int i = 0;
    while (i < stripped.length() && Character.isLetter(stripped.charAt(i))) i++;
    if (i == 0) return null;
    return stripped.substring(0, i).toUpperCase(Locale.ROOT);
  }

  private static String stripComments(String sql) {
    return sql.replaceAll("(?s)/\\*.*?\\*/", " ").replaceAll("--.*?(\\n|$)", " ");
  }
}
