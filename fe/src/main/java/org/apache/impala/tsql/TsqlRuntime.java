package org.apache.impala.tsql;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.impala.lpsql.LpsqlParser;

/**
 * Interprets a T-SQL script one Impala statement at a time.
 * {@link #next} returns the next statement to run. The caller feeds the result
 * back on the following call. Query options changed with SET apply only until
 * the script returns.
 */
public final class TsqlRuntime {
  public enum Kind { SQL, CLIENT_RESULT, FINISHED, FAILED }

  public static final class Step {
    public final Kind kind;
    public final String sql;
    public final String errorMessage;
    public final int sqlcode;
    public final Map<String, String> procedureOptions;

    private Step(Kind kind, String sql, String errorMessage, int sqlcode,
        Map<String, String> procedureOptions) {
      this.kind = kind;
      this.sql = sql;
      this.errorMessage = errorMessage;
      this.sqlcode = sqlcode;
      this.procedureOptions = procedureOptions;
    }

    public static Step sql(String sql, Map<String, String> options) {
      return new Step(Kind.SQL, sql, null, 0, options);
    }

    public static Step client(String sql, Map<String, String> options) {
      return new Step(Kind.CLIENT_RESULT, sql, null, 0, options);
    }

    public static Step finished(String message) {
      return new Step(Kind.FINISHED, null, message, 0, new HashMap<String, String>());
    }

    public static Step failed(String message) {
      return new Step(Kind.FAILED, null, message, -1, new HashMap<String, String>());
    }
  }

  private static final class Cursor {
    List<String> columns = new ArrayList<String>();
    List<TsqlSqlRunner.ValueKind> kinds = new ArrayList<TsqlSqlRunner.ValueKind>();
    List<List<TsqlSqlRunner.Cell>> rows = new ArrayList<List<TsqlSqlRunner.Cell>>();
    int index = -1;
    boolean open;
  }

  /** A procedure value. A null kind means the declared type, or the text, decides. */
  static final class Value {
    static final Value NULL = new Value(null, TsqlSqlRunner.ValueKind.TEXT);
    final String text;
    final TsqlSqlRunner.ValueKind kind;

    Value(String text, TsqlSqlRunner.ValueKind kind) {
      this.text = text;
      this.kind = kind;
    }
  }

  private static final class Frame {
    final List<TsqlAst.Stmt> body;
    int index;
    final TsqlAst.Expr whileCond;
    final List<TsqlAst.Stmt> handler;
    boolean handling;

    Frame(List<TsqlAst.Stmt> body, TsqlAst.Expr whileCond, List<TsqlAst.Stmt> handler) {
      this.body = body;
      this.whileCond = whileCond;
      this.handler = handler;
    }
  }

  private final TsqlProcedureStore store;
  private final String database;
  private final String user;
  private final TsqlAccess access;
  private final Map<String, Value> vars = new HashMap<String, Value>();
  /** Null value means the kind is inferred from the assigned text. */
  private final Map<String, TsqlSqlRunner.ValueKind> declared =
      new HashMap<String, TsqlSqlRunner.ValueKind>();
  private static final Pattern SQL_NUMBER =
      Pattern.compile("[+-]?(\\d+(\\.\\d*)?|\\.\\d+)([eE][+-]?\\d+)?");
  private final Map<String, String> procedureOptions = new HashMap<String, String>();
  private final Map<String, Cursor> cursors = new HashMap<String, Cursor>();
  private final List<Frame> stack = new ArrayList<Frame>();
  private final String originalSource;
  private final Map<String, TsqlAst.CreateProc> defined =
      new HashMap<String, TsqlAst.CreateProc>();
  private final Map<String, String> outputs = new HashMap<String, String>();
  private String waitSql;
  private List<String> waitAssign;
  private String waitCursor;
  private boolean waitClient;
  private boolean stop;
  private String failMessage;
  private int fetchStatus = -1;
  private int lastError;
  private int sqlCode;

  public TsqlRuntime(String source, String database, String user, TsqlProcedureStore store) {
    this(source, database, user, store, TsqlAccess.OWNER_ONLY);
  }

  public TsqlRuntime(String source, String database, String user, TsqlProcedureStore store,
      TsqlAccess access) {
    this.database = database == null ? "default" : database;
    this.user = user == null ? "" : user;
    this.access = access == null ? TsqlAccess.OWNER_ONLY : access;
    this.originalSource = source == null ? "" : source;
    this.store = store == null ? new MemoryStore() : store;
    // Each child DDL must finish before the next step reads its result.
    procedureOptions.put("enable_async_ddl_execution", "0");
    TsqlAst.Script script = parseDialect(this.originalSource);
    stack.add(new Frame(script.statements, null, null));
  }

  static TsqlAst.Script parseDialect(String source) {
    if (source == null) source = "";
    if (LpsqlParser.looksLikeLpsql(source)) return LpsqlParser.parse(source);
    if (TsqlParser.looksLikeTsql(source)) return TsqlParser.parse(source);
    throw new TsqlParser.ParseException("A procedure batch must start with LPSQL or TSQL");
  }

  public Step next(TsqlSqlRunner.Result previous) {
    if (failMessage != null) return Step.failed(failMessage);
    if (stop) return Step.finished(summary());
    try {
      if (waitSql != null) {
        applyResult(previous);
        waitSql = null;
      }
      while (true) {
        Step yielded = advance();
        if (yielded != null) return yielded;
      }
    } catch (TsqlParser.ParseException e) {
      failMessage = e.getMessage();
      return Step.failed(failMessage);
    } catch (RuntimeException e) {
      failMessage = e.getMessage();
      return Step.failed(failMessage);
    }
  }

  public Map<String, String> procedureOptions() {
    return procedureOptions;
  }

  private Step advance() {
    if (stack.isEmpty()) {
      stop = true;
      return Step.finished(summary());
    }
    Frame frame = stack.get(stack.size() - 1);
    if (frame.whileCond != null && frame.index >= frame.body.size()) {
      if (truth(eval(frame.whileCond))) frame.index = 0;
      else stack.remove(stack.size() - 1);
      return null;
    }
    if (frame.index >= frame.body.size()) {
      stack.remove(stack.size() - 1);
      return null;
    }
    TsqlAst.Stmt stmt = frame.body.get(frame.index++);
    return exec(stmt, frame);
  }

  private Step exec(TsqlAst.Stmt stmt, Frame frame) {
    if (stmt instanceof TsqlAst.DeclareVar) {
      TsqlAst.DeclareVar d = (TsqlAst.DeclareVar) stmt;
      noteType(d.name, d.typeName);
      setVar(d.name, d.initializer == null ? Value.NULL : evalValue(d.initializer));
      return null;
    }
    if (stmt instanceof TsqlAst.SetStmt) {
      TsqlAst.SetStmt s = (TsqlAst.SetStmt) stmt;
      setVar(s.name, evalValue(s.value));
      return null;
    }
    if (stmt instanceof TsqlAst.SelectAssign) {
      TsqlAst.SelectAssign s = (TsqlAst.SelectAssign) stmt;
      waitAssign = s.targets;
      waitSql = rewrite(s.sql);
      return Step.sql(waitSql, copyOptions());
    }
    if (stmt instanceof TsqlAst.BareSelect) {
      waitClient = true;
      waitSql = rewrite(((TsqlAst.BareSelect) stmt).sql);
      return Step.client(waitSql, copyOptions());
    }
    if (stmt instanceof TsqlAst.RawSql) {
      String sql = rewrite(((TsqlAst.RawSql) stmt).sql);
      if (sql == null || sql.trim().isEmpty()) return null;
      if (isSetOption(sql)) {
        applySetOption(sql);
        return null;
      }
      waitSql = sql;
      return Step.sql(sql, copyOptions());
    }
    if (stmt instanceof TsqlAst.ExecString) {
      String text = asString(eval(((TsqlAst.ExecString) stmt).text));
      if (text != null && isSetOption(text)) {
        applySetOption(text);
        return null;
      }
      waitSql = text;
      return Step.sql(text, copyOptions());
    }
    if (stmt instanceof TsqlAst.ExecProc) {
      pushProc((TsqlAst.ExecProc) stmt);
      return null;
    }
    if (stmt instanceof TsqlAst.IfStmt) {
      TsqlAst.IfStmt s = (TsqlAst.IfStmt) stmt;
      List<TsqlAst.Stmt> branch = truth(eval(s.condition)) ? s.thenBody : s.elseBody;
      stack.add(new Frame(branch, null, null));
      return null;
    }
    if (stmt instanceof TsqlAst.WhileStmt) {
      TsqlAst.WhileStmt s = (TsqlAst.WhileStmt) stmt;
      if (truth(eval(s.condition))) {
        stack.add(new Frame(s.body, s.condition, null));
      }
      return null;
    }
    if (stmt instanceof TsqlAst.TryStmt) {
      TsqlAst.TryStmt s = (TsqlAst.TryStmt) stmt;
      stack.add(new Frame(s.body, null, s.handler));
      return null;
    }
    if (stmt instanceof TsqlAst.BreakStmt) {
      while (!stack.isEmpty() && stack.get(stack.size() - 1).whileCond == null) {
        stack.remove(stack.size() - 1);
      }
      if (!stack.isEmpty()) stack.remove(stack.size() - 1);
      return null;
    }
    if (stmt instanceof TsqlAst.ContinueStmt) {
      while (!stack.isEmpty() && stack.get(stack.size() - 1).whileCond == null) {
        stack.remove(stack.size() - 1);
      }
      if (!stack.isEmpty()) stack.get(stack.size() - 1).index = stack.get(stack.size() - 1).body.size();
      return null;
    }
    if (stmt instanceof TsqlAst.ReturnStmt) {
      stack.clear();
      stop = true;
      return Step.finished(summary());
    }
    if (stmt instanceof TsqlAst.DeclareCursor) {
      TsqlAst.DeclareCursor d = (TsqlAst.DeclareCursor) stmt;
      Cursor c = new Cursor();
      cursors.put(d.name.toLowerCase(Locale.ROOT), c);
      c.columns.add(d.dynamicSql == null ? d.sql : asString(eval(d.dynamicSql)));
      return null;
    }
    if (stmt instanceof TsqlAst.OpenCursor) {
      String name = ((TsqlAst.OpenCursor) stmt).name;
      Cursor c = cursor(name);
      String sql = c.columns.isEmpty() ? "" : c.columns.get(0);
      c.columns.clear();
      waitCursor = name;
      waitSql = rewrite(sql);
      return Step.sql(waitSql, copyOptions());
    }
    if (stmt instanceof TsqlAst.FetchCursor) {
      TsqlAst.FetchCursor f = (TsqlAst.FetchCursor) stmt;
      Cursor c = cursor(f.name);
      c.index++;
      if (c.index >= c.rows.size()) {
        fetchStatus = -1;
        for (String target : f.targets) setVar(target, Value.NULL);
      } else {
        fetchStatus = 0;
        List<TsqlSqlRunner.Cell> row = c.rows.get(c.index);
        for (int i = 0; i < f.targets.size(); i++) setVar(f.targets.get(i), cellValue(c.kinds, row, i));
      }
      sqlCode = fetchStatus;
      return null;
    }
    if (stmt instanceof TsqlAst.CloseCursor || stmt instanceof TsqlAst.DeallocateCursor) {
      return null;
    }
    if (stmt instanceof TsqlAst.CreateProc) {
      TsqlAst.CreateProc proc = (TsqlAst.CreateProc) stmt;
      if (!access.canCreate(database, user)) {
        throw new TsqlParser.ParseException(String.format(
            "User '%s' does not have privileges to CREATE procedures in database '%s'",
            user, database));
      }
      String current = store.getOwner(database, proc.name);
      if (current != null && !current.equals(user) && !access.canReplaceAny(database, user)) {
        throw new TsqlParser.ParseException(String.format(
            "Procedure %s.%s is owned by '%s'. Only the owner or a user with ALL on "
                + "database %s may replace it",
            database, proc.name, current, database));
      }
      String stored = originalSource == null ? "" : originalSource;
      proc.source = stored;
      defined.put(proc.name.toLowerCase(Locale.ROOT), proc);
      store.upsert(database, proc.name, current != null ? current : user,
          proc.executeAsOwner, stored);
      return null;
    }
    throw new TsqlParser.ParseException("Unsupported statement");
  }

  private void pushProc(TsqlAst.ExecProc call) {
    TsqlAst.CreateProc found = defined.get(call.name.toLowerCase(Locale.ROOT));
    if (found == null) {
      String source = store.getSource(database, call.name);
      if (source == null) throw new TsqlParser.ParseException("Unknown procedure " + call.name);
      TsqlAst.Script script = parseDialect(source);
      for (TsqlAst.Stmt s : script.statements) {
        if (s instanceof TsqlAst.CreateProc
            && ((TsqlAst.CreateProc) s).name.equalsIgnoreCase(call.name)) {
          found = (TsqlAst.CreateProc) s;
        }
      }
    }
    if (found == null) {
      throw new TsqlParser.ParseException("Unknown procedure " + call.name);
    }
    if (call.args.size() != found.params.size()) {
      throw new TsqlParser.ParseException(
          "Procedure " + call.name + " expects " + found.params.size() + " arguments");
    }
    for (int i = 0; i < found.params.size(); i++) {
      TsqlAst.Param param = found.params.get(i);
      TsqlAst.Arg arg = matchArg(call, param, i);
      noteType(param.name, param.typeName);
      setVar(param.name, evalValue(arg.value));
    }
    stack.add(new Frame(found.body, null, null));
  }

  private static TsqlAst.Arg matchArg(TsqlAst.ExecProc call, TsqlAst.Param param, int index) {
    for (TsqlAst.Arg arg : call.args) {
      if (arg.paramName != null && arg.paramName.equalsIgnoreCase(param.name)) return arg;
    }
    TsqlAst.Arg positional = call.args.get(index);
    if (positional.paramName != null) {
      throw new TsqlParser.ParseException("Argument " + positional.paramName + " does not match");
    }
    return positional;
  }

  private void applyResult(TsqlSqlRunner.Result previous) {
    lastError = 0;
    if (previous != null && !previous.ok()) {
      lastError = previous.sqlcode == 0 ? -1 : previous.sqlcode;
      sqlCode = lastError;
      // The failed statement must not consume the next successful result.
      waitCursor = null;
      waitAssign = null;
      waitClient = false;
      String message = previous.error == null ? "SQL failed" : previous.error;
      if (handleError(message)) return;
      throw new TsqlParser.ParseException(message);
    }
    sqlCode = 0;
    if (waitClient) {
      waitClient = false;
      stop = true;
      return;
    }
    if (waitCursor != null) {
      Cursor c = cursor(waitCursor);
      c.rows = previous == null ? new ArrayList<List<TsqlSqlRunner.Cell>>() : previous.rows;
      c.columns = previous == null ? new ArrayList<String>() : new ArrayList<String>(previous.columns);
      c.kinds = previous == null || previous.columnKinds == null
          ? new ArrayList<TsqlSqlRunner.ValueKind>() : new ArrayList<TsqlSqlRunner.ValueKind>(previous.columnKinds);
      c.index = -1;
      c.open = true;
      fetchStatus = c.rows.isEmpty() ? -1 : 0;
      waitCursor = null;
      return;
    }
    if (waitAssign != null) {
      List<TsqlSqlRunner.Cell> row = previous == null || previous.rows.isEmpty()
          ? null : previous.rows.get(0);
      List<TsqlSqlRunner.ValueKind> kinds = previous == null ? null : previous.columnKinds;
      for (int i = 0; i < waitAssign.size(); i++) {
        setVar(waitAssign.get(i), row == null ? Value.NULL : cellValue(kinds, row, i));
      }
      waitAssign = null;
    }
  }

  private boolean handleError(String message) {
    for (int i = stack.size() - 1; i >= 0; i--) {
      Frame frame = stack.get(i);
      if (frame.handler != null && !frame.handling) {
        while (stack.size() > i + 1) stack.remove(stack.size() - 1);
        frame.handling = true;
        frame.index = frame.body.size();
        stack.add(new Frame(frame.handler, null, null));
        setVar("ERROR_MESSAGE", new Value(message, TsqlSqlRunner.ValueKind.TEXT));
        return true;
      }
    }
    return false;
  }

  private Cursor cursor(String name) {
    Cursor c = cursors.get(name.toLowerCase(Locale.ROOT));
    if (c == null) throw new TsqlParser.ParseException("Unknown cursor " + name);
    return c;
  }

  private String summary() {
    if (outputs.isEmpty()) return "OK";
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> e : outputs.entrySet()) {
      if (sb.length() > 0) sb.append(',');
      sb.append(e.getKey()).append('=').append(e.getValue());
    }
    return sb.toString();
  }

  private Map<String, String> copyOptions() {
    return new HashMap<String, String>(procedureOptions);
  }

  private void noteType(String name, String typeName) {
    declared.put(name.toLowerCase(Locale.ROOT), kindOfType(typeName));
  }

  private void setVar(String name, Value value) {
    if (value == null) value = Value.NULL;
    String key = name.toLowerCase(Locale.ROOT);
    TsqlSqlRunner.ValueKind kind = declared.containsKey(key) ? declared.get(key) : value.kind;
    if (kind == null) kind = inferKind(value.text);
    if (value.text != null && kind == TsqlSqlRunner.ValueKind.NUMBER && !isSqlNumber(value.text)) {
      throw new TsqlParser.ParseException(
          "Cannot assign '" + value.text + "' to numeric @" + name);
    }
    String text = value.text;
    if (text != null && kind == TsqlSqlRunner.ValueKind.NUMBER) text = text.trim();
    vars.put(key, new Value(text, kind));
  }

  private String eval(TsqlAst.Expr expr) {
    Value value = evalValue(expr);
    return value == null ? null : value.text;
  }

  private Value evalValue(TsqlAst.Expr expr) {
    if (expr instanceof TsqlAst.Literal) return literalValue((TsqlAst.Literal) expr);
    if (expr instanceof TsqlAst.Var) return varValue(((TsqlAst.Var) expr).name);
    if (expr instanceof TsqlAst.CursorField) return cursorValue((TsqlAst.CursorField) expr);
    if (expr instanceof TsqlAst.IsNull) {
      TsqlAst.IsNull isNull = (TsqlAst.IsNull) expr;
      boolean nil = eval(isNull.inner) == null;
      boolean result = isNull.negated ? !nil : nil;
      return flag(result);
    }
    if (expr instanceof TsqlAst.Unary) {
      return flag(!truth(eval(((TsqlAst.Unary) expr).inner)));
    }
    if (expr instanceof TsqlAst.Binary) return binaryValue((TsqlAst.Binary) expr);
    if (expr instanceof TsqlAst.Call) return callValue((TsqlAst.Call) expr);
    return Value.NULL;
  }

  private static Value literalValue(TsqlAst.Literal lit) {
    if (lit.text == null) return Value.NULL;
    if (lit.string) return new Value(lit.text, TsqlSqlRunner.ValueKind.TEXT);
    if (isSqlNumber(lit.text)) return new Value(lit.text, TsqlSqlRunner.ValueKind.NUMBER);
    if (lit.text.equalsIgnoreCase("true") || lit.text.equalsIgnoreCase("false")) {
      return new Value(lit.text, TsqlSqlRunner.ValueKind.BOOLEAN);
    }
    return new Value(lit.text, TsqlSqlRunner.ValueKind.TEXT);
  }

  private Value varValue(String name) {
    String key = name.toLowerCase(Locale.ROOT);
    if (key.equals("fetch_status")) return new Value(Integer.toString(fetchStatus), TsqlSqlRunner.ValueKind.NUMBER);
    if (key.equals("error")) return new Value(Integer.toString(lastError), TsqlSqlRunner.ValueKind.NUMBER);
    if (key.equals("sqlcode")) return new Value(Integer.toString(sqlCode), TsqlSqlRunner.ValueKind.NUMBER);
    Value value = vars.get(key);
    return value == null ? Value.NULL : value;
  }

  private Value cursorValue(TsqlAst.CursorField field) {
    Cursor c = cursor(field.cursor);
    if (!c.open || c.index < 0 || c.index >= c.rows.size()) {
      throw new TsqlParser.ParseException("Cursor " + field.cursor + " has no current row");
    }
    int col = -1;
    for (int i = 0; i < c.columns.size(); i++) {
      if (c.columns.get(i) != null && c.columns.get(i).equalsIgnoreCase(field.field)) {
        col = i;
        break;
      }
    }
    if (col < 0) {
      throw new TsqlParser.ParseException(
          "Cursor " + field.cursor + " has no column " + field.field);
    }
    return cellValue(c.kinds, c.rows.get(c.index), col);
  }

  private Value binaryValue(TsqlAst.Binary b) {
    if (b.op.equals("AND")) return flag(truth(eval(b.left)) && truth(eval(b.right)));
    if (b.op.equals("OR")) return flag(truth(eval(b.left)) || truth(eval(b.right)));
    if (b.op.equals("||")) {
      String l = eval(b.left);
      String r = eval(b.right);
      if (l == null) l = "";
      if (r == null) r = "";
      return new Value(l + r, TsqlSqlRunner.ValueKind.TEXT);
    }
    if (b.op.equals("+")) {
      Value l = evalValue(b.left);
      Value r = evalValue(b.right);
      String lt = l.text == null ? "" : l.text;
      String rt = r.text == null ? "" : r.text;
      if (numericPair(l, r)) {
        return new Value(decimalText(decimal(lt).add(decimal(rt))), TsqlSqlRunner.ValueKind.NUMBER);
      }
      return new Value(lt + rt, TsqlSqlRunner.ValueKind.TEXT);
    }
    if (b.op.equals("-")) {
      return new Value(decimalText(decimal(eval(b.left)).subtract(decimal(eval(b.right)))),
          TsqlSqlRunner.ValueKind.NUMBER);
    }
    int cmp = compareValues(evalValue(b.left), evalValue(b.right));
    boolean ok;
    if (b.op.equals("=")) ok = cmp == 0;
    else if (b.op.equals("!=") || b.op.equals("<>")) ok = cmp != 0;
    else if (b.op.equals("<")) ok = cmp < 0;
    else if (b.op.equals(">")) ok = cmp > 0;
    else if (b.op.equals("<=")) ok = cmp <= 0;
    else if (b.op.equals(">=")) ok = cmp >= 0;
    else ok = false;
    return flag(ok);
  }

  private Value callValue(TsqlAst.Call call) {
    String name = call.name.toUpperCase(Locale.ROOT);
    if (name.equals("QUOTE_LITERAL")) {
      return new Value(quoteLiteral(asString(eval(call.args.get(0)))), TsqlSqlRunner.ValueKind.TEXT);
    }
    if (name.equals("CURRENT_DATABASE")) {
      return new Value(database, TsqlSqlRunner.ValueKind.TEXT);
    }
    if (name.equals("CAST")) {
      Value inner = evalValue(call.args.get(0));
      TsqlSqlRunner.ValueKind kind = kindOfType(call.castType);
      if (kind == null) kind = TsqlSqlRunner.ValueKind.TEXT;
      if (inner.text != null && kind == TsqlSqlRunner.ValueKind.NUMBER && !isSqlNumber(inner.text)) {
        throw new TsqlParser.ParseException("Cannot cast '" + inner.text + "' to " + call.castType);
      }
      String text = inner.text;
      if (text != null && kind == TsqlSqlRunner.ValueKind.NUMBER) text = text.trim();
      return new Value(text, kind);
    }
    if (name.equals("SPLIT_PART")) {
      String text = asString(eval(call.args.get(0)));
      String delim = asString(eval(call.args.get(1)));
      int n = decimal(eval(call.args.get(2))).intValue();
      if (text == null) return new Value("", TsqlSqlRunner.ValueKind.TEXT);
      String[] parts = text.split(Pattern.quote(delim == null ? "" : delim), -1);
      if (n < 1 || n > parts.length) return new Value("", TsqlSqlRunner.ValueKind.TEXT);
      return new Value(parts[n - 1].trim(), TsqlSqlRunner.ValueKind.TEXT);
    }
    throw new TsqlParser.ParseException("Unknown function " + call.name);
  }

  private static Value cellValue(List<TsqlSqlRunner.ValueKind> kinds,
      List<TsqlSqlRunner.Cell> row, int i) {
    if (row == null || i >= row.size() || row.get(i).isNull) return Value.NULL;
    TsqlSqlRunner.ValueKind kind = kinds != null && i < kinds.size() ? kinds.get(i) : null;
    return new Value(row.get(i).value, kind);
  }

  private static Value flag(boolean value) {
    return new Value(value ? "1" : "0", TsqlSqlRunner.ValueKind.NUMBER);
  }

  String rewriteVars(String sql) {
    return substitute(sql);
  }

  private String rewrite(String sql) {
    return substitute(sql);
  }

  private String substitute(String sql) {
    if (sql == null) return null;
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
        int j = SqlText.skipQuoted(sql, i);
        out.append(sql, i, j);
        i = j;
        continue;
      }
      if (c == '`') {
        int j = sql.indexOf('`', i + 1);
        if (j < 0) j = sql.length() - 1;
        out.append(sql, i, j + 1);
        i = j + 1;
        continue;
      }
      if (c == '@') {
        int j = i + 1;
        if (j < sql.length() && sql.charAt(j) == '@') j++;
        int start = j;
        while (j < sql.length() && (Character.isLetterOrDigit(sql.charAt(j)) || sql.charAt(j) == '_')) j++;
        String name = sql.substring(start, j);
        String key = name.toLowerCase(Locale.ROOT);
        Value current = vars.get(key);
        if (current == null || current.text == null) out.append("NULL");
        else out.append(sqlLiteral(current));
        i = j;
        continue;
      }
      out.append(c);
      i++;
    }
    return out.toString();
  }

  static String quoteLiteral(String value) {
    return SqlText.quote(value);
  }

  private static String sqlLiteral(Value value) {
    if (value == null || value.text == null) return "NULL";
    if (value.kind == TsqlSqlRunner.ValueKind.NUMBER) return value.text;
    if (value.kind == TsqlSqlRunner.ValueKind.BOOLEAN) return truth(value.text) ? "TRUE" : "FALSE";
    return quoteLiteral(value.text);
  }

  static TsqlSqlRunner.ValueKind kindOfType(String type) {
    if (type == null) return null;
    switch (type.toUpperCase(Locale.ROOT)) {
      case "TINYINT":
      case "SMALLINT":
      case "INT":
      case "INTEGER":
      case "BIGINT":
      case "DECIMAL":
      case "NUMERIC":
      case "NUMBER":
      case "FLOAT":
      case "REAL":
      case "DOUBLE":
      case "MONEY":
        return TsqlSqlRunner.ValueKind.NUMBER;
      case "BOOLEAN":
      case "BOOL":
      case "BIT":
        return TsqlSqlRunner.ValueKind.BOOLEAN;
      default:
        return TsqlSqlRunner.ValueKind.TEXT;
    }
  }

  private static TsqlSqlRunner.ValueKind inferKind(String text) {
    if (isSqlNumber(text)) return TsqlSqlRunner.ValueKind.NUMBER;
    if (text != null && (text.equalsIgnoreCase("true") || text.equalsIgnoreCase("false"))) {
      return TsqlSqlRunner.ValueKind.BOOLEAN;
    }
    return TsqlSqlRunner.ValueKind.TEXT;
  }

  private static boolean isSqlNumber(String value) {
    return value != null && SQL_NUMBER.matcher(value.trim()).matches();
  }

  private static boolean numericPair(Value l, Value r) {
    String lt = l.text == null ? "" : l.text;
    String rt = r.text == null ? "" : r.text;
    return (l.kind == TsqlSqlRunner.ValueKind.NUMBER || r.kind == TsqlSqlRunner.ValueKind.NUMBER)
        && isSqlNumber(lt) && isSqlNumber(rt);
  }

  private static BigDecimal decimal(String value) {
    if (!isSqlNumber(value)) return BigDecimal.ZERO;
    return new BigDecimal(value.trim());
  }

  private static String decimalText(BigDecimal value) {
    return value.stripTrailingZeros().toPlainString();
  }

  private static int compareValues(Value l, Value r) {
    if (l.text == null && r.text == null) return 0;
    if (l.text == null) return -1;
    if (r.text == null) return 1;
    if (numericPair(l, r)) return decimal(l.text).compareTo(decimal(r.text));
    return l.text.compareTo(r.text);
  }

  private static boolean isSetOption(String sql) {
    String t = sql.trim();
    return t.length() > 4 && t.substring(0, 3).equalsIgnoreCase("SET")
        && Character.isWhitespace(t.charAt(3));
  }

  private void applySetOption(String sql) {
    String body = sql.trim().substring(3).trim();
    int eq = body.indexOf('=');
    if (eq < 0) return;
    String key = body.substring(0, eq).trim();
    String value = body.substring(eq + 1).trim();
    if (value.endsWith(";")) value = value.substring(0, value.length() - 1).trim();
    procedureOptions.put(key, value);
  }

  private static boolean truth(String value) {
    if (value == null) return false;
    if (value.equals("0") || value.equalsIgnoreCase("false")) return false;
    return true;
  }

  private static String asString(String value) {
    return value;
  }

  /** In-memory store used when the caller does not supply one. */
  public static final class MemoryStore implements TsqlProcedureStore {
    private final Map<String, String> sources = new HashMap<String, String>();
    private final Map<String, Boolean> owners = new HashMap<String, Boolean>();
    private final Map<String, String> ownerNames = new HashMap<String, String>();

    public void upsert(String database, String name, String owner, boolean executeAsOwner,
        String source) {
      String key = key(database, name);
      sources.put(key, source == null ? "" : source);
      owners.put(key, executeAsOwner);
      ownerNames.put(key, owner == null ? "" : owner);
    }

    public String getSource(String database, String name) {
      return sources.get(key(database, name));
    }

    public String getOwner(String database, String name) {
      String key = key(database, name);
      if (!sources.containsKey(key)) return null;
      String owner = ownerNames.get(key);
      return owner == null ? "" : owner;
    }

    public List<String> list(String database) {
      String prefix = database.toLowerCase(Locale.ROOT) + ".";
      List<String> names = new ArrayList<String>();
      for (String key : sources.keySet()) {
        if (key.startsWith(prefix)) names.add(key.substring(prefix.length()));
      }
      return names;
    }

    private static String key(String database, String name) {
      return (database + "." + name).toLowerCase(Locale.ROOT);
    }
  }
}
