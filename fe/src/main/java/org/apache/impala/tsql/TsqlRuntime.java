package org.apache.impala.tsql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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
    List<List<TsqlSqlRunner.Cell>> rows = new ArrayList<List<TsqlSqlRunner.Cell>>();
    int index = -1;
    boolean open;
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
  private final String owner;
  private final Map<String, String> vars = new HashMap<String, String>();
  private final Map<String, Boolean> nulls = new HashMap<String, Boolean>();
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

  public TsqlRuntime(String source, String database, String owner, TsqlProcedureStore store) {
    this.database = database == null ? "default" : database;
    this.owner = owner == null ? "" : owner;
    this.originalSource = source == null ? "" : source;
    this.store = store == null ? new MemoryStore() : store;
    TsqlAst.Script script = TsqlParser.parse(source);
    stack.add(new Frame(script.statements, null, null));
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
      if (d.initializer == null) setVar(d.name, null);
      else setVar(d.name, eval(d.initializer));
      return null;
    }
    if (stmt instanceof TsqlAst.SetStmt) {
      TsqlAst.SetStmt s = (TsqlAst.SetStmt) stmt;
      setVar(s.name, eval(s.value));
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
        for (String target : f.targets) setVar(target, null);
      } else {
        fetchStatus = 0;
        List<TsqlSqlRunner.Cell> row = c.rows.get(c.index);
        for (int i = 0; i < f.targets.size(); i++) {
          if (i < row.size() && !row.get(i).isNull) setVar(f.targets.get(i), row.get(i).value);
          else setVar(f.targets.get(i), null);
        }
      }
      return null;
    }
    if (stmt instanceof TsqlAst.CloseCursor || stmt instanceof TsqlAst.DeallocateCursor) {
      return null;
    }
    if (stmt instanceof TsqlAst.CreateProc) {
      TsqlAst.CreateProc proc = (TsqlAst.CreateProc) stmt;
      String stored = originalSource.startsWith("tsql\n")
          ? originalSource : "tsql\n" + originalSource;
      proc.source = stored;
      defined.put(proc.name.toLowerCase(Locale.ROOT), proc);
      store.upsert(database, proc.name, owner, proc.executeAsOwner, stored);
      return null;
    }
    throw new TsqlParser.ParseException("Unsupported statement");
  }

  private void pushProc(TsqlAst.ExecProc call) {
    TsqlAst.CreateProc found = defined.get(call.name.toLowerCase(Locale.ROOT));
    if (found == null) {
      String source = store.getSource(database, call.name);
      if (source == null) throw new TsqlParser.ParseException("Unknown procedure " + call.name);
      String body = source.startsWith("tsql\n") ? source.substring(5) : source;
      TsqlAst.Script script = TsqlParser.parse(body);
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
      TsqlAst.Arg arg = matchArg(call, found.params.get(i), i);
      setVar(found.params.get(i).name, eval(arg.value));
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
      // The failed statement must not consume the next successful result.
      waitCursor = null;
      waitAssign = null;
      waitClient = false;
      String message = previous.error == null ? "SQL failed" : previous.error;
      if (handleError(message)) return;
      throw new TsqlParser.ParseException(message);
    }
    if (waitClient) {
      waitClient = false;
      stop = true;
      return;
    }
    if (waitCursor != null) {
      Cursor c = cursor(waitCursor);
      c.rows = previous == null ? new ArrayList<List<TsqlSqlRunner.Cell>>() : previous.rows;
      c.columns = previous == null ? new ArrayList<String>() : new ArrayList<String>(previous.columns);
      c.index = -1;
      c.open = true;
      fetchStatus = c.rows.isEmpty() ? -1 : 0;
      waitCursor = null;
      return;
    }
    if (waitAssign != null) {
      if (previous == null || previous.rows.isEmpty()) {
        for (String target : waitAssign) setVar(target, null);
      } else {
        List<TsqlSqlRunner.Cell> row = previous.rows.get(0);
        for (int i = 0; i < waitAssign.size(); i++) {
          if (i < row.size() && !row.get(i).isNull) setVar(waitAssign.get(i), row.get(i).value);
          else setVar(waitAssign.get(i), null);
        }
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
        setVar("ERROR_MESSAGE", message);
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

  private void setVar(String name, String value) {
    String key = name.toLowerCase(Locale.ROOT);
    if (value == null) {
      nulls.put(key, Boolean.TRUE);
      vars.remove(key);
    } else {
      nulls.put(key, Boolean.FALSE);
      vars.put(key, value);
    }
  }

  private String eval(TsqlAst.Expr expr) {
    if (expr instanceof TsqlAst.Literal) {
      TsqlAst.Literal lit = (TsqlAst.Literal) expr;
      return lit.text;
    }
    if (expr instanceof TsqlAst.Var) {
      String key = ((TsqlAst.Var) expr).name.toLowerCase(Locale.ROOT);
      if (key.equals("fetch_status")) return Integer.toString(fetchStatus);
      if (key.equals("error")) return Integer.toString(lastError);
      if (Boolean.TRUE.equals(nulls.get(key))) return null;
      return vars.get(key);
    }
    if (expr instanceof TsqlAst.IsNull) {
      TsqlAst.IsNull isNull = (TsqlAst.IsNull) expr;
      boolean nil = eval(isNull.inner) == null;
      boolean result = isNull.negated ? !nil : nil;
      return result ? "1" : "0";
    }
    if (expr instanceof TsqlAst.Unary) {
      boolean v = truth(eval(((TsqlAst.Unary) expr).inner));
      return v ? "0" : "1";
    }
    if (expr instanceof TsqlAst.Binary) {
      TsqlAst.Binary b = (TsqlAst.Binary) expr;
      if (b.op.equals("AND")) return truth(eval(b.left)) && truth(eval(b.right)) ? "1" : "0";
      if (b.op.equals("OR")) return truth(eval(b.left)) || truth(eval(b.right)) ? "1" : "0";
      if (b.op.equals("+")) {
        String l = eval(b.left);
        String r = eval(b.right);
        if (l == null) l = "";
        if (r == null) r = "";
        if (isNumber(l) && isNumber(r)) return strip(Double.parseDouble(l) + Double.parseDouble(r));
        return l + r;
      }
      if (b.op.equals("-")) {
        return strip(number(eval(b.left)) - number(eval(b.right)));
      }
      String l = eval(b.left);
      String r = eval(b.right);
      int cmp = compare(l, r);
      boolean ok;
      if (b.op.equals("=")) ok = cmp == 0;
      else if (b.op.equals("!=") || b.op.equals("<>")) ok = cmp != 0;
      else if (b.op.equals("<")) ok = cmp < 0;
      else if (b.op.equals(">")) ok = cmp > 0;
      else if (b.op.equals("<=")) ok = cmp <= 0;
      else if (b.op.equals(">=")) ok = cmp >= 0;
      else ok = false;
      return ok ? "1" : "0";
    }
    if (expr instanceof TsqlAst.Call) {
      TsqlAst.Call call = (TsqlAst.Call) expr;
      String name = call.name.toUpperCase(Locale.ROOT);
      if (name.equals("QUOTE_LITERAL")) {
        return quoteLiteral(asString(eval(call.args.get(0))));
      }
      if (name.equals("CURRENT_DATABASE")) return database;
      if (name.equals("CAST")) {
        return asString(eval(call.args.get(0)));
      }
      if (name.equals("SPLIT_PART")) {
        String text = asString(eval(call.args.get(0)));
        String delim = asString(eval(call.args.get(1)));
        int n = (int) number(eval(call.args.get(2)));
        if (text == null) return "";
        String[] parts = text.split(java.util.regex.Pattern.quote(delim), -1);
        if (n < 1 || n > parts.length) return "";
        return parts[n - 1].trim();
      }
      throw new TsqlParser.ParseException("Unknown function " + call.name);
    }
    return null;
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
      if (c == '\'') {
        int j = i + 1;
        while (j < sql.length()) {
          if (sql.charAt(j) == '\'' && j + 1 < sql.length() && sql.charAt(j + 1) == '\'') {
            j += 2;
            continue;
          }
          if (sql.charAt(j) == '\'') break;
          j++;
        }
        out.append(sql, i, Math.min(sql.length(), j + 1));
        i = j + 1;
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
        if (Boolean.TRUE.equals(nulls.get(key)) || !vars.containsKey(key)) {
          out.append("NULL");
        } else {
          out.append(sqlLiteral(vars.get(key)));
        }
        i = j;
        continue;
      }
      out.append(c);
      i++;
    }
    return out.toString();
  }

  static String quoteLiteral(String value) {
    if (value == null) return "NULL";
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'";
  }

  private static String sqlLiteral(String value) {
    if (value == null) return "NULL";
    if (isNumber(value)) return strip(Double.parseDouble(value));
    return quoteLiteral(value);
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

  private static boolean isNumber(String value) {
    if (value == null || value.isEmpty()) return false;
    try {
      Double.parseDouble(value);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  private static double number(String value) {
    if (value == null) return 0;
    return Double.parseDouble(value);
  }

  private static String strip(double value) {
    if (value == Math.rint(value)) return Long.toString((long) value);
    return Double.toString(value);
  }

  private static int compare(String l, String r) {
    if (l == null && r == null) return 0;
    if (l == null) return -1;
    if (r == null) return 1;
    if (isNumber(l) && isNumber(r)) return Double.compare(Double.parseDouble(l), Double.parseDouble(r));
    return l.compareTo(r);
  }

  /** In-memory store used when the caller does not supply one. */
  public static final class MemoryStore implements TsqlProcedureStore {
    private final Map<String, String> sources = new HashMap<String, String>();
    private final Map<String, Boolean> owners = new HashMap<String, Boolean>();

    public void upsert(String database, String name, String owner, boolean executeAsOwner,
        String source) {
      String key = key(database, name);
      sources.put(key, source.startsWith("tsql\n") ? source : "tsql\n" + source);
      owners.put(key, executeAsOwner);
    }

    public String getSource(String database, String name) {
      return sources.get(key(database, name));
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
