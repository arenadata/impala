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

import java.util.ArrayList;
import java.util.List;

/** Statement tree produced by {@link TsqlParser}. No Impala types. */
public final class TsqlAst {
  private TsqlAst() {}

  public static final class Script {
    public final List<Stmt> statements = new ArrayList<Stmt>();
  }

  public abstract static class Stmt {}

  public static final class DeclareVar extends Stmt {
    public final String name;
    public final String typeName;
    public final Expr initializer;

    public DeclareVar(String name, String typeName, Expr initializer) {
      this.name = name;
      this.typeName = typeName;
      this.initializer = initializer;
    }
  }

  public static final class SetStmt extends Stmt {
    public final String name;
    public final Expr value;

    public SetStmt(String name, Expr value) {
      this.name = name;
      this.value = value;
    }
  }

  /** SELECT @a = expr, @b = expr FROM ... */
  public static final class SelectAssign extends Stmt {
    public final List<String> targets = new ArrayList<String>();
    public String sql;
  }

  /** Client result. Legal only as the last statement of the outermost body. */
  public static final class BareSelect extends Stmt {
    public final String sql;

    public BareSelect(String sql) {
      this.sql = sql;
    }
  }

  /** Impala SQL that is not a SELECT assignment or a client result. */
  public static final class RawSql extends Stmt {
    public final String sql;

    public RawSql(String sql) {
      this.sql = sql;
    }
  }

  public static final class ExecString extends Stmt {
    public final Expr text;

    public ExecString(Expr text) {
      this.text = text;
    }
  }

  public static final class ExecProc extends Stmt {
    public String database;
    public String name;
    /** Variable that receives the procedure return code, or null. */
    public String returnTarget;
    public final List<Arg> args = new ArrayList<Arg>();
  }

  public static final class Arg {
    public final String paramName; // null when positional
    public final Expr value;
    public final boolean output;

    public Arg(String paramName, Expr value, boolean output) {
      this.paramName = paramName;
      this.value = value;
      this.output = output;
    }
  }

  public static final class IfStmt extends Stmt {
    public Expr condition;
    public final List<Stmt> thenBody = new ArrayList<Stmt>();
    public final List<Stmt> elseBody = new ArrayList<Stmt>();
  }

  public static final class WhileStmt extends Stmt {
    public Expr condition;
    public final List<Stmt> body = new ArrayList<Stmt>();
  }

  public static final class TryStmt extends Stmt {
    public final List<Stmt> body = new ArrayList<Stmt>();
    public final List<Stmt> handler = new ArrayList<Stmt>();
  }

  /** BEGIN...END. Not an error handler. */
  public static final class Block extends Stmt {
    public final List<Stmt> body = new ArrayList<Stmt>();
  }

  public static final class ReturnStmt extends Stmt {
    public Expr value;
  }

  public static final class ThrowStmt extends Stmt {
    public Expr number;
    public Expr message;
    public Expr state;
  }

  public static final class DropProc extends Stmt {
    public String database;
    public String name;
    public boolean ifExists;
  }

  /** SET NOCOUNT and the other session toggles this build ignores. */
  public static final class Noop extends Stmt {}

  public static final class BreakStmt extends Stmt {}

  public static final class ContinueStmt extends Stmt {}

  public static final class DeclareCursor extends Stmt {
    public final String name;
    /** Query text, or an expression that evaluates to the query when it starts with @. */
    public final String sql;
    public final Expr dynamicSql;

    public DeclareCursor(String name, String sql, Expr dynamicSql) {
      this.name = name;
      this.sql = sql;
      this.dynamicSql = dynamicSql;
    }
  }

  public static final class OpenCursor extends Stmt {
    public final String name;

    public OpenCursor(String name) {
      this.name = name;
    }
  }

  public static final class FetchCursor extends Stmt {
    public String name;
    public final List<String> targets = new ArrayList<String>();
  }

  public static final class CloseCursor extends Stmt {
    public final String name;

    public CloseCursor(String name) {
      this.name = name;
    }
  }

  public static final class DeallocateCursor extends Stmt {
    public final String name;

    public DeallocateCursor(String name) {
      this.name = name;
    }
  }

  public static final class CreateProc extends Stmt {
    public String database;
    public String name;
    public final List<Param> params = new ArrayList<Param>();
    public final List<Stmt> body = new ArrayList<Stmt>();
    public String source = "tsql\n";
  }

  public static final class Param {
    public final String name;
    public final String typeName;
    public final boolean output;

    public final Expr defaultValue;

    public Param(String name, String typeName, boolean output) {
      this(name, typeName, output, null);
    }

    public Param(String name, String typeName, boolean output, Expr defaultValue) {
      this.name = name;
      this.typeName = typeName;
      this.output = output;
      this.defaultValue = defaultValue;
    }
  }

  public abstract static class Expr {}

  public static final class Literal extends Expr {
    public final String text;
    public final boolean string;

    public Literal(String text, boolean string) {
      this.text = text;
      this.string = string;
    }
  }

  public static final class Var extends Expr {
    public final String name;

    public Var(String name) {
      this.name = name;
    }
  }

  public static final class Binary extends Expr {
    public final String op;
    public final Expr left;
    public final Expr right;

    public Binary(String op, Expr left, Expr right) {
      this.op = op;
      this.left = left;
      this.right = right;
    }
  }

  public static final class Unary extends Expr {
    public final String op;
    public final Expr inner;

    public Unary(String op, Expr inner) {
      this.op = op;
      this.inner = inner;
    }
  }

  public static final class Call extends Expr {
    public String name;
    public final List<Expr> args = new ArrayList<Expr>();
    /** Target type of CAST, or null for every other call. */
    public String castType;
  }

  public static final class IsNull extends Expr {
    public final Expr inner;
    public final boolean negated;

    public IsNull(Expr inner, boolean negated) {
      this.inner = inner;
      this.negated = negated;
    }
  }

  /** Current row of an open cursor, addressed by result column name. */
  public static final class CursorField extends Expr {
    public final String cursor;
    public final String field;

    public CursorField(String cursor, String field) {
      this.cursor = cursor;
      this.field = field;
    }
  }

  /** Stable tree text used by the keyword tests. No source positions. */
  public static String dump(Script script) {
    StringBuilder sb = new StringBuilder();
    if (script != null) {
      for (Stmt stmt : script.statements) appendStmt(sb, stmt, 0);
    }
    if (sb.length() > 0 && sb.charAt(sb.length() - 1) == '\n') {
      sb.setLength(sb.length() - 1);
    }
    return sb.toString();
  }

  private static void appendStmt(StringBuilder sb, Stmt stmt, int indent) {
    if (stmt instanceof DeclareVar) {
      DeclareVar d = (DeclareVar) stmt;
      line(sb, indent, "DeclareVar " + d.name + " " + d.typeName
          + (d.initializer == null ? "" : " = " + expr(d.initializer)));
    } else if (stmt instanceof SetStmt) {
      SetStmt s = (SetStmt) stmt;
      line(sb, indent, "SetStmt " + s.name + " = " + expr(s.value));
    } else if (stmt instanceof SelectAssign) {
      SelectAssign s = (SelectAssign) stmt;
      line(sb, indent, "SelectAssign " + join(s.targets) + " " + s.sql);
    } else if (stmt instanceof BareSelect) {
      line(sb, indent, "BareSelect " + ((BareSelect) stmt).sql);
    } else if (stmt instanceof RawSql) {
      String sql = ((RawSql) stmt).sql;
      line(sb, indent, "RawSql" + (sql == null || sql.isEmpty() ? "" : " " + sql));
    } else if (stmt instanceof ExecString) {
      line(sb, indent, "ExecString " + expr(((ExecString) stmt).text));
    } else if (stmt instanceof ExecProc) {
      ExecProc call = (ExecProc) stmt;
      line(sb, indent, "ExecProc " + call.name);
      for (Arg arg : call.args) {
        String named = arg.paramName == null ? "" : arg.paramName + " = ";
        line(sb, indent + 1, "Arg " + named + expr(arg.value) + (arg.output ? " out" : ""));
      }
    } else if (stmt instanceof IfStmt) {
      IfStmt s = (IfStmt) stmt;
      line(sb, indent, "IfStmt " + expr(s.condition));
      line(sb, indent + 1, "then");
      for (Stmt child : s.thenBody) appendStmt(sb, child, indent + 2);
      if (!s.elseBody.isEmpty()) {
        line(sb, indent + 1, "else");
        for (Stmt child : s.elseBody) appendStmt(sb, child, indent + 2);
      }
    } else if (stmt instanceof WhileStmt) {
      WhileStmt s = (WhileStmt) stmt;
      line(sb, indent, "WhileStmt " + expr(s.condition));
      for (Stmt child : s.body) appendStmt(sb, child, indent + 1);
    } else if (stmt instanceof TryStmt) {
      TryStmt s = (TryStmt) stmt;
      line(sb, indent, "TryStmt");
      line(sb, indent + 1, "body");
      for (Stmt child : s.body) appendStmt(sb, child, indent + 2);
      line(sb, indent + 1, "handler");
      for (Stmt child : s.handler) appendStmt(sb, child, indent + 2);
    } else if (stmt instanceof ReturnStmt) {
      ReturnStmt r = (ReturnStmt) stmt;
      line(sb, indent, "ReturnStmt" + (r.value == null ? "" : " " + expr(r.value)));
    } else if (stmt instanceof Block) {
      line(sb, indent, "Block");
      for (Stmt child : ((Block) stmt).body) appendStmt(sb, child, indent + 1);
    } else if (stmt instanceof ThrowStmt) {
      ThrowStmt t = (ThrowStmt) stmt;
      line(sb, indent, "ThrowStmt" + (t.number == null ? "" : " " + expr(t.number)));
    } else if (stmt instanceof DropProc) {
      DropProc d = (DropProc) stmt;
      line(sb, indent, "DropProc " + (d.database == null ? "" : d.database + ".") + d.name
          + (d.ifExists ? " ifexists" : ""));
    } else if (stmt instanceof Noop) {
      line(sb, indent, "Noop");
    } else if (stmt instanceof BreakStmt) {
      line(sb, indent, "BreakStmt");
    } else if (stmt instanceof ContinueStmt) {
      line(sb, indent, "ContinueStmt");
    } else if (stmt instanceof DeclareCursor) {
      DeclareCursor d = (DeclareCursor) stmt;
      if (d.dynamicSql != null) {
        line(sb, indent, "DeclareCursor " + d.name + " dynamic=" + expr(d.dynamicSql));
      } else if (d.sql != null && !d.sql.isEmpty()) {
        line(sb, indent, "DeclareCursor " + d.name + " sql=" + d.sql);
      } else {
        line(sb, indent, "DeclareCursor " + d.name);
      }
    } else if (stmt instanceof OpenCursor) {
      line(sb, indent, "OpenCursor " + ((OpenCursor) stmt).name);
    } else if (stmt instanceof FetchCursor) {
      FetchCursor f = (FetchCursor) stmt;
      line(sb, indent, "FetchCursor " + f.name + (f.targets.isEmpty() ? "" : " " + join(f.targets)));
    } else if (stmt instanceof CloseCursor) {
      line(sb, indent, "CloseCursor " + ((CloseCursor) stmt).name);
    } else if (stmt instanceof DeallocateCursor) {
      line(sb, indent, "DeallocateCursor " + ((DeallocateCursor) stmt).name);
    } else if (stmt instanceof CreateProc) {
      CreateProc proc = (CreateProc) stmt;
      String dialect = proc.source != null && proc.source.startsWith("lpsql") ? "lpsql" : "tsql";
      String db = proc.database == null ? "" : proc.database + ".";
      line(sb, indent, "CreateProc " + db + proc.name + " " + dialect);
      for (Param param : proc.params) {
        line(sb, indent + 1, "Param " + param.name + " " + param.typeName + (param.output ? " out" : " in"));
      }
      for (Stmt child : proc.body) appendStmt(sb, child, indent + 1);
    } else if (stmt != null) {
      line(sb, indent, stmt.getClass().getSimpleName());
    }
  }

  private static String expr(Expr expr) {
    if (expr == null) return "null";
    if (expr instanceof Literal) {
      Literal lit = (Literal) expr;
      if (lit.text == null) return "Literal NULL";
      if (lit.string) return "Literal '" + lit.text.replace("'", "''") + "'";
      return "Literal " + lit.text;
    }
    if (expr instanceof Var) return "Var " + ((Var) expr).name;
    if (expr instanceof CursorField) {
      CursorField field = (CursorField) expr;
      return "CursorField " + field.cursor + "." + field.field;
    }
    if (expr instanceof Binary) {
      Binary b = (Binary) expr;
      return "(" + expr(b.left) + " " + b.op + " " + expr(b.right) + ")";
    }
    if (expr instanceof Unary) {
      Unary u = (Unary) expr;
      return "(" + u.op + " " + expr(u.inner) + ")";
    }
    if (expr instanceof Call) {
      Call call = (Call) expr;
      StringBuilder sb = new StringBuilder("Call ").append(call.name).append('(');
      for (int i = 0; i < call.args.size(); i++) {
        if (i > 0) sb.append(", ");
        sb.append(expr(call.args.get(i)));
      }
      return sb.append(')').toString();
    }
    if (expr instanceof IsNull) {
      IsNull isNull = (IsNull) expr;
      return "(" + expr(isNull.inner) + (isNull.negated ? " IS NOT NULL)" : " IS NULL)");
    }
    return expr.getClass().getSimpleName();
  }

  private static void line(StringBuilder sb, int indent, String text) {
    for (int i = 0; i < indent; i++) sb.append("  ");
    sb.append(text).append('\n');
  }

  /** SQL text Impala can parse without running the batch. Dynamic SQL is omitted. */
  public static List<String> staticSql(Script script) {
    List<String> out = new ArrayList<String>();
    if (script != null) collectSql(script.statements, out);
    return out;
  }

  private static void collectSql(List<Stmt> stmts, List<String> out) {
    if (stmts == null) return;
    for (Stmt s : stmts) {
      if (s instanceof RawSql) addSql(out, ((RawSql) s).sql);
      else if (s instanceof BareSelect) addSql(out, ((BareSelect) s).sql);
      else if (s instanceof SelectAssign) addSql(out, ((SelectAssign) s).sql);
      else if (s instanceof DeclareCursor) {
        DeclareCursor c = (DeclareCursor) s;
        if (c.dynamicSql == null) addSql(out, c.sql);
      } else if (s instanceof IfStmt) {
        collectSql(((IfStmt) s).thenBody, out);
        collectSql(((IfStmt) s).elseBody, out);
      } else if (s instanceof WhileStmt) {
        collectSql(((WhileStmt) s).body, out);
      } else if (s instanceof TryStmt) {
        collectSql(((TryStmt) s).body, out);
        collectSql(((TryStmt) s).handler, out);
      } else if (s instanceof Block) {
        collectSql(((Block) s).body, out);
      } else if (s instanceof CreateProc) {
        collectSql(((CreateProc) s).body, out);
      }
    }
  }

  private static void addSql(List<String> out, String sql) {
    if (sql != null && !sql.trim().isEmpty()) out.add(sql);
  }

  private static String join(List<String> parts) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < parts.size(); i++) {
      if (i > 0) sb.append(", ");
      sb.append(parts.get(i));
    }
    return sb.toString();
  }
}
