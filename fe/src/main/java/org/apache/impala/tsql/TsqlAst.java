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
    public String name;
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

  public static final class ReturnStmt extends Stmt {}

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
    public String name;
    public final List<Param> params = new ArrayList<Param>();
    public final List<Stmt> body = new ArrayList<Stmt>();
    public boolean executeAsOwner;
    public String source = "tsql\n";
  }

  public static final class Param {
    public final String name;
    public final String typeName;
    public final boolean output;

    public Param(String name, String typeName, boolean output) {
      this.name = name;
      this.typeName = typeName;
      this.output = output;
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
  }

  public static final class IsNull extends Expr {
    public final Expr inner;
    public final boolean negated;

    public IsNull(Expr inner, boolean negated) {
      this.inner = inner;
      this.negated = negated;
    }
  }
}
