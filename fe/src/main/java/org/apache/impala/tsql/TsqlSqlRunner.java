package org.apache.impala.tsql;

import java.util.List;
import java.util.Map;

/**
 * Executes one Impala statement on behalf of the T-SQL runtime.
 * The runtime itself does not import Impala or Thrift.
 * procedureOptions are in force only for this procedure and must not be
 * written back to the caller's session.
 */
public interface TsqlSqlRunner {
  final class Cell {
    public final boolean isNull;
    public final String value;

    private Cell(boolean isNull, String value) {
      this.isNull = isNull;
      this.value = value;
    }

    public static Cell of(String value) {
      return value == null ? new Cell(true, null) : new Cell(false, value);
    }

    public static Cell nul() {
      return new Cell(true, null);
    }
  }

  final class Result {
    public final List<String> columns;
    public final List<List<Cell>> rows;
    public final int sqlcode;
    public final String error;

    public Result(List<String> columns, List<List<Cell>> rows, int sqlcode, String error) {
      this.columns = columns;
      this.rows = rows;
      this.sqlcode = sqlcode;
      this.error = error;
    }

    public boolean ok() {
      return sqlcode == 0;
    }
  }

  Result execute(String sql, Map<String, String> procedureOptions);
}
