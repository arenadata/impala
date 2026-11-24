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

import java.util.List;
import java.util.Map;

/**
 * Executes one Impala statement on behalf of the T-SQL runtime.
 * The runtime itself does not import Impala or Thrift.
 * procedureOptions are in force only for this procedure and must not be
 * written back to the caller's session.
 */
public interface TsqlSqlRunner {
  /** How a value is written back into Impala SQL. */
  enum ValueKind { TEXT, NUMBER, BOOLEAN }

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
    /** Parallel to {@link #columns}. Null means each cell's kind is inferred. */
    public final List<ValueKind> columnKinds;

    public Result(List<String> columns, List<List<Cell>> rows, int sqlcode, String error) {
      this(columns, rows, sqlcode, error, null);
    }

    public Result(List<String> columns, List<List<Cell>> rows, int sqlcode, String error,
        List<ValueKind> columnKinds) {
      this.columns = columns;
      this.rows = rows;
      this.sqlcode = sqlcode;
      this.error = error;
      this.columnKinds = columnKinds;
    }

    public boolean ok() {
      return sqlcode == 0;
    }
  }

  Result execute(String sql, Map<String, String> procedureOptions);
}
