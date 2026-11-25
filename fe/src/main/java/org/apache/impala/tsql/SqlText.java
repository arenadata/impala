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

/**
 * Procedure source follows its dialect: a doubled quote is one quote and a
 * backslash is an ordinary character. Impala SQL uses backslash escapes, so a
 * literal is translated only when it is emitted.
 */
public final class SqlText {
  private SqlText() {}

  /** Source literal. A backslash does not escape the closing quote. */
  public static int skipSourceLiteral(String sql, int start) {
    char q = sql.charAt(start);
    int n = sql.length();
    int j = start + 1;
    while (j < n) {
      if (sql.charAt(j) == q) {
        if (j + 1 < n && sql.charAt(j + 1) == q) {
          j += 2;
          continue;
        }
        return j + 1;
      }
      j++;
    }
    return n;
  }

  /** Impala literal. A backslash escapes the next character, including a quote. */
  public static int skipImpalaLiteral(String sql, int start) {
    char q = sql.charAt(start);
    int n = sql.length();
    int j = start + 1;
    while (j < n) {
      char c = sql.charAt(j);
      if (c == '\\') {
        j += 2;
        continue;
      }
      if (c == q) return j + 1;
      j++;
    }
    return n;
  }

  /** Kept for callers that already hold Impala SQL. */
  public static int skipQuoted(String sql, int start) {
    return skipImpalaLiteral(sql, start);
  }

  /**
   * Body of a source literal {@code sql[start, end)} as it must appear between
   * single quotes in Impala SQL.
   */
  public static String impalaBody(String sql, int start, int end) {
    char q = sql.charAt(start);
    int stop = end - 1 > start && end <= sql.length() && sql.charAt(end - 1) == q
        ? end - 1 : end;
    StringBuilder sb = new StringBuilder();
    for (int j = start + 1; j < stop; j++) {
      char c = sql.charAt(j);
      if (c == '\\') {
        sb.append("\\\\");
      } else if (c == q && j + 1 < stop && sql.charAt(j + 1) == q) {
        sb.append(q == '\'' ? "\\'" : String.valueOf(q));
        j++;
      } else if (c == '\'') {
        sb.append("\\'");
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  /** Value of an Impala literal body. Only {@code \'} {@code \"} and {@code \\} are decoded. */
  public static String value(String impalaBody) {
    if (impalaBody == null) return null;
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < impalaBody.length(); i++) {
      char c = impalaBody.charAt(i);
      if (c == '\\' && i + 1 < impalaBody.length()) {
        char n = impalaBody.charAt(i + 1);
        if (n == '\'' || n == '"' || n == '\\') {
          sb.append(n);
          i++;
          continue;
        }
      }
      sb.append(c);
    }
    return sb.toString();
  }

  /** Impala literal for a procedure value. Null stays the SQL keyword NULL. */
  public static String quote(String value) {
    if (value == null) return "NULL";
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
  }

  /** Replaces {@code @name} outside Impala literals with NULL so the SQL parser can check shape. */
  public static String withPlaceholders(String sql) {
    if (sql == null) return null;
    StringBuilder out = new StringBuilder();
    int i = 0;
    while (i < sql.length()) {
      char c = sql.charAt(i);
      if (c == '\'' || c == '"') {
        int j = skipImpalaLiteral(sql, i);
        out.append(sql, i, j);
        i = j;
        continue;
      }
      if (c == '@') {
        int j = i + 1;
        if (j < sql.length() && sql.charAt(j) == '@') j++;
        while (j < sql.length() && (Character.isLetterOrDigit(sql.charAt(j)) || sql.charAt(j) == '_')) {
          j++;
        }
        out.append("NULL");
        i = j;
        continue;
      }
      out.append(c);
      i++;
    }
    return out.toString();
  }
}
