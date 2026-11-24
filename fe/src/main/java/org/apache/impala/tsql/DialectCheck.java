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
 * Dialect is selected only by a leading {@code LPSQL} or {@code TSQL} keyword.
 * The body is then checked for keywords of the other dialect so a mixed batch
 * fails here instead of at execution.
 */
public final class DialectCheck {
  private DialectCheck() {}

  public static boolean leadingIs(String sql, String keyword) {
    if (sql == null) return false;
    int i = skipIgnored(sql, 0);
    return startsWithKeyword(sql, i, keyword);
  }

  /**
   * Body after an optional leading keyword. {@code $$...$$} and a quoted body
   * are unwrapped. Text that does not start with the keyword is returned unchanged.
   */
  public static String stripLeading(String sql, String keyword) {
    if (sql == null) return "";
    int i = skipIgnored(sql, 0);
    if (!startsWithKeyword(sql, i, keyword)) return sql;
    i = skipIgnored(sql, i + keyword.length());
    if (i >= sql.length()) return "";
    if (sql.startsWith("$$", i)) {
      int end = sql.indexOf("$$", i + 2);
      if (end < 0) throw new TsqlParser.ParseException("Unclosed $$");
      return sql.substring(i + 2, end);
    }
    char q = sql.charAt(i);
    if (q == '\'' || q == '"') return readQuoted(sql, i, q);
    return sql.substring(i);
  }

  public static void verifyLpsql(String sql) {
    scan(sql == null ? "" : sql, true);
  }

  public static void verifyTsql(String sql) {
    scan(sql == null ? "" : sql, false);
  }

  private static void scan(String sql, boolean lpsql) {
    int i = 0;
    int paren = 0;
    boolean statementStart = true;
    boolean selectStmt = false;
    String prev = null;
    while (i < sql.length()) {
      char c = sql.charAt(i);
      if (Character.isWhitespace(c)) {
        i++;
        continue;
      }
      if (c == '-' && i + 1 < sql.length() && sql.charAt(i + 1) == '-') {
        while (i < sql.length() && sql.charAt(i) != '\n') i++;
        continue;
      }
      if (c == '/' && i + 1 < sql.length() && sql.charAt(i + 1) == '*') {
        i += 2;
        while (i + 1 < sql.length() && !(sql.charAt(i) == '*' && sql.charAt(i + 1) == '/')) i++;
        i = Math.min(sql.length(), i + 2);
        continue;
      }
      if (c == '\'' || c == '"') {
        i = SqlText.skipSourceLiteral(sql, i);
        statementStart = false;
        continue;
      }
      if (c == '`') {
        int j = sql.indexOf('`', i + 1);
        i = j < 0 ? sql.length() : j + 1;
        statementStart = false;
        continue;
      }
      if (c == '@') {
        int j = i + 1;
        if (j < sql.length() && sql.charAt(j) == '@') j++;
        int start = j;
        while (j < sql.length() && isNameChar(sql.charAt(j))) j++;
        if (lpsql) {
          throw new TsqlParser.ParseException(
              "LPSQL batch uses a T-SQL variable " + sql.substring(i, j));
        }
        prev = sql.substring(start, j).toUpperCase(java.util.Locale.ROOT);
        statementStart = false;
        i = j;
        continue;
      }
      if (c == '|' && i + 1 < sql.length() && sql.charAt(i + 1) == '|') {
        if (!lpsql) {
          throw new TsqlParser.ParseException(
              "TSQL batch uses ||; T-SQL string concatenation in a procedure is +");
        }
        statementStart = false;
        i += 2;
        continue;
      }
      if (c == '(') {
        paren++;
        statementStart = false;
        i++;
        continue;
      }
      if (c == ')') {
        paren = Math.max(0, paren - 1);
        statementStart = false;
        i++;
        continue;
      }
      if (c == ';') {
        statementStart = true;
        selectStmt = false;
        prev = null;
        i++;
        continue;
      }
      if (Character.isLetter(c) || c == '_') {
        int j = i + 1;
        while (j < sql.length() && isNameChar(sql.charAt(j))) j++;
        String word = sql.substring(i, j);
        String upper = word.toUpperCase(java.util.Locale.ROOT);
        if (upper.equals("GO") && paren == 0) {
          statementStart = true;
          selectStmt = false;
          prev = null;
          i = j;
          continue;
        }
        checkWord(sql, j, lpsql, statementStart, selectStmt, paren, prev, upper);
        if (paren == 0 && statementStart && upper.equals("SELECT")) selectStmt = true;
        if (paren == 0 && statementStart && upper.equals("INSERT")) selectStmt = false;
        if (paren == 0 && upper.equals("END")) statementStart = true;
        else statementStart = false;
        prev = upper;
        i = j;
        continue;
      }
      statementStart = false;
      i++;
    }
  }

  private static void checkWord(String sql, int afterWord, boolean lpsql, boolean statementStart,
      boolean selectStmt, int paren, String prev, String upper) {
    if (lpsql) {
      if (upper.equals("TRY") || upper.equals("CATCH") || upper.equals("BREAK")
          || upper.equals("CONTINUE") || upper.equals("DEALLOCATE")) {
        throw new TsqlParser.ParseException("LPSQL batch uses T-SQL keyword " + upper);
      }
      if ("OR".equals(prev) && upper.equals("ALTER")) {
        throw new TsqlParser.ParseException(
            "LPSQL batch uses CREATE OR ALTER; LPSQL procedures use CREATE OR REPLACE");
      }
      if ("FETCH".equals(prev) && upper.equals("NEXT")) {
        throw new TsqlParser.ParseException(
            "LPSQL batch uses FETCH NEXT; LPSQL fetch is FETCH cursor INTO");
      }
      if ("BEGIN".equals(prev) && (upper.equals("TRY") || upper.equals("CATCH"))) {
        throw new TsqlParser.ParseException("LPSQL batch uses T-SQL keyword " + upper);
      }
      if (statementStart && upper.equals("TSQL")) {
        throw new TsqlParser.ParseException("LPSQL batch uses TSQL keyword");
      }
      return;
    }
    if (upper.equals("LEAVE") || upper.equals("EXCEPTION") || upper.equals("SQLCODE")) {
      throw new TsqlParser.ParseException("TSQL batch uses LPSQL keyword " + upper);
    }
    if (statementStart && (upper.equals("LOOP") || upper.equals("LPSQL"))) {
      throw new TsqlParser.ParseException("TSQL batch uses LPSQL keyword " + upper);
    }
    if (statementStart && upper.equals("FOR")) {
      throw new TsqlParser.ParseException("TSQL batch uses FOR; T-SQL loops are WHILE");
    }
    if ("END".equals(prev) && (upper.equals("LOOP") || upper.equals("IF"))
        && closerFollows(sql, afterWord)) {
      throw new TsqlParser.ParseException("TSQL batch uses LPSQL phrase END " + upper);
    }
    if ("OR".equals(prev) && upper.equals("REPLACE")) {
      throw new TsqlParser.ParseException(
          "TSQL batch uses CREATE OR REPLACE; T-SQL procedures use CREATE OR ALTER");
    }
    if (("EXECUTE".equals(prev) || "EXEC".equals(prev)) && upper.equals("IMMEDIATE")) {
      throw new TsqlParser.ParseException(
          "TSQL batch uses EXECUTE IMMEDIATE; T-SQL dynamic SQL is EXEC(...)");
    }
    if (selectStmt && paren == 0 && upper.equals("INTO")) {
      throw new TsqlParser.ParseException(
          "TSQL batch uses SELECT INTO; T-SQL assignment is SELECT @var =");
    }
  }

  /** True when {@code END IF} or {@code END LOOP} closes an LPSQL block. */
  private static boolean closerFollows(String sql, int i) {
    i = skipIgnored(sql, i);
    if (i >= sql.length()) return true;
    char c = sql.charAt(i);
    if (c == ';') return true;
    if (!Character.isLetter(c)) return false;
    int j = i + 1;
    while (j < sql.length() && isNameChar(sql.charAt(j))) j++;
    String next = sql.substring(i, j).toUpperCase(java.util.Locale.ROOT);
    return next.equals("GO") || next.equals("END") || next.equals("ELSE");
  }

  private static int skipIgnored(String sql, int i) {
    while (i < sql.length()) {
      char c = sql.charAt(i);
      if (c == '\uFEFF') {
        i++;
        continue;
      }
      if (Character.isWhitespace(c)) {
        i++;
        continue;
      }
      if (c == '-' && i + 1 < sql.length() && sql.charAt(i + 1) == '-') {
        while (i < sql.length() && sql.charAt(i) != '\n') i++;
        continue;
      }
      if (c == '/' && i + 1 < sql.length() && sql.charAt(i + 1) == '*') {
        i += 2;
        while (i + 1 < sql.length() && !(sql.charAt(i) == '*' && sql.charAt(i + 1) == '/')) i++;
        i = Math.min(sql.length(), i + 2);
        continue;
      }
      break;
    }
    return i;
  }

  private static boolean startsWithKeyword(String sql, int i, String keyword) {
    if (i < 0 || i + keyword.length() > sql.length()) return false;
    if (!sql.regionMatches(true, i, keyword, 0, keyword.length())) return false;
    int end = i + keyword.length();
    return end >= sql.length() || !isNameChar(sql.charAt(end));
  }

  private static boolean isNameChar(char c) {
    return Character.isLetterOrDigit(c) || c == '_';
  }

  private static String readQuoted(String sql, int i, char q) {
    int end = SqlText.skipSourceLiteral(sql, i);
    boolean closed = end > i && end <= sql.length() && sql.charAt(end - 1) == q;
    if (!closed) throw new TsqlParser.ParseException("Unclosed string after " + q);
    return SqlText.value(SqlText.impalaBody(sql, i, end));
  }
}
