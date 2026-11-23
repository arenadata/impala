package org.apache.impala.tsql;

/**
 * ADH-TSQL. Impala string literals use backslash escapes, not doubled quotes.
 * Procedure text is lexed once into that form so later substitution and the
 * statement sent to Impala agree.
 */
public final class SqlText {
  private SqlText() {}

  /** Index just past the literal that starts at {@code sql[start]}. */
  public static int skipQuoted(String sql, int start) {
    char q = sql.charAt(start);
    int n = sql.length();
    int j = start + 1;
    while (j < n) {
      char c = sql.charAt(j);
      if (c == '\\') {
        // A trailing backslash stays inside an unclosed literal.
        j += 2;
        continue;
      }
      if (c == q) {
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

  /**
   * Body of {@code sql[start, end)} as it must appear between single quotes in
   * Impala SQL. Doubled quotes become {@code \'} ; a backslash escape is kept.
   */
  public static String impalaBody(String sql, int start, int end) {
    char q = sql.charAt(start);
    int stop = end - 1 > start && sql.charAt(end - 1) == q && !escaped(sql, start, end - 1)
        ? end - 1 : end;
    StringBuilder sb = new StringBuilder();
    for (int j = start + 1; j < stop; j++) {
      char c = sql.charAt(j);
      if (c == '\\' && j + 1 < stop) {
        sb.append(c).append(sql.charAt(++j));
      } else if (c == q && j + 1 < stop && sql.charAt(j + 1) == q) {
        sb.append(q == '\'' ? "\\'" : String.valueOf(q));
        j++;
      } else if (c == '\'') {
        // A single quote inside an LPSQL double-quoted string.
        sb.append("\\'");
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  /** Value of a procedure-level literal. Only {@code \'} {@code \"} and {@code \\} are decoded. */
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

  /** True when the quote at {@code pos} is escaped by a backslash. */
  public static boolean escaped(String sql, int start, int pos) {
    int slashes = 0;
    for (int k = pos - 1; k > start; k--) {
      if (sql.charAt(k) != '\\') break;
      slashes++;
    }
    return (slashes % 2) == 1;
  }
}
