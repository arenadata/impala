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

package org.apache.impala.service.flight;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/** Statement classification by the first keyword (comments and parentheses skipped). */
final class FlightSqlStatements {
  private FlightSqlStatements() {}

  private static final Set<String> QUERY_KEYWORDS = new HashSet<>(Arrays.asList(
      "select", "with", "values", "show", "describe", "desc", "explain", "table"));

  /** True for statements that return rows (queries, SHOW, DESCRIBE, EXPLAIN). */
  static boolean isQuery(String sql) {
    String keyword = firstKeyword(sql);
    return keyword != null && QUERY_KEYWORDS.contains(keyword);
  }

  /** The first keyword in lower case, or null. */
  static String firstKeyword(String sql) {
    if (sql == null) return null;
    int i = 0;
    int n = sql.length();
    while (i < n) {
      char c = sql.charAt(i);
      if (Character.isWhitespace(c) || c == '(') {
        i++;
      } else if (sql.startsWith("--", i)) {
        int end = sql.indexOf('\n', i);
        i = end < 0 ? n : end + 1;
      } else if (sql.startsWith("/*", i)) {
        int end = sql.indexOf("*/", i + 2);
        if (end < 0) return null;
        i = end + 2;
      } else {
        int start = i;
        while (i < n && Character.isLetter(sql.charAt(i))) i++;
        return i > start ? sql.substring(start, i).toLowerCase(Locale.ROOT) : null;
      }
    }
    return null;
  }
}
