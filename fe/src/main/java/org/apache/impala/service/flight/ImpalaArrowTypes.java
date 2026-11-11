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

import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.arrow.flight.sql.FlightSqlColumnMetadata;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.hive.service.rpc.thrift.TTypeId;

/**
 * Impala column types -> Arrow types, one mapping for query results (HS2 type ids) and
 * table metadata (Impala type names from GetColumns), so a table's schema from GetTables
 * equals the schema of "select *". TIMESTAMP -> timestamp(us) without time zone; complex
 * types (ARRAY / MAP / STRUCT) and CHAR / VARCHAR / STRING -> utf8.
 */
final class ImpalaArrowTypes {
  private ImpalaArrowTypes() {}

  /** Impala's default DECIMAL. */
  static final int DEFAULT_PRECISION = 9;
  static final int DEFAULT_SCALE = 0;

  private static final Pattern DECIMAL =
      Pattern.compile("DECIMAL\\s*\\(\\s*(\\d+)\\s*(?:,\\s*(\\d+)\\s*)?\\)");

  static ArrowType forTypeId(TTypeId id, int precision, int scale) {
    switch (id) {
      case BOOLEAN_TYPE: return ArrowType.Bool.INSTANCE;
      case TINYINT_TYPE: return new ArrowType.Int(8, true);
      case SMALLINT_TYPE: return new ArrowType.Int(16, true);
      case INT_TYPE: return new ArrowType.Int(32, true);
      case BIGINT_TYPE: return new ArrowType.Int(64, true);
      case FLOAT_TYPE: return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
      case DOUBLE_TYPE: return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
      case DECIMAL_TYPE: return new ArrowType.Decimal(precision, scale, 128);
      case TIMESTAMP_TYPE: return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
      case DATE_TYPE: return new ArrowType.Date(DateUnit.DAY);
      case BINARY_TYPE: return ArrowType.Binary.INSTANCE;
      case NULL_TYPE: return ArrowType.Null.INSTANCE;
      default:
        // STRING, VARCHAR, CHAR, ARRAY, MAP, STRUCT, UNION, ...: text
        return ArrowType.Utf8.INSTANCE;
    }
  }

  /**
   * Length reported for STRING, BINARY and complex types (no declared length):
   * Integer.MAX_VALUE, as Impala's HS2 GetColumns reports for STRING
   * (Type.getColumnSize), so JDBC clients show STRING, not STRING(n). The ODBC driver's
   * own default is 1024; a DSN stringColumnLength overrides it on the client.
   */
  static final int STRING_COLUMN_LENGTH = Integer.MAX_VALUE;

  /** Base type name (no parameters) of an HS2 type id: INT, DECIMAL, VARCHAR, ... */
  static String baseName(TTypeId id) {
    String name = id.name();
    return name.endsWith("_TYPE") ? name.substring(0, name.length() - 5) : name;
  }

  /** Base type name of an Impala type name: DECIMAL(10,2) -> DECIMAL. */
  static String baseName(String typeName) {
    if (typeName == null) return "STRING";
    String name = typeName.trim().toUpperCase(Locale.ROOT);
    for (int i = 0; i < name.length(); i++) {
      char c = name.charAt(i);
      if (c == '(' || c == '<' || Character.isWhitespace(c)) return name.substring(0, i);
    }
    return name;
  }

  /**
   * Flight SQL column metadata (ARROW:FLIGHT:SQL:*) for a column, which ODBC / JDBC
   * clients use for the type name, precision, scale, case sensitivity and searchability;
   * without it the ODBC driver reports generic types, 1024 characters for strings and
   * "not searchable". 'precision' / 'scale' / 'length' may be null (unknown);
   * STRING_COLUMN_LENGTH is reported for STRING, BINARY and complex types.
   * 'schema' / 'table' only for table columns (null for query results).
   */
  static Map<String, String> columnMetadata(String baseName, Integer precision,
      Integer scale, Integer length, boolean resultColumn,
      String schema, String table) {
    FlightSqlColumnMetadata.Builder b = new FlightSqlColumnMetadata.Builder()
        .typeName(baseName)
        .isAutoIncrement(false)
        .isReadOnly(resultColumn);
    boolean complex = false;
    switch (baseName) {
      case "DECIMAL":
        if (precision != null) b.precision(precision);
        if (scale != null) b.scale(scale);
        break;
      case "VARCHAR":
      case "CHAR":
        b.precision(length != null && length > 0 ? length : STRING_COLUMN_LENGTH);
        break;
      case "STRING":
      case "BINARY":
        b.precision(STRING_COLUMN_LENGTH);
        break;
      case "ARRAY":
      case "MAP":
      case "STRUCT":
      case "UNION":
        b.precision(STRING_COLUMN_LENGTH);
        complex = true;
        break;
      case "TIMESTAMP":
        // values are sent with microseconds
        b.scale(6);
        break;
      default:
        break;
    }
    b.isCaseSensitive(baseName.equals("STRING") || baseName.equals("VARCHAR")
        || baseName.equals("CHAR"));
    b.isSearchable(!complex);
    if (schema != null) b.schemaName(schema);
    if (table != null) b.tableName(table);
    return b.build().getMetadataMap();
  }

  /** Arrow type for an Impala type name such as INT, DECIMAL(10,2), ARRAY<INT>. */
  static ArrowType forTypeName(String typeName) {
    if (typeName == null) return ArrowType.Utf8.INSTANCE;
    String name = typeName.trim().toUpperCase(Locale.ROOT);
    int end = name.length();
    for (int i = 0; i < name.length(); i++) {
      char c = name.charAt(i);
      if (c == '(' || c == '<' || Character.isWhitespace(c)) {
        end = i;
        break;
      }
    }
    switch (name.substring(0, end)) {
      case "BOOLEAN": return forTypeId(TTypeId.BOOLEAN_TYPE, 0, 0);
      case "TINYINT": return forTypeId(TTypeId.TINYINT_TYPE, 0, 0);
      case "SMALLINT": return forTypeId(TTypeId.SMALLINT_TYPE, 0, 0);
      case "INT": case "INTEGER": return forTypeId(TTypeId.INT_TYPE, 0, 0);
      case "BIGINT": return forTypeId(TTypeId.BIGINT_TYPE, 0, 0);
      case "FLOAT": case "REAL": return forTypeId(TTypeId.FLOAT_TYPE, 0, 0);
      case "DOUBLE": return forTypeId(TTypeId.DOUBLE_TYPE, 0, 0);
      case "DECIMAL": {
        Matcher m = DECIMAL.matcher(name);
        int precision = DEFAULT_PRECISION;
        int scale = DEFAULT_SCALE;
        if (m.lookingAt()) {
          precision = Integer.parseInt(m.group(1));
          scale = m.group(2) == null ? 0 : Integer.parseInt(m.group(2));
        }
        return forTypeId(TTypeId.DECIMAL_TYPE, precision, scale);
      }
      case "TIMESTAMP": return forTypeId(TTypeId.TIMESTAMP_TYPE, 0, 0);
      case "DATE": return forTypeId(TTypeId.DATE_TYPE, 0, 0);
      case "BINARY": return forTypeId(TTypeId.BINARY_TYPE, 0, 0);
      case "NULL": return forTypeId(TTypeId.NULL_TYPE, 0, 0);
      default: return ArrowType.Utf8.INSTANCE;
    }
  }
}
