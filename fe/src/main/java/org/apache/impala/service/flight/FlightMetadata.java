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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Maps Impala's JDBC-shaped HS2 metadata results (TABLE_CAT, TABLE_SCHEM, ...) onto the
 * fixed Flight SQL metadata schemas by column name (JDBC and Flight SQL order columns
 * differently, e.g. GetSchemas is TABLE_SCHEM, TABLE_CATALOG in JDBC). Ported from
 * Kyuubi's KyuubiFlightMetadata. Rows are lower-case column name -> value.
 */
final class FlightMetadata {
  private FlightMetadata() {}

  /** Flight field -> candidate HS2 columns (first non-null wins). */
  static final class Sources {
    private final Map<String, List<String>> sources_ = new HashMap<>();
    private final Map<String, Long> intDefaults_ = new HashMap<>();
    private final Map<String, Boolean> boolDefaults_ = new HashMap<>();

    Sources map(String field, String... columns) {
      sources_.put(field, Arrays.asList(columns));
      return this;
    }

    Sources intDefault(String field, long value) {
      intDefaults_.put(field, value);
      return this;
    }

    Sources boolDefault(String field, boolean value) {
      boolDefaults_.put(field, value);
      return this;
    }

    List<String> columns(String field) {
      return sources_.getOrDefault(field, Collections.emptyList());
    }
  }

  static final Sources SCHEMAS = new Sources()
      .map("catalog_name", "TABLE_CATALOG", "TABLE_CAT")
      .map("db_schema_name", "TABLE_SCHEM");

  static final Sources TABLES = new Sources()
      .map("catalog_name", "TABLE_CAT")
      .map("db_schema_name", "TABLE_SCHEM")
      .map("table_name", "TABLE_NAME")
      .map("table_type", "TABLE_TYPE");

  static final Sources TABLE_TYPES = new Sources().map("table_type", "TABLE_TYPE");

  static final Sources PRIMARY_KEYS = new Sources()
      .map("catalog_name", "TABLE_CAT")
      .map("db_schema_name", "TABLE_SCHEM")
      .map("table_name", "TABLE_NAME")
      .map("column_name", "COLUMN_NAME")
      .map("key_name", "PK_NAME")
      // Hive and Impala name it KEQ_SEQ
      .map("key_sequence", "KEY_SEQ", "KEQ_SEQ");

  /** Imported keys, exported keys and cross reference share one schema. */
  static final Sources FOREIGN_KEYS = new Sources()
      .map("pk_catalog_name", "PKTABLE_CAT")
      .map("pk_db_schema_name", "PKTABLE_SCHEM")
      .map("pk_table_name", "PKTABLE_NAME")
      .map("pk_column_name", "PKCOLUMN_NAME")
      .map("fk_catalog_name", "FKTABLE_CAT")
      .map("fk_db_schema_name", "FKTABLE_SCHEM")
      .map("fk_table_name", "FKTABLE_NAME")
      .map("fk_column_name", "FKCOLUMN_NAME")
      .map("key_sequence", "KEY_SEQ", "KEQ_SEQ")
      .map("fk_key_name", "FK_NAME")
      .map("pk_key_name", "PK_NAME")
      .map("update_rule", "UPDATE_RULE")
      .map("delete_rule", "DELETE_RULE")
      // Flight SQL uses the JDBC rule codes: missing -> NO ACTION
      .intDefault("update_rule", java.sql.DatabaseMetaData.importedKeyNoAction)
      .intDefault("delete_rule", java.sql.DatabaseMetaData.importedKeyNoAction);

  /** XDBC type info (GetTypeInfo, JDBC shape) -> GET_TYPE_INFO_SCHEMA. */
  static final Sources TYPE_INFO = new Sources()
      .map("type_name", "TYPE_NAME")
      .map("data_type", "DATA_TYPE")
      .map("column_size", "PRECISION", "COLUMN_SIZE")
      .map("literal_prefix", "LITERAL_PREFIX")
      .map("literal_suffix", "LITERAL_SUFFIX")
      .map("create_params", "CREATE_PARAMS")
      .map("nullable", "NULLABLE")
      .map("case_sensitive", "CASE_SENSITIVE")
      .map("searchable", "SEARCHABLE")
      .map("unsigned_attribute", "UNSIGNED_ATTRIBUTE")
      .map("fixed_prec_scale", "FIXED_PREC_SCALE")
      .map("auto_increment", "AUTO_INCREMENT")
      .map("local_type_name", "LOCAL_TYPE_NAME")
      .map("minimum_scale", "MINIMUM_SCALE")
      .map("maximum_scale", "MAXIMUM_SCALE")
      .map("sql_data_type", "SQL_DATA_TYPE", "DATA_TYPE")
      .map("datetime_subcode", "SQL_DATETIME_SUB")
      .map("num_prec_radix", "NUM_PREC_RADIX")
      .map("interval_precision", "INTERVAL_PRECISION")
      .intDefault("nullable", java.sql.DatabaseMetaData.typeNullable)
      .intDefault("searchable", java.sql.DatabaseMetaData.typeSearchable);

  /** Flight GetTables field with the IPC-serialized Arrow schema of the table. */
  static final String TABLE_SCHEMA = "table_schema";

  /** First non-null, non-empty-for-catalogs value of the source columns. */
  static Object lookup(Map<String, Object> row, List<String> columns) {
    for (String c : columns) {
      Object v = row.get(c.toLowerCase(java.util.Locale.ROOT));
      if (v != null) return v;
    }
    return null;
  }

  /**
   * Fills 'root' (a Flight SQL metadata schema) from rows. Impala has no catalogs: empty
   * catalog names become null. Required strings without a value get "", required integers
   * and booleans their default (0 / false unless set in 'sources'). 'tableSchemas' feeds
   * table_schema (keyed by schema + "." + table; empty schema if missing).
   */
  static void fill(VectorSchemaRoot root, List<Map<String, Object>> rows, Sources sources,
      Map<String, byte[]> tableSchemas) {
    root.allocateNew();
    for (Field field : root.getSchema().getFields()) {
      FieldVector vector = root.getVector(field.getName());
      for (int i = 0; i < rows.size(); i++) {
        Map<String, Object> row = rows.get(i);
        if (TABLE_SCHEMA.equals(field.getName())) {
          byte[] schema = tableSchemas.get(tableKey(row));
          ((VarBinaryVector) vector).setSafe(i, schema != null ? schema : EMPTY_SCHEMA);
          continue;
        }
        Object value = lookup(row, sources.columns(field.getName()));
        if (value instanceof String && ((String) value).isEmpty()
            && field.getName().endsWith("catalog_name")) {
          value = null;
        }
        set(vector, field, i, value, sources);
      }
      vector.setValueCount(rows.size());
    }
    root.setRowCount(rows.size());
  }

  private static void set(FieldVector vector, Field field, int i, Object value,
      Sources sources) {
    String name = field.getName();
    if (vector instanceof VarCharVector) {
      if (value == null && field.isNullable()) {
        vector.setNull(i);
      } else {
        ((VarCharVector) vector).setSafe(i, (value == null ? "" : value.toString())
            .getBytes(StandardCharsets.UTF_8));
      }
    } else if (vector instanceof BaseIntVector) {
      Long n = toLong(value);
      if (n == null && field.isNullable()) {
        vector.setNull(i);
      } else {
        ((BaseIntVector) vector).setWithPossibleTruncate(i,
            n != null ? n : sources.intDefaults_.getOrDefault(name, 0L));
      }
    } else if (vector instanceof BitVector) {
      Boolean b = toBool(value);
      if (b == null && field.isNullable()) {
        vector.setNull(i);
      } else {
        boolean bit = b != null ? b : sources.boolDefaults_.getOrDefault(name, false);
        ((BitVector) vector).setSafe(i, bit ? 1 : 0);
      }
    } else if (vector instanceof ListVector) {
      // create_params: JDBC gives a comma separated string
      ListVector list = (ListVector) vector;
      if (value == null || value.toString().trim().isEmpty()) {
        if (field.isNullable()) {
          list.setNull(i);
        } else {
          list.startNewValue(i);
          list.endValue(i, 0);
        }
        return;
      }
      VarCharVector items = (VarCharVector) list.getDataVector();
      int start = list.startNewValue(i);
      String[] parts = value.toString().split(",");
      for (int p = 0; p < parts.length; p++) {
        items.setSafe(start + p, parts[p].trim().getBytes(StandardCharsets.UTF_8));
      }
      list.endValue(i, parts.length);
    } else {
      vector.setNull(i);
    }
  }

  /**
   * Arrow type of a GetColumns row. Impala's TYPE_NAME is plain "DECIMAL"; precision and
   * scale are in COLUMN_SIZE and DECIMAL_DIGITS.
   */
  private static ArrowType columnType(Map<String, Object> column) {
    ArrowType type =
        ImpalaArrowTypes.forTypeName(Objects.toString(column.get("type_name"), null));
    if (type instanceof ArrowType.Decimal) {
      Long precision = toLong(column.get("column_size"));
      Long scale = toLong(column.get("decimal_digits"));
      if (precision != null && precision > 0) {
        return new ArrowType.Decimal(precision.intValue(),
            scale == null ? 0 : scale.intValue(), 128);
      }
    }
    return type;
  }

  /** Flight SQL column metadata of a GetColumns row (a table column). */
  private static Map<String, String> columnMetadata(Map<String, Object> column) {
    Long size = toLong(column.get("column_size"));
    Long digits = toLong(column.get("decimal_digits"));
    String name =
        ImpalaArrowTypes.baseName(Objects.toString(column.get("type_name"), null));
    ArrowType type = columnType(column);
    Integer precision = null;
    Integer scale = null;
    if (type instanceof ArrowType.Decimal) {
      precision = ((ArrowType.Decimal) type).getPrecision();
      scale = ((ArrowType.Decimal) type).getScale();
    }
    Object schema = column.get("table_schem");
    if (schema == null) schema = column.get("table_md");
    return ImpalaArrowTypes.columnMetadata(name, precision, scale,
        size == null ? null : size.intValue(), false,
        schema == null ? null : schema.toString(),
        Objects.toString(column.get("table_name"), null));
  }

  private static Long toLong(Object value) {
    if (value instanceof Number) return ((Number) value).longValue();
    if (value instanceof String && !((String) value).trim().isEmpty()) {
      try {
        return Long.parseLong(((String) value).trim());
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  private static Boolean toBool(Object value) {
    if (value instanceof Boolean) return (Boolean) value;
    if (value instanceof Number) return ((Number) value).intValue() != 0;
    if (value instanceof String && !((String) value).trim().isEmpty()) {
      return Boolean.parseBoolean(((String) value).trim());
    }
    return null;
  }

  private static final byte[] EMPTY_SCHEMA =
      new Schema(Collections.emptyList()).serializeAsMessage();

  /**
   * schema + "." + table of a GetTables or GetColumns row. Impala's GetColumns names the
   * schema column TABLE_MD instead of TABLE_SCHEM.
   */
  static String tableKey(Map<String, Object> row) {
    Object schema = row.get("table_schem");
    if (schema == null) schema = row.get("table_md");
    return Objects.toString(schema, "") + "."
        + Objects.toString(row.get("table_name"), "");
  }

  /**
   * IPC-serialized Arrow schema per table from GetColumns rows (TABLE_SCHEM or TABLE_MD,
   * TABLE_NAME, COLUMN_NAME, TYPE_NAME, NULLABLE, ORDINAL_POSITION); types from Impala's
   * type names, so they equal the types of "select *"; fields carry Flight SQL column
   * metadata.
   */
  static Map<String, byte[]> tableSchemas(List<Map<String, Object>> columnRows) {
    Map<String, List<Map<String, Object>>> byTable = new LinkedHashMap<>();
    for (Map<String, Object> row : columnRows) {
      byTable.computeIfAbsent(tableKey(row), k -> new ArrayList<>()).add(row);
    }
    Map<String, byte[]> out = new HashMap<>();
    for (Map.Entry<String, List<Map<String, Object>>> e : byTable.entrySet()) {
      List<Map<String, Object>> columns = e.getValue();
      columns.sort(Comparator.comparingLong(c -> {
        Long pos = toLong(c.get("ordinal_position"));
        return pos == null ? Long.MAX_VALUE : pos;
      }));
      List<Field> fields = new ArrayList<>();
      for (Map<String, Object> c : columns) {
        Long nullable = toLong(c.get("nullable"));
        boolean isNullable = nullable == null
            || nullable != java.sql.DatabaseMetaData.columnNoNulls;
        fields.add(new Field(Objects.toString(c.get("column_name"), ""),
            new FieldType(isNullable, columnType(c), null,
                columnMetadata(c)),
            Collections.emptyList()));
      }
      out.put(e.getKey(), new Schema(fields).serializeAsMessage());
    }
    return out;
  }
}
