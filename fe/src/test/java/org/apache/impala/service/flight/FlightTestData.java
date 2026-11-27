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

import static org.apache.impala.service.flight.FakeHs2.bools;
import static org.apache.impala.service.flight.FakeHs2.column;
import static org.apache.impala.service.flight.FakeHs2.ints;
import static org.apache.impala.service.flight.FakeHs2.strings;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.apache.hive.service.rpc.thrift.TBinaryColumn;
import org.apache.hive.service.rpc.thrift.TByteColumn;
import org.apache.hive.service.rpc.thrift.TColumn;
import org.apache.hive.service.rpc.thrift.TColumnDesc;
import org.apache.hive.service.rpc.thrift.TDoubleColumn;
import org.apache.hive.service.rpc.thrift.TGetColumnsReq;
import org.apache.hive.service.rpc.thrift.TGetCrossReferenceReq;
import org.apache.hive.service.rpc.thrift.TGetPrimaryKeysReq;
import org.apache.hive.service.rpc.thrift.TGetSchemasReq;
import org.apache.hive.service.rpc.thrift.TGetTablesReq;
import org.apache.hive.service.rpc.thrift.TI16Column;
import org.apache.hive.service.rpc.thrift.TI64Column;
import org.apache.hive.service.rpc.thrift.TTypeId;
import org.apache.hive.service.rpc.thrift.TTypeQualifierValue;
import org.apache.hive.service.rpc.thrift.TTypeQualifiers;

/**
 * Scripted data for {@link FakeHs2}, shared by {@link FlightSqlTestServer} (the Python
 * end-to-end tests) and the JDBC driver tests: one query with every type, an empty and a
 * multi-page result, DML, errors, and a small catalog (databases 'default' and
 * 'flight_test') whose metadata calls apply the request's patterns as Impala does.
 */
final class FlightTestData {
  private FlightTestData() {}

  static final String DB = "flight_test";

  /** Every type, 3 rows: typical values, edge values, all NULL. */
  static final String TYPES_QUERY = "select * from flight_test.types";
  static final String EMPTY_QUERY = "select * from flight_test.empty";
  /** BIG_ROWS rows (id BIGINT, name STRING 'row-<id>') in pages of BIG_PAGE rows. */
  static final String BIG_QUERY = "select * from flight_test.big";
  static final int BIG_ROWS = 10000;
  static final int BIG_PAGE = 1000;
  /** DML with a row count from CloseImpalaOperation. */
  static final String INSERT = "insert into flight_test.big values (1, 'a'), (2, 'b')";
  static final long INSERT_ROWS = 2;
  /** Statement without a result set or row count. */
  static final String DDL = "create table flight_test.t (id int)";
  static final String SYNTAX_ERROR = "selec 1";
  static final String MISSING_TABLE = "select * from flight_test.missing";
  static final String NO_PRIVILEGE = "select * from flight_test.secret";
  /** Starts fine, fails while fetching. */
  static final String FETCH_ERROR = "select * from flight_test.broken";

  /** Columns of TYPES_QUERY / table flight_test.types: name, Impala type name. */
  static final String[][] TYPE_COLUMNS = {
      {"b", "BOOLEAN"}, {"ti", "TINYINT"}, {"si", "SMALLINT"}, {"i", "INT"},
      {"bi", "BIGINT"}, {"f", "FLOAT"}, {"d", "DOUBLE"}, {"dec", "DECIMAL(10,2)"},
      {"s", "STRING"}, {"v", "VARCHAR(5)"}, {"c", "CHAR(3)"}, {"ts", "TIMESTAMP"},
      {"dt", "DATE"}, {"bin", "BINARY"}, {"arr", "ARRAY<INT>"}};

  /** Installs the scripts. */
  static void install(FakeHs2 hs2) {
    installQueries(hs2);
    installMetadata(hs2);
  }

  // ---------------------------------------------------------------- queries

  private static void installQueries(FakeHs2 hs2) {
    hs2.on(TYPES_QUERY).schema(typeSchema())
        .page(bools(true, false, null),
            bytes((byte) 1, Byte.MIN_VALUE, null),
            shorts((short) 2, Short.MIN_VALUE, null),
            ints(3, Integer.MIN_VALUE, null),
            longs(4L, Long.MIN_VALUE, null),
            doubles(1.5, -0.25, null),
            doubles(2.25, -1e300, null),
            strings("12345.67", "-0.01", null),
            strings("hello", "привет ✓", null),
            strings("abc", "", null),
            strings("ab ", "xyz", null),
            strings("2026-11-12 10:20:30.123456", "1970-01-01 00:00:00", null),
            strings("2026-11-12", "1970-01-01", null),
            binaries(new byte[] {1, 2, 3}, new byte[0], null),
            strings("[1,2]", "[]", null));

    hs2.on(EMPTY_QUERY).schema(column("id", TTypeId.BIGINT_TYPE),
        column("name", TTypeId.STRING_TYPE));

    FakeHs2.Script big = hs2.on(BIG_QUERY).schema(column("id", TTypeId.BIGINT_TYPE),
        column("name", TTypeId.STRING_TYPE));
    for (int start = 0; start < BIG_ROWS; start += BIG_PAGE) {
      Long[] ids = new Long[BIG_PAGE];
      String[] names = new String[BIG_PAGE];
      for (int r = 0; r < BIG_PAGE; r++) {
        ids[r] = (long) (start + r);
        names[r] = "row-" + (start + r);
      }
      big.page(longs(ids), strings(names));
    }

    hs2.on(INSERT).dmlRows = Collections.singletonMap("", INSERT_ROWS);
    hs2.on(DDL);
    hs2.on(SYNTAX_ERROR).executeError = "ParseException: Syntax error in line 1:\n"
        + "selec 1\n^\nEncountered: IDENTIFIER\nExpected: ALTER, COMMENT, ...";
    hs2.on(MISSING_TABLE).executeError =
        "AnalysisException: Could not resolve table reference: 'flight_test.missing'";
    hs2.on(NO_PRIVILEGE).executeError = "AuthorizationException: User 'alice' does not "
        + "have privileges to execute 'SELECT' on: flight_test.secret";
    FakeHs2.Script broken = hs2.on(FETCH_ERROR).schema(column("id", TTypeId.INT_TYPE));
    broken.fetchError = "Disk I/O error on host:22000: Failed to open HDFS file "
        + "hdfs://nn/warehouse/broken/0.parq";
  }

  private static TColumnDesc[] typeSchema() {
    TColumnDesc[] columns = new TColumnDesc[TYPE_COLUMNS.length];
    for (int i = 0; i < columns.length; i++) {
      columns[i] = columnDesc(TYPE_COLUMNS[i][0], TYPE_COLUMNS[i][1]);
    }
    return columns;
  }

  /** HS2 result column of an Impala type name (DECIMAL, VARCHAR, CHAR qualifiers). */
  private static TColumnDesc columnDesc(String name, String typeName) {
    String base = typeName.replaceAll("[(<].*", "");
    TColumnDesc c = column(name, TTypeId.valueOf(base + "_TYPE"));
    Map<String, TTypeQualifierValue> q = new HashMap<>();
    java.util.regex.Matcher m =
        Pattern.compile("\\((\\d+)(?:,(\\d+))?\\)").matcher(typeName);
    if (m.find()) {
      if ("DECIMAL".equals(base)) {
        q.put("precision", TTypeQualifierValue.i32Value(Integer.parseInt(m.group(1))));
        q.put("scale", TTypeQualifierValue.i32Value(Integer.parseInt(m.group(2))));
      } else {
        q.put("characterMaximumLength",
            TTypeQualifierValue.i32Value(Integer.parseInt(m.group(1))));
      }
      c.getTypeDesc().getTypes().get(0).getPrimitiveEntry()
          .setTypeQualifiers(new TTypeQualifiers(q));
    }
    return c;
  }

  // ---------------------------------------------------------------- catalog

  private static final class Table {
    final String db, name, type;
    final String[][] columns; // name, Impala type name

    Table(String db, String name, String type, String[]... columns) {
      this.db = db;
      this.name = name;
      this.type = type;
      this.columns = columns;
    }
  }

  static final List<String> DATABASES = Arrays.asList("default", DB);

  private static final List<Table> TABLES = Arrays.asList(
      new Table(DB, "types", "TABLE", TYPE_COLUMNS),
      new Table(DB, "big", "TABLE", new String[] {"id", "BIGINT"},
          new String[] {"name", "STRING"}),
      new Table(DB, "parent", "TABLE", new String[] {"id", "BIGINT"},
          new String[] {"name", "STRING"}),
      new Table(DB, "child", "TABLE", new String[] {"id", "BIGINT"},
          new String[] {"parent_id", "BIGINT"}),
      new Table(DB, "big_view", "VIEW", new String[] {"id", "BIGINT"}));

  /** All table names of the catalog, in order. */
  static List<String> tableNames() {
    List<String> names = new ArrayList<>();
    for (Table t : TABLES) names.add(t.name);
    return names;
  }

  /** SQL LIKE pattern ('%', '_', '\' escapes) as a predicate; null or empty: all. */
  static Predicate<String> like(String pattern) {
    if (pattern == null || pattern.isEmpty()) return s -> true;
    StringBuilder re = new StringBuilder();
    for (int i = 0; i < pattern.length(); i++) {
      char ch = pattern.charAt(i);
      if (ch == '\\' && i + 1 < pattern.length()) {
        re.append(Pattern.quote(String.valueOf(pattern.charAt(++i))));
      } else if (ch == '%' || ch == '*') {
        re.append(".*");
      } else if (ch == '_') {
        re.append('.');
      } else {
        re.append(Pattern.quote(String.valueOf(ch)));
      }
    }
    Pattern p = Pattern.compile(re.toString(), Pattern.CASE_INSENSITIVE);
    return s -> s != null && p.matcher(s).matches();
  }

  private static void installMetadata(FakeHs2 hs2) {
    hs2.onMetadata("GetSchemas", r -> {
      TGetSchemasReq req = (TGetSchemasReq) r;
      Rows rows = new Rows("TABLE_SCHEM:S", "TABLE_CATALOG:S");
      for (String db : DATABASES) {
        if (like(req.getSchemaName()).test(db)) rows.add(db, "");
      }
      return rows.script();
    });

    hs2.onMetadata("GetTables", r -> {
      TGetTablesReq req = (TGetTablesReq) r;
      Rows rows = new Rows("TABLE_CAT:S", "TABLE_SCHEM:S", "TABLE_NAME:S",
          "TABLE_TYPE:S", "REMARKS:S");
      for (Table t : TABLES) {
        if (like(req.getSchemaName()).test(t.db) && like(req.getTableName()).test(t.name)
            && (!req.isSetTableTypes() || req.getTableTypes().contains(t.type))) {
          rows.add("", t.db, t.name, t.type, null);
        }
      }
      return rows.script();
    });

    hs2.onMetadata("GetTableTypes", r -> {
      Rows rows = new Rows("TABLE_TYPE:S");
      rows.add("TABLE");
      rows.add("VIEW");
      return rows.script();
    });

    hs2.onMetadata("GetColumns", r -> {
      TGetColumnsReq req = (TGetColumnsReq) r;
      Rows rows = new Rows("TABLE_CAT:S", "TABLE_SCHEM:S", "TABLE_NAME:S",
          "COLUMN_NAME:S", "DATA_TYPE:I", "TYPE_NAME:S", "COLUMN_SIZE:I",
          "DECIMAL_DIGITS:I", "NULLABLE:I", "REMARKS:S", "ORDINAL_POSITION:I");
      for (Table t : TABLES) {
        if (!like(req.getSchemaName()).test(t.db)
            || !like(req.getTableName()).test(t.name)) {
          continue;
        }
        for (int i = 0; i < t.columns.length; i++) {
          if (!like(req.getColumnName()).test(t.columns[i][0])) continue;
          rows.add("", t.db, t.name, t.columns[i][0], null, t.columns[i][1], null, null,
              1, null, i + 1);
        }
      }
      return rows.script();
    });

    hs2.onMetadata("GetPrimaryKeys", r -> {
      TGetPrimaryKeysReq req = (TGetPrimaryKeysReq) r;
      Rows rows = new Rows("TABLE_CAT:S", "TABLE_SCHEM:S", "TABLE_NAME:S",
          "COLUMN_NAME:S", "KEQ_SEQ:I", "PK_NAME:S");
      if (DB.equals(req.getSchemaName()) && "parent".equals(req.getTableName())) {
        rows.add("", DB, "parent", "id", 1, "parent_pk");
      }
      return rows.script();
    });

    hs2.onMetadata("GetCrossReference", r -> {
      TGetCrossReferenceReq req = (TGetCrossReferenceReq) r;
      Rows rows = new Rows("PKTABLE_CAT:S", "PKTABLE_SCHEM:S", "PKTABLE_NAME:S",
          "PKCOLUMN_NAME:S", "FKTABLE_CAT:S", "FKTABLE_SCHEM:S", "FKTABLE_NAME:S",
          "FKCOLUMN_NAME:S", "KEQ_SEQ:I", "UPDATE_RULE:I", "DELETE_RULE:I", "FK_NAME:S",
          "PK_NAME:S");
      boolean parent = !req.isSetParentTableName()
          || ("parent".equals(req.getParentTableName())
              && Objects.equals(DB, req.getParentSchemaName()));
      boolean child = !req.isSetForeignTableName()
          || ("child".equals(req.getForeignTableName())
              && Objects.equals(DB, req.getForeignSchemaName()));
      if (parent && child) {
        rows.add("", DB, "parent", "id", "", DB, "child", "parent_id", 1, null, null,
            "child_fk", "parent_pk");
      }
      return rows.script();
    });

    hs2.onMetadata("GetTypeInfo", r -> {
      Rows rows = new Rows("TYPE_NAME:S", "DATA_TYPE:I", "PRECISION:I",
          "CREATE_PARAMS:S", "CASE_SENSITIVE:B");
      rows.add("BOOLEAN", 16, null, null, false);
      rows.add("TINYINT", -6, 3, null, false);
      rows.add("SMALLINT", 5, 5, null, false);
      rows.add("INT", 4, 10, null, false);
      rows.add("BIGINT", -5, 19, null, false);
      rows.add("FLOAT", 6, 7, null, false);
      rows.add("DOUBLE", 8, 15, null, false);
      rows.add("DECIMAL", 3, 38, "precision,scale", false);
      rows.add("STRING", 12, null, null, true);
      rows.add("VARCHAR", 12, 65535, "max_length", true);
      rows.add("CHAR", 1, 255, "length", true);
      rows.add("TIMESTAMP", 93, 29, null, false);
      rows.add("DATE", 91, 10, null, false);
      rows.add("BINARY", -2, null, null, false);
      return rows.script();
    });
  }

  /** Builds a one-page metadata result from rows; columns are "NAME:S|I|B". */
  private static final class Rows {
    private final LinkedHashMap<String, Character> columns_ = new LinkedHashMap<>();
    private final List<Object[]> rows_ = new ArrayList<>();

    Rows(String... columns) {
      for (String c : columns) {
        String[] parts = c.split(":");
        columns_.put(parts[0], parts[1].charAt(0));
      }
    }

    void add(Object... values) {
      if (values.length != columns_.size()) {
        throw new IllegalArgumentException(values.length + " values for "
            + columns_.size() + " columns");
      }
      rows_.add(values);
    }

    FakeHs2.Script script() {
      List<TColumnDesc> descs = new ArrayList<>();
      List<TColumn> cols = new ArrayList<>();
      int c = 0;
      for (Map.Entry<String, Character> e : columns_.entrySet()) {
        final int index = c++;
        switch (e.getValue()) {
          case 'S':
            descs.add(column(e.getKey(), TTypeId.STRING_TYPE));
            cols.add(strings(rows_.stream().map(r -> (String) r[index])
                .toArray(String[]::new)));
            break;
          case 'I':
            descs.add(column(e.getKey(), TTypeId.INT_TYPE));
            cols.add(ints(rows_.stream().map(r -> (Integer) r[index])
                .toArray(Integer[]::new)));
            break;
          default:
            descs.add(column(e.getKey(), TTypeId.BOOLEAN_TYPE));
            cols.add(bools(rows_.stream().map(r -> (Boolean) r[index])
                .toArray(Boolean[]::new)));
        }
      }
      FakeHs2.Script script =
          new FakeHs2.Script().schema(descs.toArray(new TColumnDesc[0]));
      if (!rows_.isEmpty()) script.page(cols.toArray(new TColumn[0]));
      return script;
    }
  }

  // ---------------------------------------------------------------- columns

  private static ByteBuffer nulls(Object[] values) {
    byte[] nulls = new byte[(values.length + 7) / 8];
    for (int i = 0; i < values.length; i++) {
      if (values[i] == null) nulls[i / 8] |= (byte) (1 << (i % 8));
    }
    return ByteBuffer.wrap(nulls);
  }

  static TColumn bytes(Byte... values) {
    List<Byte> list = new ArrayList<>();
    for (Byte v : values) list.add(v == null ? 0 : v);
    return TColumn.byteVal(new TByteColumn(list, nulls(values)));
  }

  static TColumn shorts(Short... values) {
    List<Short> list = new ArrayList<>();
    for (Short v : values) list.add(v == null ? 0 : v);
    return TColumn.i16Val(new TI16Column(list, nulls(values)));
  }

  static TColumn longs(Long... values) {
    List<Long> list = new ArrayList<>();
    for (Long v : values) list.add(v == null ? 0L : v);
    return TColumn.i64Val(new TI64Column(list, nulls(values)));
  }

  static TColumn doubles(Double... values) {
    List<Double> list = new ArrayList<>();
    for (Double v : values) list.add(v == null ? 0.0 : v);
    return TColumn.doubleVal(new TDoubleColumn(list, nulls(values)));
  }

  static TColumn binaries(byte[]... values) {
    List<ByteBuffer> list = new ArrayList<>();
    for (byte[] v : values) list.add(ByteBuffer.wrap(v == null ? new byte[0] : v));
    return TColumn.binaryVal(new TBinaryColumn(list, nulls(values)));
  }

  /** The scripted SQL texts and expected values, for the Python tests' info file. */
  static Map<String, Object> describe() {
    Map<String, Object> d = new LinkedHashMap<>();
    d.put("database", DB);
    d.put("databases", DATABASES);
    d.put("tables", tableNames());
    d.put("types_query", TYPES_QUERY);
    d.put("type_columns", Arrays.asList(TYPE_COLUMNS));
    d.put("empty_query", EMPTY_QUERY);
    d.put("big_query", BIG_QUERY);
    d.put("big_rows", BIG_ROWS);
    d.put("insert", INSERT);
    d.put("insert_rows", INSERT_ROWS);
    d.put("ddl", DDL);
    d.put("syntax_error", SYNTAX_ERROR);
    d.put("missing_table", MISSING_TABLE);
    d.put("no_privilege", NO_PRIVILEGE);
    d.put("fetch_error", FETCH_ERROR);
    d.put("unicode_string", new String(
        "привет ✓".getBytes(StandardCharsets.UTF_8),
        StandardCharsets.UTF_8));
    return d;
  }
}
