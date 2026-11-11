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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

public class FlightMetadataTest {

  private static Map<String, Object> row(Object... kv) {
    Map<String, Object> row = new HashMap<>();
    for (int i = 0; i < kv.length; i += 2) row.put((String) kv[i], kv[i + 1]);
    return row;
  }

  private static Object get(VectorSchemaRoot root, String field, int row) {
    Object v = root.getVector(field).getObject(row);
    return v == null ? null : v instanceof byte[] ? v : v.toString();
  }

  @Test
  public void testSchemasByNameAndEmptyCatalog() {
    // JDBC order is TABLE_SCHEM, TABLE_CATALOG; Impala's catalog is ""
    List<Map<String, Object>> rows = Arrays.asList(
        row("table_schem", "default", "table_catalog", ""),
        row("table_schem", "vdmitriev", "table_catalog", ""));
    try (BufferAllocator a = new RootAllocator();
         VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_SCHEMAS_SCHEMA, a)) {
      FlightMetadata.fill(root, rows, FlightMetadata.SCHEMAS, Collections.emptyMap());
      assertEquals(2, root.getRowCount());
      assertNull(get(root, "catalog_name", 0));
      assertEquals("vdmitriev", get(root, "db_schema_name", 1));
    }
  }

  @Test
  public void testKeysAndRules() {
    try (BufferAllocator a = new RootAllocator();
         VectorSchemaRoot pk =
             VectorSchemaRoot.create(Schemas.GET_PRIMARY_KEYS_SCHEMA, a);
         VectorSchemaRoot fk =
             VectorSchemaRoot.create(Schemas.GET_IMPORTED_KEYS_SCHEMA, a)) {
      FlightMetadata.fill(pk, Collections.singletonList(row("table_cat", "",
          "table_schem", "s", "table_name", "orders", "column_name", "id", "keq_seq", 1,
          "pk_name", "orders_pk")), FlightMetadata.PRIMARY_KEYS, Collections.emptyMap());
      assertEquals("1", get(pk, "key_sequence", 0));
      assertEquals("orders_pk", get(pk, "key_name", 0));
      assertNull(get(pk, "catalog_name", 0));
      FlightMetadata.fill(fk, Collections.singletonList(row("pktable_schem", "s",
          "pktable_name", "customers", "pkcolumn_name", "id", "fktable_schem", "s",
          "fktable_name", "orders", "fkcolumn_name", "customer_id", "keq_seq", 1,
          "update_rule", 0)), FlightMetadata.FOREIGN_KEYS, Collections.emptyMap());
      assertEquals("customers", get(fk, "pk_table_name", 0));
      assertEquals("customer_id", get(fk, "fk_column_name", 0));
      assertEquals("1", get(fk, "key_sequence", 0));
      assertEquals("0", get(fk, "update_rule", 0));
      // missing rule: NO ACTION (3)
      assertEquals("3", get(fk, "delete_rule", 0));
    }
  }

  @Test
  public void testTypeInfo() {
    List<Map<String, Object>> rows = Collections.singletonList(row("type_name", "DECIMAL",
        "data_type", 3, "precision", 38, "create_params", "precision, scale",
        "nullable", 1, "case_sensitive", false, "searchable", 3,
        "fixed_prec_scale", false, "minimum_scale", 0, "maximum_scale", 38,
        "num_prec_radix", 10));
    try (BufferAllocator a = new RootAllocator();
         VectorSchemaRoot root =
             VectorSchemaRoot.create(Schemas.GET_TYPE_INFO_SCHEMA, a)) {
      FlightMetadata.fill(root, rows, FlightMetadata.TYPE_INFO, Collections.emptyMap());
      assertEquals("DECIMAL", get(root, "type_name", 0));
      assertEquals("38", get(root, "column_size", 0));
      assertEquals("[\"precision\",\"scale\"]", get(root, "create_params", 0));
      // SQL_DATA_TYPE missing: DATA_TYPE
      assertEquals("3", get(root, "sql_data_type", 0));
      assertNull(get(root, "literal_prefix", 0));
      assertEquals("false", get(root, "case_sensitive", 0));
    }
  }

  @Test
  public void testTableSchemas() throws Exception {
    List<Map<String, Object>> columns = Arrays.asList(
        row("table_schem", "s", "table_name", "t", "column_name", "name",
            "type_name", "VARCHAR(5)", "nullable", 1, "ordinal_position", 2),
        row("table_schem", "s", "table_name", "t", "column_name", "id",
            "type_name", "INT", "nullable", 0, "ordinal_position", 1),
        // Impala's GetColumns calls the schema column TABLE_MD
        row("table_md", "s", "table_name", "t", "column_name", "price",
            "type_name", "DECIMAL(10,2)", "nullable", 1, "ordinal_position", 3),
        // Impala: plain DECIMAL, precision / scale in COLUMN_SIZE / DECIMAL_DIGITS
        row("table_md", "s", "table_name", "t", "column_name", "amount",
            "type_name", "DECIMAL", "column_size", 12, "decimal_digits", 4,
            "nullable", 1, "ordinal_position", 4));
    Map<String, byte[]> schemas = FlightMetadata.tableSchemas(columns);
    Schema schema = MessageSerializer.deserializeSchema(new ReadChannel(
        Channels.newChannel(new ByteArrayInputStream(schemas.get("s.t")))));
    assertEquals("id", schema.getFields().get(0).getName());
    assertEquals(new ArrowType.Int(32, true), schema.getFields().get(0).getType());
    assertEquals(false, schema.getFields().get(0).isNullable());
    assertEquals(ArrowType.Utf8.INSTANCE, schema.getFields().get(1).getType());
    assertEquals(new ArrowType.Decimal(10, 2, 128), schema.getFields().get(2).getType());
    assertEquals(new ArrowType.Decimal(12, 4, 128), schema.getFields().get(3).getType());
    // Flight SQL column metadata for table columns
    Map<String, String> amount = schema.getFields().get(3).getMetadata();
    assertEquals("DECIMAL", amount.get("ARROW:FLIGHT:SQL:TYPE_NAME"));
    assertEquals("12", amount.get("ARROW:FLIGHT:SQL:PRECISION"));
    assertEquals("4", amount.get("ARROW:FLIGHT:SQL:SCALE"));
    assertEquals("s", amount.get("ARROW:FLIGHT:SQL:SCHEMA_NAME"));
    assertEquals("t", amount.get("ARROW:FLIGHT:SQL:TABLE_NAME"));
    Map<String, String> name = schema.getFields().get(1).getMetadata();
    assertEquals("VARCHAR", name.get("ARROW:FLIGHT:SQL:TYPE_NAME"));
    assertEquals("0", name.get("ARROW:FLIGHT:SQL:IS_READ_ONLY"));
    try (BufferAllocator a = new RootAllocator();
         VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_TABLES_SCHEMA, a)) {
      FlightMetadata.fill(root, Arrays.asList(
          row("table_schem", "s", "table_name", "t", "table_type", "TABLE"),
          row("table_schem", "s", "table_name", "v", "table_type", "VIEW")),
          FlightMetadata.TABLES, schemas);
      assertEquals(schemas.get("s.t").length,
          ((byte[]) get(root, "table_schema", 0)).length);
      // no columns: empty schema
      Schema empty = MessageSerializer.deserializeSchema(
          new ReadChannel(Channels.newChannel(
              new ByteArrayInputStream((byte[]) get(root, "table_schema", 1)))));
      assertEquals(0, empty.getFields().size());
      assertEquals("VIEW", get(root, "table_type", 1));
    }
  }
}
