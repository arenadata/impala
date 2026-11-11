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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.util.TableRef;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hive.service.rpc.thrift.TGetCrossReferenceReq;
import org.apache.hive.service.rpc.thrift.TGetPrimaryKeysReq;
import org.apache.hive.service.rpc.thrift.TGetSchemasReq;
import org.apache.hive.service.rpc.thrift.TGetTablesReq;
import org.apache.hive.service.rpc.thrift.TTypeId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Metadata commands through the Flight SQL endpoint, against a fake HS2. */
public class ImpalaFlightMetadataTest {
  private static final CallOption ALICE;
  static {
    FlightCallHeaders headers = new FlightCallHeaders();
    headers.insert(ImpalaFlightAuthenticator.USER_HEADER, "alice");
    ALICE = new HeaderCallOption(headers);
  }

  private FakeHs2 hs2_;
  private ImpalaFlightSqlServer server_;
  private BufferAllocator allocator_;
  private FlightSqlClient sql_;

  @Before
  public void setUp() throws Exception {
    hs2_ = new FakeHs2();
    Configuration conf = new Configuration(false);
    conf.set(ImpalaFlightSqlServer.BIND_HOST, "127.0.0.1");
    conf.setInt(ImpalaFlightSqlServer.PORT, 0);
    conf.setBoolean(ImpalaFlightSqlServer.SSL_ENABLED, false);
    conf.set(ImpalaFlightAuthenticator.METHODS, "none");
    conf.set(LoopbackConfig.HOST, "127.0.0.1");
    conf.setInt(LoopbackConfig.PORT, hs2_.port);
    conf.setBoolean(LoopbackConfig.SSL, false);
    conf.set(LoopbackConfig.IMPERSONATION, "doas");
    server_ = ImpalaFlightSqlServer.start(conf, "localhost");
    allocator_ = new RootAllocator();
    sql_ = ImpalaFlightSqlServerTest.clientFor(allocator_, server_);
  }

  @After
  public void tearDown() throws Exception {
    sql_.close();
    allocator_.close();
    server_.close();
    hs2_.close();
  }

  /** Rows of a metadata result as field name -> value (strings for text). */
  private List<Map<String, Object>> rows(FlightInfo info) throws Exception {
    List<Map<String, Object>> out = new ArrayList<>();
    try (FlightStream stream =
             sql_.getStream(info.getEndpoints().get(0).getTicket(), ALICE)) {
      while (stream.next()) {
        for (int r = 0; r < stream.getRoot().getRowCount(); r++) {
          Map<String, Object> row = new HashMap<>();
          for (FieldVector v : stream.getRoot().getFieldVectors()) {
            Object value = v.getObject(r);
            row.put(v.getName(), value instanceof byte[] || value == null
                ? value : value.toString());
          }
          out.add(row);
        }
      }
    }
    return out;
  }

  @Test
  public void testCatalogsAreEmpty() throws Exception {
    assertEquals(Collections.emptyList(), rows(sql_.getCatalogs(ALICE)));
    assertTrue(hs2_.metadataRequests.isEmpty());
  }

  @Test
  public void testSchemas() throws Exception {
    hs2_.onMetadata("GetSchemas")
        .schema(column("TABLE_SCHEM", TTypeId.STRING_TYPE),
            column("TABLE_CATALOG", TTypeId.STRING_TYPE))
        .page(strings("vdmitriev"), strings(""));
    List<Map<String, Object>> rows = rows(sql_.getSchemas(null, "vd%", ALICE));
    assertEquals(1, rows.size());
    assertEquals("vdmitriev", rows.get(0).get("db_schema_name"));
    assertNull(rows.get(0).get("catalog_name"));
    TGetSchemasReq req = (TGetSchemasReq) hs2_.metadataRequests.get("GetSchemas").get(0);
    assertEquals("vd%", req.getSchemaName());
    // no pattern: all; another catalog: empty without asking HS2
    rows(sql_.getSchemas(null, null, ALICE));
    assertEquals("%", ((TGetSchemasReq) hs2_.metadataRequests.get("GetSchemas").get(1))
        .getSchemaName());
    assertEquals(Collections.emptyList(), rows(sql_.getSchemas("other", null, ALICE)));
    assertEquals(2, hs2_.metadataRequests.get("GetSchemas").size());
    assertEquals("alice",
        hs2_.opened.get(0).getConfiguration().get(LoopbackSession.DOAS_KEY));
  }

  @Test
  public void testTablesWithSchema() throws Exception {
    hs2_.onMetadata("GetTables")
        .schema(column("TABLE_CAT", TTypeId.STRING_TYPE),
            column("TABLE_SCHEM", TTypeId.STRING_TYPE),
            column("TABLE_NAME", TTypeId.STRING_TYPE),
            column("TABLE_TYPE", TTypeId.STRING_TYPE),
            column("REMARKS", TTypeId.STRING_TYPE))
        .page(strings("", ""), strings("s", "s"), strings("t", "empty"),
            strings("TABLE", "TABLE"), strings((String) null, null));
    hs2_.onMetadata("GetColumns")
        // as Impala names it
        .schema(column("TABLE_MD", TTypeId.STRING_TYPE),
            column("TABLE_NAME", TTypeId.STRING_TYPE),
            column("COLUMN_NAME", TTypeId.STRING_TYPE),
            column("TYPE_NAME", TTypeId.STRING_TYPE),
            column("NULLABLE", TTypeId.INT_TYPE),
            column("ORDINAL_POSITION", TTypeId.INT_TYPE))
        .page(strings("s", "s"), strings("t", "t"), strings("price", "id"),
            strings("DECIMAL(10,2)", "BIGINT"), ints(1, 1), ints(2, 1));
    FlightInfo info = sql_.getTables(null, "s", "%", Collections.singletonList("TABLE"),
        true, ALICE);
    List<Map<String, Object>> rows = rows(info);
    assertEquals(2, rows.size());
    assertEquals("t", rows.get(0).get("table_name"));
    assertEquals("TABLE", rows.get(0).get("table_type"));
    Schema t = MessageSerializer.deserializeSchema(new ReadChannel(Channels.newChannel(
        new ByteArrayInputStream((byte[]) rows.get(0).get("table_schema")))));
    assertEquals(Arrays.asList("id", "price"), Arrays.asList(
        t.getFields().get(0).getName(), t.getFields().get(1).getName()));
    assertEquals(new ArrowType.Int(64, true), t.getFields().get(0).getType());
    assertEquals(new ArrowType.Decimal(10, 2, 128), t.getFields().get(1).getType());
    TGetTablesReq req = (TGetTablesReq) hs2_.metadataRequests.get("GetTables").get(0);
    assertEquals("s", req.getSchemaName());
    assertEquals("%", req.getTableName());
    assertEquals(Collections.singletonList("TABLE"), req.getTableTypes());
    // without schema: no GetColumns
    rows(sql_.getTables(null, "s", null, null, false, ALICE));
    assertEquals(1, hs2_.metadataRequests.get("GetColumns").size());
    assertFalse(((TGetTablesReq) hs2_.metadataRequests.get("GetTables").get(1))
        .isSetTableTypes());
  }

  @Test
  public void testTableTypes() throws Exception {
    hs2_.onMetadata("GetTableTypes").schema(column("TABLE_TYPE", TTypeId.STRING_TYPE))
        .page(strings("TABLE", "VIEW"));
    List<Map<String, Object>> rows = rows(sql_.getTableTypes(ALICE));
    assertEquals("VIEW", rows.get(1).get("table_type"));
  }

  @Test
  public void testKeys() throws Exception {
    hs2_.onMetadata("GetPrimaryKeys")
        .schema(column("TABLE_CAT", TTypeId.STRING_TYPE),
            column("TABLE_SCHEM", TTypeId.STRING_TYPE),
            column("TABLE_NAME", TTypeId.STRING_TYPE),
            column("COLUMN_NAME", TTypeId.STRING_TYPE),
            column("KEQ_SEQ", TTypeId.INT_TYPE), column("PK_NAME", TTypeId.STRING_TYPE))
        .page(strings(""), strings("s"), strings("parent"), strings("id"), ints(1),
            strings("pk"));
    hs2_.onMetadata("GetCrossReference")
        .schema(column("PKTABLE_SCHEM", TTypeId.STRING_TYPE),
            column("PKTABLE_NAME", TTypeId.STRING_TYPE),
            column("PKCOLUMN_NAME", TTypeId.STRING_TYPE),
            column("FKTABLE_SCHEM", TTypeId.STRING_TYPE),
            column("FKTABLE_NAME", TTypeId.STRING_TYPE),
            column("FKCOLUMN_NAME", TTypeId.STRING_TYPE),
            column("KEQ_SEQ", TTypeId.INT_TYPE), column("UPDATE_RULE", TTypeId.INT_TYPE))
        .page(strings("s"), strings("parent"), strings("id"), strings("s"),
            strings("child"), strings("parent_id"), ints(1), ints((Integer) null));
    List<Map<String, Object>> pk =
        rows(sql_.getPrimaryKeys(TableRef.of(null, "s", "parent"), ALICE));
    assertEquals("1", pk.get(0).get("key_sequence"));
    TGetPrimaryKeysReq pkReq =
        (TGetPrimaryKeysReq) hs2_.metadataRequests.get("GetPrimaryKeys").get(0);
    assertEquals("s", pkReq.getSchemaName());
    assertEquals("parent", pkReq.getTableName());

    List<Map<String, Object>> imported =
        rows(sql_.getImportedKeys(TableRef.of(null, "s", "child"), ALICE));
    assertEquals("parent_id", imported.get(0).get("fk_column_name"));
    assertEquals("3", imported.get(0).get("update_rule"));
    rows(sql_.getExportedKeys(TableRef.of(null, "s", "parent"), ALICE));
    rows(sql_.getCrossReference(TableRef.of(null, "s", "parent"),
        TableRef.of(null, "s", "child"), ALICE));
    List<Object> reqs = hs2_.metadataRequests.get("GetCrossReference");
    TGetCrossReferenceReq importedReq = (TGetCrossReferenceReq) reqs.get(0);
    assertFalse(importedReq.isSetParentTableName());
    assertEquals("child", importedReq.getForeignTableName());
    TGetCrossReferenceReq exportedReq = (TGetCrossReferenceReq) reqs.get(1);
    assertEquals("parent", exportedReq.getParentTableName());
    assertFalse(exportedReq.isSetForeignTableName());
    TGetCrossReferenceReq bothReq = (TGetCrossReferenceReq) reqs.get(2);
    assertEquals("parent", bothReq.getParentTableName());
    assertEquals("child", bothReq.getForeignTableName());
  }

  @Test
  public void testXdbcTypeInfo() throws Exception {
    hs2_.onMetadata("GetTypeInfo")
        .schema(column("TYPE_NAME", TTypeId.STRING_TYPE),
            column("DATA_TYPE", TTypeId.INT_TYPE), column("PRECISION", TTypeId.INT_TYPE),
            column("CREATE_PARAMS", TTypeId.STRING_TYPE),
            column("CASE_SENSITIVE", TTypeId.BOOLEAN_TYPE))
        .page(strings("INT", "DECIMAL"), ints(4, 3), ints(10, 38),
            strings(null, "precision,scale"), bools(false, false));
    List<Map<String, Object>> all = rows(sql_.getXdbcTypeInfo(ALICE));
    assertEquals(2, all.size());
    assertEquals("[\"precision\",\"scale\"]", all.get(1).get("create_params"));
    assertNull(all.get(0).get("create_params"));
    List<Map<String, Object>> decimal = rows(sql_.getXdbcTypeInfo(3, ALICE));
    assertEquals(1, decimal.size());
    assertEquals("DECIMAL", decimal.get(0).get("type_name"));
  }

  @Test
  public void testErrorsAreReported() throws Exception {
    hs2_.onMetadata("GetTables").executeError =
        "AuthorizationException: User 'alice' does not have privileges";
    try {
      rows(sql_.getTables(null, "s", null, null, false, ALICE));
      fail("expected an error");
    } catch (FlightRuntimeException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("does not have privileges"));
    }
    // the session went back to the pool
    rows(sql_.getTableTypes(ALICE));
    assertEquals(1, hs2_.opened.size());
  }
}
