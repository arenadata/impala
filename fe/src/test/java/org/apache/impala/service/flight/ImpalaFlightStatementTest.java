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

import static org.apache.impala.service.flight.FakeHs2.column;
import static org.apache.impala.service.flight.FakeHs2.strings;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CancelFlightInfoRequest;
import org.apache.arrow.flight.CancelStatus;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hive.service.rpc.thrift.TColumn;
import org.apache.hive.service.rpc.thrift.TI32Column;
import org.apache.hive.service.rpc.thrift.TTypeId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Statements through the Flight SQL endpoint, against a fake HS2. */
public class ImpalaFlightStatementTest {
  private static final CallOption ALICE = user("alice");
  private static final CallOption BOB = user("bob");

  private FakeHs2 hs2_;
  private ImpalaFlightSqlServer server_;
  private BufferAllocator allocator_;
  private FlightSqlClient sql_;

  private static CallOption user(String name) {
    FlightCallHeaders headers = new FlightCallHeaders();
    headers.insert(ImpalaFlightAuthenticator.USER_HEADER, name);
    return new HeaderCallOption(headers);
  }

  @Before
  public void setUp() throws Exception {
    hs2_ = new FakeHs2();
    Configuration conf = new Configuration(false);
    conf.set(ImpalaFlightSqlServer.BIND_HOST, "127.0.0.1");
    conf.setInt(ImpalaFlightSqlServer.PORT, 0);
    conf.setBoolean(ImpalaFlightSqlServer.SSL_ENABLED, false);
    conf.set(ImpalaFlightAuthenticator.METHODS, "none");
    conf.setInt(ImpalaFlightSqlServer.FETCH_ROWS, 2);
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

  private static TColumn ints(Integer... values) {
    return TColumn.i32Val(new TI32Column(Arrays.asList(values),
        java.nio.ByteBuffer.wrap(new byte[1])));
  }

  /** All rows of a statement as "id:name" strings; counts the batches in 'batches'. */
  private List<String> rows(FlightInfo info, CallOption option, int[] batches)
      throws Exception {
    List<String> out = new ArrayList<>();
    try (FlightStream stream =
             sql_.getStream(info.getEndpoints().get(0).getTicket(), option)) {
      while (stream.next()) {
        if (batches != null && stream.getRoot().getRowCount() > 0) batches[0]++;
        for (int r = 0; r < stream.getRoot().getRowCount(); r++) {
          out.add(stream.getRoot().getVector(0).getObject(r) + ":"
              + stream.getRoot().getVector(1).getObject(r));
        }
      }
    }
    return out;
  }

  private void assertCode(FlightStatusCode code, String messagePart, Runnable call) {
    try {
      call.run();
      fail("expected " + code);
    } catch (FlightRuntimeException e) {
      assertEquals(e.getMessage(), code, e.status().code());
      if (messagePart != null) {
        assertTrue(e.getMessage(), e.getMessage().contains(messagePart));
      }
    }
  }

  @Test
  public void testQueryStreamsAllPagesAsBatches() throws Exception {
    hs2_.on("select id, name from t")
        .schema(column("id", TTypeId.INT_TYPE), column("name", TTypeId.STRING_TYPE))
        .page(ints(1, 2), strings("a", "b"))
        .page(ints(3), strings((String) null));
    FlightInfo info = sql_.execute("select id, name from t", ALICE);
    assertEquals(new ArrowType.Int(32, true),
        info.getSchema().getFields().get(0).getType());
    assertEquals("name", info.getSchema().getFields().get(1).getName());
    int[] batches = {0};
    assertEquals(Arrays.asList("1:a", "2:b", "3:null"), rows(info, ALICE, batches));
    assertEquals(2, batches[0]);
    // the statement ran as alice; its operation was closed, the session reused next time
    assertEquals("alice", hs2_.opened.get(0).getConfiguration().get("impala.doas.user"));
    assertEquals(1, hs2_.closedOperations.get());
    rows(sql_.execute("select id, name from t", ALICE), ALICE, null);
    assertEquals(1, hs2_.opened.size());
  }

  @Test
  public void testColumnMetadataInFlightInfoAndStream() throws Exception {
    hs2_.on("select id, name from t")
        .schema(column("id", TTypeId.INT_TYPE), column("name", TTypeId.STRING_TYPE))
        .page(ints(1), strings("a"));
    FlightInfo info = sql_.execute("select id, name from t", ALICE);
    assertEquals("INT", info.getSchema().getFields().get(0).getMetadata()
        .get("ARROW:FLIGHT:SQL:TYPE_NAME"));
    assertEquals(String.valueOf(Integer.MAX_VALUE), info.getSchema().getFields().get(1)
        .getMetadata().get("ARROW:FLIGHT:SQL:PRECISION"));
    try (FlightStream stream =
             sql_.getStream(info.getEndpoints().get(0).getTicket(), ALICE)) {
      while (stream.next()) {
        // drain
      }
      assertEquals(info.getSchema(), stream.getSchema());
    }
  }

  @Test
  public void testTicketIsUsableOnceAndOnlyByItsOwner() throws Exception {
    FlightInfo info = sql_.execute("select user(), effective_user()", ALICE);
    assertCode(FlightStatusCode.NOT_FOUND, "Unknown, expired or already fetched",
        () -> drain(info, BOB));
    assertEquals(Collections.singletonList("impala/host@REALM:alice"),
        rows(info, ALICE, null));
    assertCode(FlightStatusCode.NOT_FOUND, null, () -> drain(info, ALICE));
  }

  private void drain(FlightInfo info, CallOption option) {
    try {
      rows(info, option, null);
    } catch (FlightRuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  @Test
  public void testStatementWithoutResultSet() throws Exception {
    hs2_.on("create table t (a int)");
    FlightInfo info = sql_.execute("create table t (a int)", ALICE);
    assertEquals(0, info.getSchema().getFields().size());
    assertEquals(Collections.emptyList(), rows(info, ALICE, null));
    assertEquals(Collections.singletonList("create table t (a int)"), hs2_.statements);
  }

  @Test
  public void testUpdateCounts() {
    hs2_.on("insert into t values (1), (2)").dmlRows =
        Collections.singletonMap("", 2L);
    hs2_.on("drop table t");
    assertEquals(2, sql_.executeUpdate("insert into t values (1), (2)", ALICE));
    assertEquals(0, sql_.executeUpdate("drop table t", ALICE));
    assertEquals(1, hs2_.opened.size());
  }

  @Test
  public void testErrorsAreMapped() {
    hs2_.on("select * from missing").executeError =
        "AnalysisException: Could not resolve table reference: 'missing'";
    hs2_.on("select * from secret").executeError =
        "AuthorizationException: User 'alice' does not have privileges to access: secret";
    hs2_.on("select boom").schema(column("x", TTypeId.STRING_TYPE)).fetchError =
        "Query 1 failed: something went wrong";
    assertCode(FlightStatusCode.INVALID_ARGUMENT, "Could not resolve table reference",
        () -> sql_.execute("select * from missing", ALICE));
    assertCode(FlightStatusCode.UNAUTHORIZED, "does not have privileges",
        () -> sql_.execute("select * from secret", ALICE));
    FlightInfo info = sql_.execute("select boom", ALICE);
    assertCode(FlightStatusCode.INTERNAL, "something went wrong",
        () -> drain(info, ALICE));
    assertCode(FlightStatusCode.INVALID_ARGUMENT, "Could not resolve table reference",
        () -> sql_.executeUpdate("select * from missing", ALICE));
    // failed statements gave their session back: still one session
    sql_.executeUpdate("drop table t", ALICE);
    assertEquals(1, hs2_.opened.size());
  }

  @Test
  public void testSchemaOnlyRunsQueries() {
    hs2_.on("select id, name from t")
        .schema(column("id", TTypeId.INT_TYPE), column("name", TTypeId.STRING_TYPE));
    assertEquals(2, sql_.getExecuteSchema("select id, name from t", ALICE).getSchema()
        .getFields().size());
    assertEquals(1, hs2_.cancelledOperations.get());
    assertEquals(0, sql_.getExecuteSchema("drop table t", ALICE).getSchema()
        .getFields().size());
    assertEquals(Collections.singletonList("select id, name from t"), hs2_.statements);
  }

  @Test
  public void testCancelBeforeFetch() throws Exception {
    FlightInfo info = sql_.execute("select user(), effective_user()", ALICE);
    assertEquals(CancelStatus.CANCELLED,
        sql_.cancelFlightInfo(new CancelFlightInfoRequest(info), ALICE).getStatus());
    assertEquals(1, hs2_.cancelledOperations.get());
    assertCode(FlightStatusCode.NOT_FOUND, null, () -> drain(info, ALICE));
    assertEquals(CancelStatus.NOT_CANCELLABLE,
        sql_.cancelFlightInfo(new CancelFlightInfoRequest(info), ALICE).getStatus());
  }

  @Test
  public void testUnfetchedStatementsExpire() throws Exception {
    try (LoopbackSessions sessions = LoopbackSessions.create(new LoopbackConfig(
        loopbackConf(), "localhost", ""));
         QueryRegistry registry = new QueryRegistry(sessions, 1000)) {
      LoopbackSession s = sessions.borrow("alice");
      registry.register("alice", s, s.execute("select 1"), null);
      assertEquals(0, registry.expire(System.currentTimeMillis()));
      assertEquals(1, registry.expire(System.currentTimeMillis() + 2000));
      assertEquals(0, registry.size());
      assertEquals(1, hs2_.cancelledOperations.get());
      assertEquals(1, hs2_.closedOperations.get());
      assertEquals(1, sessions.idleSessions("alice"));
    }
  }

  private Configuration loopbackConf() {
    Configuration conf = new Configuration(false);
    conf.set(LoopbackConfig.HOST, "127.0.0.1");
    conf.setInt(LoopbackConfig.PORT, hs2_.port);
    conf.setBoolean(LoopbackConfig.SSL, false);
    conf.set(LoopbackConfig.IMPERSONATION, "doas");
    return conf;
  }
}
