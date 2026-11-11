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
import static org.apache.impala.service.flight.FakeHs2.ints;
import static org.apache.impala.service.flight.FakeHs2.strings;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.hadoop.conf.Configuration;
import org.apache.hive.service.rpc.thrift.TTypeId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Prepared statements through the Flight SQL endpoint, against a fake HS2. */
public class ImpalaFlightPreparedTest {
  private static final CallOption ALICE = user("alice");
  private static final CallOption BOB = user("bob");
  private static final String QUERY = "select id, name from t";

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
    hs2_.on(QUERY)
        .schema(column("id", TTypeId.INT_TYPE), column("name", TTypeId.STRING_TYPE))
        .page(ints(1, 2), strings("a", "b"));
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

  private List<String> rows(FlightInfo info) throws Exception {
    List<String> out = new ArrayList<>();
    try (FlightStream stream =
             sql_.getStream(info.getEndpoints().get(0).getTicket(), ALICE)) {
      while (stream.next()) {
        for (int r = 0; r < stream.getRoot().getRowCount(); r++) {
          out.add(stream.getRoot().getVector(0).getObject(r) + ":"
              + stream.getRoot().getVector(1).getObject(r));
        }
      }
    }
    return out;
  }

  private static void assertCode(FlightStatusCode code, String messagePart,
      Runnable call) {
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
  public void testQueryRunsOnceForPrepareAndFirstExecution() throws Exception {
    FlightSqlClient.PreparedStatement ps = sql_.prepare(QUERY, ALICE);
    assertEquals(Arrays.asList("id", "name"), Arrays.asList(
        ps.getResultSetSchema().getFields().get(0).getName(),
        ps.getResultSetSchema().getFields().get(1).getName()));
    assertEquals(0, ps.getParameterSchema().getFields().size());
    assertEquals(Collections.singletonList(QUERY), hs2_.statements);
    // first execution: the result started at prepare time
    assertEquals(Arrays.asList("1:a", "2:b"), rows(ps.execute(ALICE)));
    assertEquals(1, hs2_.statements.size());
    // second execution runs again
    assertEquals(Arrays.asList("1:a", "2:b"), rows(ps.execute(ALICE)));
    assertEquals(2, hs2_.statements.size());
    assertEquals(2, ps.fetchSchema(ALICE).getSchema().getFields().size());
    ps.close(ALICE);
    // one session for everything, back in the pool
    assertEquals(1, hs2_.opened.size());
  }

  @Test
  public void testUpdatesRunOnlyWhenExecuted() {
    hs2_.on("insert into t values (1), (2)").dmlRows = Collections.singletonMap("", 2L);
    hs2_.on("create table t (a int)");
    FlightSqlClient.PreparedStatement insert =
        sql_.prepare("insert into t values (1), (2)", ALICE);
    FlightSqlClient.PreparedStatement ddl = sql_.prepare("create table t (a int)", ALICE);
    assertEquals(0, insert.getResultSetSchema().getFields().size());
    assertTrue("nothing runs at prepare", hs2_.statements.isEmpty());
    assertEquals(2, insert.executeUpdate(ALICE));
    assertEquals(0, ddl.executeUpdate(ALICE));
    assertEquals(2, hs2_.statements.size());
    insert.close(ALICE);
    ddl.close(ALICE);
  }

  /**
   * The Arrow Flight SQL JDBC driver runs an update with DoPut and then also asks for its
   * result with GetFlightInfo: the statement must run only once.
   */
  @Test
  public void testUpdateIsNotRunAgainForItsResult() throws Exception {
    hs2_.on("create table t (a int)");
    FlightSqlClient.PreparedStatement ps = sql_.prepare("create table t (a int)", ALICE);
    assertEquals(0, ps.executeUpdate(ALICE));
    FlightInfo info = ps.execute(ALICE);
    assertEquals(0, info.getSchema().getFields().size());
    assertEquals(Collections.emptyList(), rows(info));
    assertEquals(Collections.singletonList("create table t (a int)"), hs2_.statements);
    // a plain execution later runs it again
    ps.execute(ALICE);
    assertEquals(2, hs2_.statements.size());
    ps.close(ALICE);
  }

  @Test
  public void testQueryThroughTheUpdatePath() {
    FlightSqlClient.PreparedStatement ps = sql_.prepare(QUERY, ALICE);
    // the prepare-time result is discarded, the statement runs as an update
    assertEquals(0, ps.executeUpdate(ALICE));
    assertEquals(1, hs2_.cancelledOperations.get());
    assertEquals(2, hs2_.statements.size());
    ps.close(ALICE);
  }

  @Test
  public void testParametersAreRejected() {
    FlightSqlClient.PreparedStatement ps = sql_.prepare(QUERY, ALICE);
    try (IntVector p = new IntVector("p", allocator_)) {
      p.allocateNew(1);
      p.set(0, 1);
      p.setValueCount(1);
      try (VectorSchemaRoot params = VectorSchemaRoot.of(p)) {
        params.setRowCount(1);
        ps.setParameters(params);
        assertCode(FlightStatusCode.INVALID_ARGUMENT, "parameters are not supported",
            () -> ps.execute(ALICE));
        ps.clearParameters();
      }
      ps.close(ALICE);
    }
  }

  @Test
  public void testHandleBelongsToItsOwner() {
    FlightSqlClient.PreparedStatement ps = sql_.prepare(QUERY, ALICE);
    assertCode(FlightStatusCode.NOT_FOUND, "Unknown or expired",
        () -> ps.execute(BOB));
    assertCode(FlightStatusCode.NOT_FOUND, null, () -> ps.executeUpdate(BOB));
    ps.close(ALICE);
  }

  @Test
  public void testCloseDiscardsTheUnusedResult() {
    FlightSqlClient.PreparedStatement ps = sql_.prepare(QUERY, ALICE);
    ps.close(ALICE);
    assertEquals(1, hs2_.cancelledOperations.get());
    assertEquals(1, hs2_.closedOperations.get());
    // the session went back: the next statement does not open another one
    sql_.executeUpdate("drop table t", ALICE);
    assertEquals(1, hs2_.opened.size());
  }

  @Test
  public void testPrepareErrorsAreMapped() {
    hs2_.on("select * from missing").executeError =
        "AnalysisException: Could not resolve table reference: 'missing'";
    assertCode(FlightStatusCode.INVALID_ARGUMENT, "Could not resolve table reference",
        () -> sql_.prepare("select * from missing", ALICE));
  }

  @Test
  public void testRegistryExpiry() {
    AtomicInteger discarded = new AtomicInteger();
    StartedStatement started = new StartedStatement(null, null, null);
    try (PreparedRegistry registry =
             new PreparedRegistry(s -> discarded.incrementAndGet(), 1000, 10000)) {
      String id = registry.register("alice", "select 1", true, null, started);
      assertNull(registry.get(id, "bob"));
      long now = System.currentTimeMillis();
      assertEquals(0, registry.expire(now));
      // cached result too old: discarded, statement kept
      assertEquals(1, registry.expire(now + 2000));
      assertEquals(1, discarded.get());
      assertNull(registry.get(id, "alice").takePending().cached);
      // idle handle dropped
      assertEquals(1, registry.expire(System.currentTimeMillis() + 20000));
      assertEquals(0, registry.size());
    }
  }

  @Test
  public void testNextExecutionRule() {
    long now = 1_000_000;
    long window = PreparedRegistry.UPDATE_RESULT_WINDOW_MS;
    StartedStatement started = new StartedStatement(null, null, null);
    assertEquals(PreparedRegistry.Execution.USE_CACHED, PreparedRegistry.nextExecution(
        PreparedRegistry.Pending.cached(started, now), true, now));
    assertEquals(PreparedRegistry.Execution.RUN,
        PreparedRegistry.nextExecution(PreparedRegistry.Pending.NONE, false, now));
    // right after an update: its result request does not run it again
    assertEquals(PreparedRegistry.Execution.EMPTY_RESULT, PreparedRegistry.nextExecution(
        PreparedRegistry.Pending.updateRan(now - window), false, now));
    // long after the update, or a query: runs again
    assertEquals(PreparedRegistry.Execution.RUN, PreparedRegistry.nextExecution(
        PreparedRegistry.Pending.updateRan(now - window - 1), false, now));
    assertEquals(PreparedRegistry.Execution.RUN, PreparedRegistry.nextExecution(
        PreparedRegistry.Pending.updateRan(now), true, now));
  }
}
