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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.service.rpc.thrift.TOpenSessionReq;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** HS2 loopback sessions against a fake HS2 ({@link FakeHs2}, plain socket, no SASL). */
public class LoopbackSessionsTest {

  private FakeHs2 hs2_;

  @Before
  public void setUp() throws Exception { hs2_ = new FakeHs2(); }

  @After
  public void tearDown() { hs2_.close(); }

  private Configuration conf(String impersonation) {
    Configuration conf = new Configuration(false);
    conf.set(LoopbackConfig.HOST, "127.0.0.1");
    conf.setInt(LoopbackConfig.PORT, hs2_.port);
    conf.setBoolean(LoopbackConfig.SSL, false);
    conf.set(LoopbackConfig.IMPERSONATION, impersonation);
    conf.setInt(LoopbackConfig.MAX_SESSIONS_PER_USER, 2);
    return conf;
  }

  private LoopbackSessions sessions(String impersonation) throws Exception {
    return LoopbackSessions.create(
        new LoopbackConfig(conf(impersonation), "localhost", ""));
  }

  @Test
  public void testDoAsUserIsSentAtOpenSession() throws Exception {
    try (LoopbackSessions sessions = sessions("doas")) {
      LoopbackSession s = sessions.borrow("alice");
      s.closeOperation(s.execute("select user(), effective_user()"));
      sessions.release(s);
      TOpenSessionReq req = hs2_.opened.get(0);
      assertEquals("alice", req.getConfiguration().get(LoopbackSession.DOAS_KEY));
      assertFalse(req.isSetUsername());
      assertEquals("select user(), effective_user()", hs2_.statements.get(0));
    }
  }

  @Test
  public void testUsernameModeWithoutKerberos() throws Exception {
    // the default without a principal
    LoopbackConfig config = new LoopbackConfig(
        without(conf("doas"), LoopbackConfig.IMPERSONATION), "localhost", "");
    assertFalse(config.doAs);
    try (LoopbackSessions sessions = LoopbackSessions.create(config)) {
      LoopbackSession s = sessions.borrow("bob");
      sessions.release(s);
      TOpenSessionReq req = hs2_.opened.get(0);
      assertEquals("bob", req.getUsername());
      assertNull(req.getConfiguration().get(LoopbackSession.DOAS_KEY));
    }
  }

  @Test
  public void testSessionsAreReusedPerUser() throws Exception {
    try (LoopbackSessions sessions = sessions("doas")) {
      LoopbackSession a1 = sessions.borrow("alice");
      sessions.release(a1);
      LoopbackSession a2 = sessions.borrow("alice");
      assertTrue("idle session reused", a1 == a2);
      LoopbackSession b = sessions.borrow("bob");
      assertTrue(a2 != b);
      sessions.release(a2);
      sessions.release(b);
      assertEquals(2, hs2_.opened.size());
      assertEquals(1, sessions.openSessions("alice"));
      assertEquals(1, sessions.idleSessions("bob"));
    }
    // close() closes the idle sessions
    assertEquals(2, hs2_.closedSessions.get());
  }

  @Test
  public void testLimitPerUser() throws Exception {
    try (LoopbackSessions sessions = sessions("doas")) {
      LoopbackSession s1 = sessions.borrow("alice");
      LoopbackSession s2 = sessions.borrow("alice");
      try {
        sessions.borrow("alice");
        fail("third concurrent session should be refused");
      } catch (LoopbackSessions.TooManySessionsException e) {
        assertTrue(e.getMessage(), e.getMessage().contains("limit 2"));
      }
      // other users are not affected
      sessions.release(sessions.borrow("bob"));
      sessions.release(s1);
      sessions.release(sessions.borrow("alice"));
      sessions.release(s2);
      assertEquals(2, sessions.openSessions("alice"));
    }
  }

  @Test
  public void testIdleSessionsAreClosed() throws Exception {
    try (LoopbackSessions sessions = sessions("doas")) {
      sessions.release(sessions.borrow("alice"));
      assertEquals(0, sessions.reapIdle(System.currentTimeMillis()));
      assertEquals(1, sessions.reapIdle(System.currentTimeMillis() + 600_000));
      assertEquals(0, sessions.openSessions("alice"));
      assertEquals(1, hs2_.closedSessions.get());
    }
  }

  @Test
  public void testOpenSessionErrorIsReported() throws Exception {
    hs2_.openError = "User 'impala' is not authorized to delegate to 'carol'.";
    try (LoopbackSessions sessions = sessions("doas")) {
      try {
        sessions.borrow("carol");
        fail("OpenSession error should be raised");
      } catch (LoopbackException e) {
        assertTrue(e.getMessage(), e.getMessage().contains("not authorized to delegate"));
        assertFalse(e.isBroken());
      }
      assertEquals(0, sessions.openSessions("carol"));
    }
  }

  @Test
  public void testBrokenSessionIsDiscarded() throws Exception {
    try (LoopbackSessions sessions = sessions("doas")) {
      LoopbackSession s = sessions.borrow("alice");
      try {
        s.execute("break");
        fail("call on a dead connection should fail");
      } catch (LoopbackException e) {
        assertTrue(e.isBroken());
      }
      sessions.release(s);
      assertEquals(0, sessions.openSessions("alice"));
      assertEquals(0, sessions.idleSessions("alice"));
    }
  }

  private static Configuration without(Configuration conf, String key) {
    conf.unset(key);
    return conf;
  }
}
