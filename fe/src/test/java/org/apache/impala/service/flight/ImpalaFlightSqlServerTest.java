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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlInfo;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class ImpalaFlightSqlServerTest {

  private static Configuration plaintextOnLoopback() {
    Configuration conf = new Configuration(false);
    conf.set(ImpalaFlightSqlServer.BIND_HOST, "127.0.0.1");
    conf.setInt(ImpalaFlightSqlServer.PORT, 0);
    conf.setBoolean(ImpalaFlightSqlServer.SSL_ENABLED, false);
    conf.set(ImpalaFlightAuthenticator.METHODS, "none");
    // no HS2 behind these tests; avoid reading the system CA bundle
    conf.setBoolean(LoopbackConfig.SSL, false);
    return conf;
  }

  static FlightSqlClient clientFor(
      BufferAllocator allocator, ImpalaFlightSqlServer server) {
    return new FlightSqlClient(FlightClient.builder(
        allocator, Location.forGrpcInsecure("127.0.0.1", server.getPort())).build());
  }

  /** FLIGHT_SQL_SERVER_NAME from GetSqlInfo, with the given call options. */
  static String serverName(FlightSqlClient client, CallOption... options)
      throws Exception {
    FlightInfo info = client.getSqlInfo(
        new int[] {SqlInfo.FLIGHT_SQL_SERVER_NAME_VALUE}, options);
    try (FlightStream stream =
             client.getStream(info.getEndpoints().get(0).getTicket(), options)) {
      String name = null;
      while (stream.next()) {
        DenseUnionVector vals = (DenseUnionVector) stream.getRoot().getVector("value");
        if (stream.getRoot().getRowCount() > 0) name = String.valueOf(vals.getObject(0));
      }
      return name;
    }
  }

  private static ImpalaFlightSqlServer startPlaintext() throws Exception {
    return ImpalaFlightSqlServer.start(plaintextOnLoopback(), "localhost");
  }

  @Test
  public void testGetSqlInfo() throws Exception {
    try (ImpalaFlightSqlServer server = startPlaintext();
         BufferAllocator allocator = new RootAllocator();
         FlightSqlClient client = clientFor(allocator, server)) {
      FlightInfo info = client.getSqlInfo(SqlInfo.FLIGHT_SQL_SERVER_NAME,
          SqlInfo.FLIGHT_SQL_SERVER_VERSION, SqlInfo.FLIGHT_SQL_SERVER_ARROW_VERSION);
      assertEquals(Schemas.GET_SQL_INFO_SCHEMA, info.getSchema());
      Map<Integer, String> values = new HashMap<>();
      try (FlightStream stream =
               client.getStream(info.getEndpoints().get(0).getTicket())) {
        while (stream.next()) {
          VectorSchemaRoot root = stream.getRoot();
          UInt4Vector names = (UInt4Vector) root.getVector("info_name");
          DenseUnionVector vals = (DenseUnionVector) root.getVector("value");
          for (int i = 0; i < root.getRowCount(); i++) {
            values.put(names.get(i), String.valueOf(vals.getObject(i)));
          }
        }
      }
      assertEquals("Impala", values.get(SqlInfo.FLIGHT_SQL_SERVER_NAME_VALUE));
      assertEquals(ImpalaFlightSqlServer.impalaVersion(),
          values.get(SqlInfo.FLIGHT_SQL_SERVER_VERSION_VALUE));
      assertEquals(ImpalaFlightSqlProducer.ARROW_VERSION,
          values.get(SqlInfo.FLIGHT_SQL_SERVER_ARROW_VERSION_VALUE));
    }
  }

  @Test
  public void testOtherCommandsAreUnimplemented() throws Exception {
    try (ImpalaFlightSqlServer server = startPlaintext();
         BufferAllocator allocator = new RootAllocator();
         FlightSqlClient client = clientFor(allocator, server)) {
      try {
        // Impala has no transactions
        client.beginTransaction();
        fail("transactions should not be implemented");
      } catch (FlightRuntimeException e) {
        assertEquals(FlightStatusCode.UNIMPLEMENTED, e.status().code());
      }
    }
  }

  @Test
  public void testDisabledWithoutPort() {
    ImpalaFlightSqlServer.startIfConfigured(new Configuration(false), "localhost", "");
    assertNull(ImpalaFlightSqlServer.running());
  }

  @Test
  public void testTlsRequiresCertificateAndKey() throws Exception {
    Configuration conf = plaintextOnLoopback();
    conf.setBoolean(ImpalaFlightSqlServer.SSL_ENABLED, true);
    try {
      ImpalaFlightSqlServer.start(conf, "localhost").close();
      fail("TLS without a certificate should be rejected");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(),
          e.getMessage().contains(ImpalaFlightSqlServer.SSL_CERTIFICATE));
    }
  }

  /** The endpoint does not start without authentication settings. */
  @Test
  public void testAuthenticationIsRequired() throws Exception {
    assertStartRejected(without(plaintextOnLoopback(), ImpalaFlightAuthenticator.METHODS),
        ImpalaFlightAuthenticator.METHODS);
    Configuration conf = plaintextOnLoopback();
    conf.set(ImpalaFlightAuthenticator.METHODS, "ldap");
    assertStartRejected(conf, LdapAuthenticator.URI);
    conf.set(ImpalaFlightAuthenticator.METHODS, "kerberos");
    assertStartRejected(conf, SpnegoValidator.PRINCIPAL);
    conf.set(ImpalaFlightAuthenticator.METHODS, "kerberos,none");
    assertStartRejected(conf, "none cannot be combined");
    conf.set(ImpalaFlightAuthenticator.METHODS, "password");
    assertStartRejected(conf, "unknown method 'password'");
  }

  /** LDAP passwords in clear text need an explicit opt-in. */
  @Test
  public void testLdapRequiresTlsOrOptIn() throws Exception {
    Configuration conf = plaintextOnLoopback();
    conf.set(ImpalaFlightAuthenticator.METHODS, "ldap");
    conf.set(LdapAuthenticator.URI, "ldap://localhost:1");
    conf.set(LdapAuthenticator.BIND_PATTERN, "uid=#UID,dc=example,dc=com");
    assertStartRejected(conf, LdapAuthenticator.ALLOW_CLEARTEXT);
    conf.setBoolean(LdapAuthenticator.STARTTLS, true);
    ImpalaFlightSqlServer.start(conf, "localhost").close();
    conf.setBoolean(LdapAuthenticator.STARTTLS, false);
    conf.setBoolean(LdapAuthenticator.ALLOW_CLEARTEXT, true);
    ImpalaFlightSqlServer.start(conf, "localhost").close();
    conf.set(LdapAuthenticator.BIND_PATTERN, "uid=someone,dc=example,dc=com");
    assertStartRejected(conf, LdapAuthenticator.UID);
  }

  private static Configuration without(Configuration conf, String key) {
    conf.unset(key);
    return conf;
  }

  private static void assertStartRejected(Configuration conf, String messagePart)
      throws Exception {
    try {
      ImpalaFlightSqlServer.start(conf, "localhost").close();
      fail("start should be rejected: " + messagePart);
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains(messagePart));
    }
  }
}
