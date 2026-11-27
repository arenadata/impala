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
import org.apache.impala.thrift.TBackendGflags;
import org.junit.Test;

public class ImpalaFlightSqlServerTest {

  private static FlightSqlConfig plaintextOnLoopback() {
    FlightSqlConfig conf = new FlightSqlConfig();
    conf.bindHost = "127.0.0.1";
    conf.port = 0;
    conf.authMethods = "none";
    // no HS2 behind these tests; avoid reading the system CA bundle
    conf.loopbackSsl = false;
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
    return ImpalaFlightSqlServer.start(plaintextOnLoopback());
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
    ImpalaFlightSqlServer.startIfConfigured(new FlightSqlConfig());
    assertNull(ImpalaFlightSqlServer.running());
  }

  @Test
  public void testTlsRequiresCertificateAndKey() throws Exception {
    FlightSqlConfig conf = plaintextOnLoopback();
    conf.sslPrivateKey = "/nonexistent/server.key";
    assertStartRejected(conf, "--ssl_server_certificate must be set");
    conf.sslCertificate = "/nonexistent/server.pem";
    assertStartRejected(conf, "--ssl_server_certificate: cannot read");
  }

  /** The endpoint does not start without authentication settings. */
  @Test
  public void testAuthenticationIsRequired() throws Exception {
    FlightSqlConfig conf = plaintextOnLoopback();
    conf.authMethods = "";
    assertStartRejected(conf, "no authentication method");
    conf.ldapAllowCleartext = true;
    conf.authMethods = "ldap";
    assertStartRejected(conf, "--ldap_uri");
    conf.authMethods = "kerberos";
    assertStartRejected(conf, "--principal");
    conf.authMethods = "kerberos,none";
    assertStartRejected(conf, "none cannot be combined");
    conf.authMethods = "password";
    assertStartRejected(conf, "unknown method 'password'");
  }

  /** LDAP passwords in clear text need an explicit opt-in. */
  @Test
  public void testLdapRequiresTlsOrOptIn() throws Exception {
    FlightSqlConfig conf = plaintextOnLoopback();
    conf.authMethods = "ldap";
    conf.ldapUri = "ldap://localhost:1";
    conf.ldapBindPattern = "uid=#UID,dc=example,dc=com";
    // plaintext Flight endpoint
    assertStartRejected(conf, "--ldap_passwords_in_clear_ok");
    conf.ldapTls = true;
    assertStartRejected(conf, "reach Flight SQL in clear text");
    conf.ldapAllowCleartext = true;
    ImpalaFlightSqlServer.start(conf).close();
    conf.ldapTls = false;
    ImpalaFlightSqlServer.start(conf).close();
    conf.ldapBindPattern = "uid=someone,dc=example,dc=com";
    assertStartRejected(conf, LdapAuthenticator.UID);
  }

  /** impalad's flags: the three Flight flags plus the HS2 TLS, Kerberos, LDAP ones. */
  @Test
  public void testConfigFromBackendFlags() {
    TBackendGflags cfg = new TBackendGflags();
    cfg.hostname = "coord.example.com";
    cfg.flight_sql_port = 21060;
    cfg.flight_sql_auth_methods = "";
    cfg.flight_sql_memory_limit_bytes = 1L << 30;
    cfg.principal = "impala/_HOST@EXAMPLE.COM";
    cfg.keytab_file = "/etc/impala.keytab";
    cfg.hs2_port = 21050;
    cfg.ssl_server_certificate = "/etc/host.pem";
    cfg.ssl_private_key = "/etc/host.key";
    cfg.ssl_client_ca_certificate = "";
    cfg.enable_ldap_auth = false;
    cfg.ldap_uri = "ldap://ipa.example.com";
    cfg.ldap_bind_pattern = "uid=#UID,dc=example,dc=com";
    cfg.ldap_tls = true;
    cfg.ldap_ca_certificate = "/etc/ipa/ca.crt";
    cfg.ldap_passwords_in_clear_ok = false;
    FlightSqlConfig c = FlightSqlConfig.fromBackend(cfg);
    assertEquals(21060, c.port);
    assertEquals(1L << 30, c.memoryLimitBytes);
    // as HS2: Kerberos only, LDAP is off for HS2
    assertEquals("kerberos", c.authMethods);
    assertTrue(c.tls());
    assertEquals("/etc/host.pem", c.sslCertificate);
    assertEquals("HTTP/_HOST@EXAMPLE.COM", c.spnegoPrincipal);
    assertEquals("/etc/impala.keytab", c.spnegoKeytab);
    assertEquals("coord.example.com", c.hostname);
    assertEquals("ldap://ipa.example.com", c.ldapUri);
    assertTrue(c.ldapTls);
    assertEquals("/etc/ipa/ca.crt", c.ldapCaCertificate);
    assertEquals("coord.example.com", c.loopbackHost);
    assertEquals(21050, c.loopbackPort);
    assertTrue(c.loopbackSsl);
    assertEquals(FlightSqlConfig.SYSTEM_CA_CERTIFICATES, c.loopbackCaCertificate);
    assertEquals("impala/_HOST@EXAMPLE.COM", c.loopbackPrincipal);
    assertEquals("/etc/impala.keytab", c.loopbackKeytab);
    assertNull(c.loopbackDoAs);

    cfg.flight_sql_auth_methods = " kerberos,ldap ";
    cfg.ssl_client_ca_certificate = "/etc/ca.pem";
    c = FlightSqlConfig.fromBackend(cfg);
    assertEquals("kerberos,ldap", c.authMethods);
    assertEquals("/etc/ca.pem", c.loopbackCaCertificate);

    // no TLS, no Kerberos: plaintext loopback, nothing to log in with by default
    cfg.flight_sql_auth_methods = "";
    cfg.principal = "";
    cfg.ssl_server_certificate = "";
    cfg.ssl_private_key = "";
    c = FlightSqlConfig.fromBackend(cfg);
    assertEquals("", c.authMethods);
    assertEquals("", c.spnegoPrincipal);
    assertTrue(!c.tls() && !c.loopbackSsl);
    cfg.enable_ldap_auth = true;
    assertEquals("ldap", FlightSqlConfig.fromBackend(cfg).authMethods);
  }

  @Test
  public void testDefaultAuthMethodsFollowHs2() {
    assertEquals("", FlightSqlConfig.authMethods("", "", false));
    assertEquals("kerberos", FlightSqlConfig.authMethods("", "impala/h@R", false));
    assertEquals("ldap", FlightSqlConfig.authMethods("  ", "", true));
    assertEquals("kerberos,ldap", FlightSqlConfig.authMethods("", "impala/h@R", true));
    assertEquals("ldap", FlightSqlConfig.authMethods("ldap", "impala/h@R", false));
  }

  private static void assertStartRejected(FlightSqlConfig conf, String messagePart)
      throws Exception {
    try {
      ImpalaFlightSqlServer.start(conf).close();
      fail("start should be rejected: " + messagePart);
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains(messagePart));
    }
  }
}
