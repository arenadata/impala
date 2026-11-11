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

import static org.apache.impala.service.flight.ImpalaFlightSqlServerTest.serverName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.integ.CreateLdapServerRule;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

/** LDAP (Basic) and bearer token authentication of the Flight SQL endpoint. */
@CreateDS(name = "flightDS",
    partitions = { @CreatePartition(name = "test", suffix = "dc=myorg,dc=com") })
@CreateLdapServer(
    transports = { @CreateTransport(protocol = "LDAP", address = "localhost") })
@ApplyLdifFiles({"users.ldif"})
public class ImpalaFlightLdapAuthTest {
  // users.ldif
  private static final String USER = "Test1Ldap";
  private static final String PASSWORD = "12345";
  private static final String OTHER_PASSWORD = "abcde"; // Test2Ldap's

  @ClassRule
  public static CreateLdapServerRule serverRule = new CreateLdapServerRule();

  private ImpalaFlightSqlServer server_;
  private BufferAllocator allocator_;
  private FlightClient client_;
  private FlightSqlClient sql_;

  static Configuration ldapConf(String uri) {
    Configuration conf = new Configuration(false);
    conf.set(ImpalaFlightSqlServer.BIND_HOST, "127.0.0.1");
    conf.setInt(ImpalaFlightSqlServer.PORT, 0);
    conf.setBoolean(ImpalaFlightSqlServer.SSL_ENABLED, false);
    conf.set(ImpalaFlightAuthenticator.METHODS, "ldap");
    // no HS2 behind these tests; avoid reading the system CA bundle
    conf.setBoolean(LoopbackConfig.SSL, false);
    conf.set(LdapAuthenticator.URI, uri);
    conf.set(LdapAuthenticator.BIND_PATTERN, "cn=#UID,ou=Users,dc=myorg,dc=com");
    // the test directory has no TLS
    conf.setBoolean(LdapAuthenticator.ALLOW_CLEARTEXT, true);
    return conf;
  }

  @Before
  public void setUp() throws Exception {
    server_ = ImpalaFlightSqlServer.start(ldapConf(
        "ldap://localhost:" + serverRule.getLdapServer().getPort()), "localhost");
    allocator_ = new RootAllocator();
    client_ = FlightClient.builder(allocator_,
        Location.forGrpcInsecure("127.0.0.1", server_.getPort())).build();
    sql_ = new FlightSqlClient(client_);
  }

  @After
  public void tearDown() throws Exception {
    sql_.close(); // closes client_
    allocator_.close();
    server_.close();
  }

  private static HeaderCallOption authorization(String value) {
    FlightCallHeaders headers = new FlightCallHeaders();
    headers.insert("authorization", value);
    return new HeaderCallOption(headers);
  }

  private static HeaderCallOption basic(String user, String password) {
    return authorization("Basic " + Base64.getEncoder().encodeToString(
        (user + ":" + password).getBytes(StandardCharsets.UTF_8)));
  }

  private void assertUnauthenticated(String messagePart, CallOption... options) {
    try {
      serverName(sql_, options);
      fail("expected UNAUTHENTICATED");
    } catch (FlightRuntimeException e) {
      assertEquals(FlightStatusCode.UNAUTHENTICATED, e.status().code());
      if (messagePart != null) {
        assertTrue(e.getMessage(), e.getMessage().contains(messagePart));
      }
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  @Test
  public void testBasicLoginIssuesBearerToken() throws Exception {
    Optional<CredentialCallOption> bearer =
        client_.authenticateBasicToken(USER, PASSWORD);
    assertTrue("no bearer token returned", bearer.isPresent());
    // GetFlightInfo + DoGet with the token only
    assertEquals("Impala", serverName(sql_, bearer.get()));
    assertEquals("Impala", serverName(sql_, bearer.get()));
  }

  @Test
  public void testBasicHeaderOnEveryCall() throws Exception {
    assertEquals("Impala", serverName(sql_, basic(USER, PASSWORD)));
  }

  @Test
  public void testRejectedLogins() {
    assertUnauthenticated("Missing Flight SQL authorization header");
    assertUnauthenticated("Invalid username or password", basic(USER, OTHER_PASSWORD));
    assertUnauthenticated("Invalid username or password", basic("NoSuchUser", PASSWORD));
    // an empty password would be an anonymous bind, which LDAP accepts
    assertUnauthenticated("Empty password", basic(USER, ""));
    assertUnauthenticated("Empty user name", basic("", PASSWORD));
    // DN injection: the user name is escaped, so this is not Test1Ldap's DN
    assertUnauthenticated("Invalid username or password",
        basic(USER + ",ou=Users,dc=myorg,dc=com", PASSWORD));
    assertUnauthenticated("Malformed Basic credentials", authorization("Basic %%%"));
    assertUnauthenticated(null, authorization("Bearer not-a-token"));
    assertUnauthenticated("Kerberos authentication is not enabled",
        authorization("Negotiate YWJj"));
  }

  @Test
  public void testDirectoryDown() throws Exception {
    try (ImpalaFlightSqlServer server =
             ImpalaFlightSqlServer.start(ldapConf("ldap://127.0.0.1:1"), "localhost");
         FlightSqlClient sql = ImpalaFlightSqlServerTest.clientFor(allocator_, server)) {
      serverName(sql, basic(USER, PASSWORD));
      fail("expected UNAUTHENTICATED");
    } catch (FlightRuntimeException e) {
      assertEquals(FlightStatusCode.UNAUTHENTICATED, e.status().code());
      assertTrue(e.getMessage(), e.getMessage().contains("LDAP server error"));
    }
  }

  @Test
  public void testBindDnEscaping() throws Exception {
    LdapAuthenticator ldap = new LdapAuthenticator(ldapConf("ldap://localhost:1"));
    assertEquals("cn=a\\,b\\=c\\+d,ou=Users,dc=myorg,dc=com", ldap.bindDn("a,b=c+d"));
    assertEquals("cn=\\#x,ou=Users,dc=myorg,dc=com", ldap.bindDn("#x"));
  }
}
