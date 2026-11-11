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

import java.io.File;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;

import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.ClientBearerHeaderHandler;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Kerberos (SPNEGO) authentication of the Flight SQL endpoint, with a Kerby KDC. */
public class SpnegoValidatorTest {
  private static final String REALM = "EXAMPLE.COM";
  private static final String HOST = "localhost";
  private static final String SERVICE = "HTTP/" + HOST + "@" + REALM;
  private static final String CLIENT = "alice@" + REALM;

  @ClassRule
  public static TemporaryFolder folder = new TemporaryFolder();

  private static SimpleKdcServer kdc_;
  private static File serviceKeytab_;
  private static File clientKeytab_;
  private static String previousKrb5Conf_;

  @BeforeClass
  public static void startKdc() throws Exception {
    kdc_ = new SimpleKdcServer();
    kdc_.setKdcRealm(REALM);
    kdc_.setAllowUdp(false);
    kdc_.setWorkDir(folder.getRoot());
    kdc_.init();
    serviceKeytab_ = new File(folder.getRoot(), "service.keytab");
    clientKeytab_ = new File(folder.getRoot(), "alice.keytab");
    kdc_.createPrincipal(SERVICE, "service-password");
    kdc_.exportPrincipal(SERVICE, serviceKeytab_);
    kdc_.createPrincipal(CLIENT, "alice-password");
    kdc_.exportPrincipal(CLIENT, clientKeytab_);
    kdc_.start();
    previousKrb5Conf_ = System.setProperty("java.security.krb5.conf",
        new File(folder.getRoot(), "krb5.conf").getCanonicalPath());
  }

  @AfterClass
  public static void stopKdc() throws Exception {
    if (previousKrb5Conf_ == null) {
      System.clearProperty("java.security.krb5.conf");
    } else {
      System.setProperty("java.security.krb5.conf", previousKrb5Conf_);
    }
    kdc_.stop();
  }

  private static Configuration kerberosConf() {
    Configuration conf = new Configuration(false);
    conf.set(ImpalaFlightSqlServer.BIND_HOST, "127.0.0.1");
    conf.setInt(ImpalaFlightSqlServer.PORT, 0);
    conf.setBoolean(ImpalaFlightSqlServer.SSL_ENABLED, false);
    conf.set(ImpalaFlightAuthenticator.METHODS, "kerberos");
    // no HS2 behind these tests; avoid reading the system CA bundle
    conf.setBoolean(LoopbackConfig.SSL, false);
    conf.set(SpnegoValidator.PRINCIPAL, "HTTP/_HOST@" + REALM);
    conf.set(SpnegoValidator.KEYTAB, serviceKeytab_.getPath());
    return conf;
  }

  /** A SPNEGO token from 'alice' (logged in from her keytab) for the service. */
  private static String clientToken() throws Exception {
    javax.security.auth.login.Configuration jaas =
        new javax.security.auth.login.Configuration() {
          @Override
          public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            Map<String, String> options = new HashMap<>();
            options.put("useKeyTab", "true");
            options.put("keyTab", clientKeytab_.getPath());
            options.put("principal", CLIENT);
            options.put("storeKey", "false");
            options.put("doNotPrompt", "true");
            options.put("refreshKrb5Config", "true");
            return new AppConfigurationEntry[] {new AppConfigurationEntry(
                "com.sun.security.auth.module.Krb5LoginModule",
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options)};
          }
        };
    LoginContext login = new LoginContext("client", new Subject(), null, jaas);
    login.login();
    try {
      return Subject.doAs(login.getSubject(), (PrivilegedExceptionAction<String>) () -> {
        GSSManager gss = GSSManager.getInstance();
        GSSName service = gss.createName(SERVICE, new Oid("1.2.840.113554.1.2.2.1"));
        GSSContext ctx = gss.createContext(service, new Oid("1.3.6.1.5.5.2"), null,
            GSSContext.DEFAULT_LIFETIME);
        try {
          byte[] token = ctx.initSecContext(new byte[0], 0, 0);
          return Base64.getEncoder().encodeToString(token);
        } finally {
          ctx.dispose();
        }
      });
    } finally {
      login.logout();
    }
  }

  private static HeaderCallOption negotiate(String token) {
    FlightCallHeaders headers = new FlightCallHeaders();
    headers.insert("authorization", "Negotiate " + token);
    return new HeaderCallOption(headers);
  }

  @Test
  public void testValidateReturnsClientPrincipal() throws Exception {
    SpnegoValidator validator = new SpnegoValidator(kerberosConf(), HOST);
    assertEquals(SERVICE, validator.getPrincipal());
    assertEquals(CLIENT, validator.validate(clientToken()));
  }

  @Test
  public void testInvalidTokensAreRejected() throws Exception {
    SpnegoValidator validator = new SpnegoValidator(kerberosConf(), HOST);
    for (String token : new String[] {"%%%", "YWJjZGVm"}) {
      try {
        validator.validate(token);
        fail("token should be rejected: " + token);
      } catch (FlightLoginException e) {
        // expected
      }
    }
  }

  @Test
  public void testWrongServicePrincipalIsRejected() throws Exception {
    Configuration conf = kerberosConf();
    conf.set(SpnegoValidator.PRINCIPAL, "HTTP/otherhost@" + REALM);
    try {
      new SpnegoValidator(conf, HOST);
      fail("keytab has no key for HTTP/otherhost");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("no key for"));
    }
  }

  @Test
  public void testNegotiateLoginThroughEndpoint() throws Exception {
    ClientIncomingAuthHeaderMiddleware.Factory bearer =
        new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler());
    try (ImpalaFlightSqlServer server = ImpalaFlightSqlServer.start(kerberosConf(), HOST);
         BufferAllocator allocator = new RootAllocator();
         FlightSqlClient sql = new FlightSqlClient(FlightClient.builder(allocator,
             Location.forGrpcInsecure("127.0.0.1", server.getPort()))
             .intercept(bearer).build())) {
      // A SPNEGO token is accepted once (replay cache), so log in with one call and
      // continue with the bearer token from the response, as clients do.
      FlightInfo info = sql.getSqlInfo(new int[0], negotiate(clientToken()));
      assertEquals(1, info.getEndpoints().size());
      CredentialCallOption token = bearer.getCredentialCallOption();
      assertTrue("no bearer token returned", token != null);
      assertEquals("Impala", serverName(sql, token));
      try {
        serverName(sql);
        fail("expected UNAUTHENTICATED without a header");
      } catch (FlightRuntimeException e) {
        assertEquals(FlightStatusCode.UNAUTHENTICATED, e.status().code());
        assertTrue(e.getMessage(), e.getMessage().contains("Negotiate"));
      }
    }
  }
}
