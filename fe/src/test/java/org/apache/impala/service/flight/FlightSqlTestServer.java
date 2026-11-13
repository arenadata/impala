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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.api.DirectoryService;
import org.apache.directory.server.core.factory.DSAnnotationProcessor;
import org.apache.directory.server.factory.ServerAnnotationProcessor;
import org.apache.directory.server.ldap.LdapServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.runner.Description;

/**
 * Flight SQL endpoint for end-to-end tests without a cluster: the real
 * {@link ImpalaFlightSqlServer} with its HS2 loopback pointed at a {@link FakeHs2} that
 * serves {@link FlightTestData}, plus an in-process LDAP server and Kerberos KDC.
 *
 * <pre>
 * FlightSqlTestServer --info=FILE [--auth=none|ldap|kerberos|all] [--tls=true|false]
 *                     [--work-dir=DIR]
 * </pre>
 *
 * Writes FILE (JSON: port, TLS CA, users and passwords, krb5.conf and keytabs, the
 * scripted SQL) once the server is up, then serves until its standard input closes, so it
 * stops with the process that started it. 'all' enables Kerberos and LDAP together.
 */
public final class FlightSqlTestServer {
  static final String REALM = "EXAMPLE.COM";
  static final String HOST = "localhost";
  /** users.ldif (test resources). */
  static final Map<String, String> LDAP_USERS = new LinkedHashMap<>();
  static {
    LDAP_USERS.put("Test1Ldap", "12345");
    LDAP_USERS.put("Test2Ldap", "abcde");
  }
  static final List<String> KERBEROS_USERS = java.util.Arrays.asList("alice", "bob");
  static final int PREPARED_IDLE_S = 5;
  static final int TICKET_TTL_S = 20;

  /** Carries the ApacheDS annotations (as ImpalaFlightLdapAuthTest). */
  @CreateDS(name = "flightTestServerDS",
      partitions = { @CreatePartition(name = "test", suffix = "dc=myorg,dc=com") })
  @CreateLdapServer(
      transports = { @CreateTransport(protocol = "LDAP", address = "localhost") })
  @ApplyLdifFiles({"users.ldif"})
  static final class Ldap {}

  private final List<AutoCloseable> resources_ = new ArrayList<>();
  private FakeHs2 hs2_;
  private ImpalaFlightSqlServer server_;

  public static void main(String[] args) throws Exception {
    Map<String, String> opts = new LinkedHashMap<>();
    for (String a : args) {
      if (!a.startsWith("--") || !a.contains("=")) usage("bad argument: " + a);
      opts.put(a.substring(2, a.indexOf('=')), a.substring(a.indexOf('=') + 1));
    }
    if (!opts.containsKey("info")) usage("--info is required");
    String auth = opts.getOrDefault("auth", "none");
    if (!java.util.Arrays.asList("none", "ldap", "kerberos", "all").contains(auth)) {
      usage("unknown --auth: " + auth);
    }
    boolean tls = Boolean.parseBoolean(opts.getOrDefault("tls", "true"));
    // the server's log (logins, streams) goes to stdout, i.e. the caller's log file
    org.apache.log4j.BasicConfigurator.configure();
    org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.INFO);
    File workDir = opts.containsKey("work-dir") ? new File(opts.get("work-dir"))
        : Files.createTempDirectory("flight-test-server").toFile();
    workDir.mkdirs();

    FlightSqlTestServer s = new FlightSqlTestServer();
    try {
      Map<String, Object> info = s.start(auth, tls, workDir.getCanonicalFile());
      writeAtomically(new File(opts.get("info")), info);
      System.out.println("FlightSqlTestServer ready on port " + info.get("port"));
      System.out.flush();
      // serve until the parent closes our stdin (or dies)
      InputStream in = System.in;
      while (in.read() != -1) {}
    } finally {
      s.close();
    }
    System.exit(0);
  }

  private static void usage(String message) {
    System.err.println(message + "\nusage: FlightSqlTestServer --info=FILE "
        + "[--auth=none|ldap|kerberos|all] [--tls=true|false] [--work-dir=DIR]");
    System.exit(2);
  }

  Map<String, Object> start(String auth, boolean tls, File workDir) throws Exception {
    Map<String, Object> info = new LinkedHashMap<>();
    FakeHs2 hs2 = new FakeHs2();
    hs2_ = hs2;
    resources_.add(hs2);
    FlightTestData.install(hs2);

    Configuration conf = new Configuration(false);
    conf.set(ImpalaFlightSqlServer.BIND_HOST, "127.0.0.1");
    conf.setInt(ImpalaFlightSqlServer.PORT, 0);
    conf.set(LoopbackConfig.HOST, "127.0.0.1");
    conf.setInt(LoopbackConfig.PORT, hs2.port);
    conf.setBoolean(LoopbackConfig.SSL, false);
    conf.set(LoopbackConfig.IMPERSONATION, "doas");
    // short, so that tests of expiry wait seconds: idle prepared statements are dropped
    // after PREPARED_IDLE_S by a reaper running every TICKET_TTL_S / 4 (here 5 s)
    conf.setInt(ImpalaFlightSqlServer.PREPARED_IDLE_S, PREPARED_IDLE_S);
    conf.setInt(ImpalaFlightSqlServer.TICKET_TTL_S, TICKET_TTL_S);
    info.put("prepared_idle_s", PREPARED_IDLE_S);
    info.put("reaper_period_s", TICKET_TTL_S / 4);

    conf.setBoolean(ImpalaFlightSqlServer.SSL_ENABLED, tls);
    info.put("tls", tls);
    if (tls) {
      File cert = new File(workDir, "server.pem");
      File key = new File(workDir, "server.key");
      selfSignedCertificate(cert, key);
      conf.set(ImpalaFlightSqlServer.SSL_CERTIFICATE, cert.getPath());
      conf.set(ImpalaFlightSqlServer.SSL_PRIVATE_KEY, key.getPath());
      info.put("ca", cert.getPath());
    }

    List<String> methods = new ArrayList<>();
    if (auth.equals("ldap") || auth.equals("all")) {
      methods.add("ldap");
      int port = startLdap();
      conf.set(LdapAuthenticator.URI, "ldap://localhost:" + port);
      conf.set(LdapAuthenticator.BIND_PATTERN, "cn=#UID,ou=Users,dc=myorg,dc=com");
      conf.setBoolean(LdapAuthenticator.ALLOW_CLEARTEXT, true);
      info.put("ldap_users", LDAP_USERS);
    }
    if (auth.equals("kerberos") || auth.equals("all")) {
      methods.add("kerberos");
      info.putAll(startKdc(workDir));
      conf.set(SpnegoValidator.PRINCIPAL, "HTTP/_HOST@" + REALM);
      conf.set(SpnegoValidator.KEYTAB, new File(workDir, "service.keytab").getPath());
    }
    if (methods.isEmpty()) methods.add("none");
    conf.set(ImpalaFlightAuthenticator.METHODS, String.join(",", methods));
    info.put("auth", methods);
    info.put("user_header", ImpalaFlightAuthenticator.USER_HEADER);

    server_ = ImpalaFlightSqlServer.start(conf, HOST);
    info.put("host", HOST);
    info.put("port", server_.getPort());
    info.put("data", FlightTestData.describe());
    return info;
  }

  private int startLdap() throws Exception {
    Description d = Description.createSuiteDescription(Ldap.class);
    DirectoryService ds = DSAnnotationProcessor.getDirectoryService(d);
    DSAnnotationProcessor.applyLdifs(d, ds);
    LdapServer ldap = ServerAnnotationProcessor.createLdapServer(d, ds);
    resources_.add(() -> {
      ldap.stop();
      ds.shutdown();
    });
    return ldap.getPort();
  }

  /** KDC with HTTP/localhost (service) and the client users; returns their files. */
  private Map<String, Object> startKdc(File workDir) throws Exception {
    File kdcDir = new File(workDir, "kdc");
    kdcDir.mkdirs();
    SimpleKdcServer kdc = new SimpleKdcServer();
    kdc.setKdcRealm(REALM);
    kdc.setKdcHost(HOST);
    kdc.setAllowUdp(false);
    kdc.setWorkDir(kdcDir);
    kdc.init();
    String service = "HTTP/" + HOST + "@" + REALM;
    kdc.createPrincipal(service, "service-password");
    kdc.exportPrincipal(service, new File(workDir, "service.keytab"));
    Map<String, String> keytabs = new LinkedHashMap<>();
    for (String user : KERBEROS_USERS) {
      File keytab = new File(workDir, user + ".keytab");
      kdc.createPrincipal(user + "@" + REALM, user + "-password");
      kdc.exportPrincipal(user + "@" + REALM, keytab);
      keytabs.put(user, keytab.getPath());
    }
    kdc.start();
    resources_.add(kdc::stop);
    File krb5Conf = new File(kdcDir, "krb5.conf");
    System.setProperty("java.security.krb5.conf", krb5Conf.getPath());

    Map<String, Object> info = new LinkedHashMap<>();
    info.put("realm", REALM);
    info.put("krb5_conf", krb5Conf.getPath());
    info.put("service_principal", service);
    info.put("keytabs", keytabs);
    return info;
  }

  /** Self-signed certificate for localhost / 127.0.0.1 (PEM) and its PKCS#8 key. */
  static void selfSignedCertificate(File cert, File key) throws Exception {
    KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
    gen.initialize(2048);
    KeyPair pair = gen.generateKeyPair();
    X500Name name = new X500Name("CN=" + HOST);
    long now = System.currentTimeMillis();
    JcaX509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(name,
        BigInteger.valueOf(now), new Date(now - 60_000L),
        new Date(now + 7L * 24 * 3600 * 1000), name, pair.getPublic());
    builder.addExtension(Extension.subjectAlternativeName, false, new GeneralNames(
        new GeneralName[] {new GeneralName(GeneralName.dNSName, HOST),
            new GeneralName(GeneralName.iPAddress, "127.0.0.1")}));
    X509Certificate x509 = new JcaX509CertificateConverter().getCertificate(builder.build(
        new JcaContentSignerBuilder("SHA256withRSA").build(pair.getPrivate())));
    try (JcaPEMWriter w = new JcaPEMWriter(new FileWriter(cert))) {
      w.writeObject(x509);
    }
    try (Writer w = new FileWriter(key)) {
      w.write("-----BEGIN PRIVATE KEY-----\n");
      w.write(java.util.Base64.getMimeEncoder(64, "\n".getBytes())
          .encodeToString(pair.getPrivate().getEncoded()));
      w.write("\n-----END PRIVATE KEY-----\n");
    }
  }

  private static void writeAtomically(File file, Map<String, Object> info)
      throws IOException {
    File tmp = new File(file.getPath() + ".tmp");
    new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).writeValue(tmp, info);
    Files.move(tmp.toPath(), file.toPath(), StandardCopyOption.ATOMIC_MOVE,
        StandardCopyOption.REPLACE_EXISTING);
  }

  void close() {
    try {
      if (server_ != null) server_.close();
    } catch (Exception e) {
      System.err.println("closing the Flight SQL server: " + e);
    }
    for (int i = resources_.size() - 1; i >= 0; i--) {
      try {
        resources_.get(i).close();
      } catch (Exception e) {
        System.err.println("closing " + resources_.get(i) + ": " + e);
      }
    }
  }

  /** The server's port (for in-process use by the JDBC tests). */
  int port() { return server_.getPort(); }

  /** The fake HS2 behind the server (statements received, ...). */
  FakeHs2 hs2() { return hs2_; }
}
