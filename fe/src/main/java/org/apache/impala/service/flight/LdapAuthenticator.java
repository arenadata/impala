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

import java.io.File;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Locale;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import javax.naming.ldap.Rdn;
import javax.naming.ldap.StartTlsRequest;
import javax.naming.ldap.StartTlsResponse;
import javax.net.ssl.SSLSocketFactory;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Validates a user name and password with an LDAP simple bind as the user, the DN built
 * from a pattern (like impalad's --ldap_bind_pattern), e.g.
 * {@code uid=#UID,cn=users,cn=accounts,dc=example,dc=com}. No service account is needed.
 *
 * The password must not cross the network in clear text: use StartTLS
 * ({@value #STARTTLS}, optionally with the CA of the directory in
 * {@value #CA_CERTIFICATE}) or an ldaps:// URI (JVM truststore), or allow clear text
 * explicitly with {@value #ALLOW_CLEARTEXT} (tests, development).
 */
public class LdapAuthenticator {
  private static final Logger LOG = LoggerFactory.getLogger(LdapAuthenticator.class);

  public static final String URI = "impala.flight-sql.ldap.uri";
  public static final String BIND_PATTERN = "impala.flight-sql.ldap.bind-pattern";
  public static final String STARTTLS = "impala.flight-sql.ldap.starttls";
  public static final String CA_CERTIFICATE = "impala.flight-sql.ldap.ca-certificate";
  public static final String ALLOW_CLEARTEXT =
      "impala.flight-sql.ldap.allow-cleartext-passwords";
  public static final String CONNECT_TIMEOUT_MS =
      "impala.flight-sql.ldap.connect-timeout-ms";
  public static final String READ_TIMEOUT_MS = "impala.flight-sql.ldap.read-timeout-ms";

  static final String UID = "#UID";

  private final String uri_;
  private final String bindPattern_;
  private final boolean startTls_;
  private final SSLSocketFactory startTlsFactory_;
  private final int connectTimeoutMs_;
  private final int readTimeoutMs_;

  public LdapAuthenticator(Configuration conf) throws IOException {
    uri_ = conf.getTrimmed(URI, "");
    bindPattern_ = conf.getTrimmed(BIND_PATTERN, "");
    Preconditions.checkArgument(!uri_.isEmpty(), "%s must be set for LDAP", URI);
    Preconditions.checkArgument(bindPattern_.contains(UID),
        "%s must be set for LDAP and contain %s", BIND_PATTERN, UID);
    boolean ldaps = uri_.toLowerCase(Locale.ROOT).startsWith("ldaps://");
    startTls_ = conf.getBoolean(STARTTLS, false);
    Preconditions.checkArgument(!(ldaps && startTls_),
        "%s cannot be used with an ldaps:// URI", STARTTLS);
    boolean cleartextOk = conf.getBoolean(ALLOW_CLEARTEXT, false);
    Preconditions.checkArgument(ldaps || startTls_ || cleartextOk,
        "LDAP passwords would be sent in clear text: use an ldaps:// %s, set %s, "
            + "or set %s", URI, STARTTLS, ALLOW_CLEARTEXT);
    String ca = conf.getTrimmed(CA_CERTIFICATE, "");
    Preconditions.checkArgument(ca.isEmpty() || startTls_,
        "%s is only used with %s (ldaps:// uses the JVM truststore)", CA_CERTIFICATE,
        STARTTLS);
    startTlsFactory_ = !startTls_ ? null
        : ca.isEmpty() ? (SSLSocketFactory) SSLSocketFactory.getDefault()
                       : TlsUtil.socketFactoryTrusting(new File(ca), CA_CERTIFICATE);
    connectTimeoutMs_ = conf.getInt(CONNECT_TIMEOUT_MS, 5000);
    readTimeoutMs_ = conf.getInt(READ_TIMEOUT_MS, 10000);
  }

  /** The bind DN for a user name, the name escaped as an RDN value (RFC 4514). */
  @VisibleForTesting
  String bindDn(String user) {
    return bindPattern_.replace(UID, Rdn.escapeValue(user));
  }

  /**
   * Binds as the user. Returns the user name on success; throws FlightLoginException for
   * wrong credentials, empty values (an empty password is an anonymous bind in LDAP and
   * would "succeed") and for directory errors.
   */
  public String authenticate(String user, String password) throws FlightLoginException {
    if (user == null || user.isEmpty()) {
      throw new FlightLoginException("Empty user name");
    }
    if (password == null || password.isEmpty()) {
      throw new FlightLoginException("Empty password");
    }
    String dn = bindDn(user);
    Hashtable<String, Object> env = new Hashtable<>();
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
    env.put(Context.PROVIDER_URL, uri_);
    env.put(Context.REFERRAL, "ignore");
    env.put("com.sun.jndi.ldap.connect.timeout", Integer.toString(connectTimeoutMs_));
    env.put("com.sun.jndi.ldap.read.timeout", Integer.toString(readTimeoutMs_));
    // no pooled connections: every login is a fresh bind
    env.put("com.sun.jndi.ldap.connect.pool", "false");
    LdapContext ctx = null;
    StartTlsResponse tls = null;
    try {
      if (!startTls_) {
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_PRINCIPAL, dn);
        env.put(Context.SECURITY_CREDENTIALS, password);
        ctx = new InitialLdapContext(env, null);
      } else {
        // connect without binding, upgrade to TLS, then bind on the TLS connection
        env.put(Context.SECURITY_AUTHENTICATION, "none");
        ctx = new InitialLdapContext(env, null);
        tls = (StartTlsResponse) ctx.extendedOperation(new StartTlsRequest());
        tls.negotiate(startTlsFactory_);
        ctx.addToEnvironment(Context.SECURITY_AUTHENTICATION, "simple");
        ctx.addToEnvironment(Context.SECURITY_PRINCIPAL, dn);
        ctx.addToEnvironment(Context.SECURITY_CREDENTIALS, password);
        // the bind happens with the next operation
        ctx.getAttributes("", new String[] {"supportedLDAPVersion"});
      }
      return user;
    } catch (javax.naming.AuthenticationException e) {
      throw new FlightLoginException("Invalid username or password");
    } catch (javax.naming.OperationNotSupportedException e) {
      // LDAP 53 (unwillingToPerform): e.g. FreeIPA "Too many failed logins" lockout or a
      // disabled account; the bind is refused whatever the password
      LOG.warn("LDAP login of {} refused by {}: {}", user, uri_, e.getMessage());
      throw new FlightLoginException("Account locked or disabled by the directory");
    } catch (NamingException | IOException e) {
      LOG.warn("LDAP login of {} at {} failed: {}", user, uri_, e.toString());
      throw new FlightLoginException("LDAP server error", e);
    } finally {
      if (tls != null) {
        try { tls.close(); } catch (IOException e) { /* connection closed below */ }
      }
      if (ctx != null) {
        try { ctx.close(); } catch (NamingException e) { /* nothing left to do */ }
      }
    }
  }
}
