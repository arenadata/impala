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

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.thrift.TBackendGflags;

import com.google.common.base.Strings;

/**
 * Settings of the Arrow Flight SQL endpoint.
 *
 * In impalad they come from gflags ({@link #fromBackend}): three of the endpoint's own,
 * the rest from the flags HS2 already uses:
 * <ul>
 *   <li>{@value #PORT_FLAG}: listening port; 0 (default) disables the endpoint</li>
 *   <li>{@value #AUTH_METHODS_FLAG}: {@code kerberos}, {@code ldap} or both; empty
 *   (default) means as HS2: kerberos with --principal, ldap with --enable_ldap_auth</li>
 *   <li>{@value #MEMORY_LIMIT_FLAG}: limit of the Arrow allocator</li>
 *   <li>TLS: --ssl_server_certificate and --ssl_private_key (TLS on when set)</li>
 *   <li>Kerberos: {@code HTTP/_HOST@<realm of --principal>} from --keytab_file</li>
 *   <li>LDAP: --ldap_uri, --ldap_bind_pattern, --ldap_tls, --ldap_ca_certificate,
 *   --ldap_passwords_in_clear_ok (used by Flight even when --enable_ldap_auth is
 *   off)</li>
 *   <li>HS2 loopback: impalad's host, --hs2_port, --principal and --keytab_file; TLS as
 *   HS2, trusting --ssl_client_ca_certificate or the system CA list</li>
 * </ul>
 * The other fields are fixed in impalad; tests change them directly.
 */
public final class FlightSqlConfig {
  public static final String PORT_FLAG = "--flight_sql_port";
  public static final String AUTH_METHODS_FLAG = "--flight_sql_auth_methods";
  public static final String MEMORY_LIMIT_FLAG = "--flight_sql_memory_limit_bytes";

  static final String SYSTEM_CA_CERTIFICATES = "/etc/ssl/certs/ca-certificates.crt";

  /** The coordinator's FQDN: replaces _HOST in the SPNEGO principal. */
  String hostname = "localhost";

  // endpoint
  /** Port; 0 disables the endpoint in impalad, and picks a free port in tests. */
  int port = 0;
  String bindHost = "0.0.0.0";
  /** PEM certificate chain and PKCS#8 key; both empty: plaintext. */
  String sslCertificate = "";
  String sslPrivateKey = "";
  long memoryLimitBytes = 256L << 20;
  /** Rows per HS2 fetch and Arrow batch. */
  int fetchRows = 10000;
  /** How long a started statement waits for its DoGet. */
  long ticketTtlMs = 300_000L;
  /** How long an unused prepared statement is kept. */
  long preparedIdleMs = 7_200_000L;

  // authentication
  /** Comma-separated kerberos, ldap; or none alone (tests, development). */
  String authMethods = "";
  /** Bearer token lifetime after its last use. */
  int tokenTtlMinutes = 120;
  /** HTTP principal (_HOST resolved) and keytab for SPNEGO. */
  String spnegoPrincipal = "";
  String spnegoKeytab = "";
  String ldapUri = "";
  /** Bind DN with #UID for the user name. */
  String ldapBindPattern = "";
  /** StartTLS on ldap:// URIs. */
  boolean ldapTls = false;
  /** CA of the LDAP server for StartTLS; empty: the JVM truststore. */
  String ldapCaCertificate = "";
  boolean ldapAllowCleartext = false;
  int ldapConnectTimeoutMs = 5000;
  int ldapReadTimeoutMs = 10000;

  // HS2 loopback
  String loopbackHost = "localhost";
  int loopbackPort = 21050;
  boolean loopbackSsl = false;
  String loopbackCaCertificate = SYSTEM_CA_CERTIFICATES;
  /** Kerberos principal (_HOST resolved); empty: no SASL. */
  String loopbackPrincipal = "";
  String loopbackKeytab = "";
  /** impala.doas.user; null: with Kerberos only, else the session user name. */
  Boolean loopbackDoAs = null;
  int loopbackConnectTimeoutMs = 10000;
  // statements block in FetchResults until rows are ready; Impala returns
  // STILL_EXECUTING after fetch_rows_timeout_ms (10 s), so 5 min is generous
  int loopbackSocketTimeoutMs = 300000;
  long loopbackIdleTimeoutMs = 600_000L;
  int loopbackMaxSessionsPerUser = 8;

  /** Settings for tests; plaintext, no authentication methods set. */
  public FlightSqlConfig() {}

  /** Settings of a coordinator from impalad's flags. */
  public static FlightSqlConfig fromBackend(TBackendGflags cfg) {
    FlightSqlConfig c = new FlightSqlConfig();
    c.hostname = cfg.hostname;
    c.port = cfg.flight_sql_port;
    c.memoryLimitBytes = cfg.flight_sql_memory_limit_bytes;
    String principal = Strings.nullToEmpty(cfg.principal).trim();
    String keytab = Strings.nullToEmpty(cfg.keytab_file).trim();
    c.authMethods = authMethods(
        Strings.nullToEmpty(cfg.flight_sql_auth_methods), principal,
        cfg.enable_ldap_auth);
    c.sslCertificate = Strings.nullToEmpty(cfg.ssl_server_certificate).trim();
    c.sslPrivateKey = Strings.nullToEmpty(cfg.ssl_private_key).trim();
    if (!principal.isEmpty()) {
      int at = principal.indexOf('@');
      c.spnegoPrincipal = "HTTP/_HOST" + (at >= 0 ? principal.substring(at) : "");
    }
    c.spnegoKeytab = keytab;
    c.ldapUri = Strings.nullToEmpty(cfg.ldap_uri).trim();
    c.ldapBindPattern = Strings.nullToEmpty(cfg.ldap_bind_pattern).trim();
    c.ldapTls = cfg.ldap_tls;
    c.ldapCaCertificate = Strings.nullToEmpty(cfg.ldap_ca_certificate).trim();
    c.ldapAllowCleartext = cfg.ldap_passwords_in_clear_ok;
    c.loopbackHost = cfg.hostname;
    c.loopbackPort = cfg.hs2_port;
    c.loopbackSsl = !c.sslCertificate.isEmpty();
    String ca = Strings.nullToEmpty(cfg.ssl_client_ca_certificate).trim();
    c.loopbackCaCertificate = ca.isEmpty() ? SYSTEM_CA_CERTIFICATES : ca;
    c.loopbackPrincipal = principal;
    c.loopbackKeytab = keytab;
    return c;
  }

  /**
   * The methods to enable: the flag when set, else those of HS2 (kerberos with a
   * principal, ldap with --enable_ldap_auth). Empty: nothing to log in with.
   */
  static String authMethods(String flag, String principal, boolean ldapEnabled) {
    if (!flag.trim().isEmpty()) return flag.trim();
    List<String> methods = new ArrayList<>();
    if (!principal.isEmpty()) methods.add("kerberos");
    if (ldapEnabled) methods.add("ldap");
    return String.join(",", methods);
  }

  boolean tls() { return !sslCertificate.isEmpty() || !sslPrivateKey.isEmpty(); }
}
