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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;

import com.google.common.base.Preconditions;

/**
 * Settings of the HS2 loopback: the connection from the Flight SQL endpoint to the
 * coordinator's own HiveServer2 port, where statements run as the Flight user.
 *
 * With a Kerberos principal (default: impalad's -principal) the loopback logs in from
 * {@value #KEYTAB} and asks HS2 to delegate the session to the Flight user
 * ({@code impala.doas.user}; impalad's --authorized_proxy_user_config must allow the
 * principal's short name). Without one, it connects without SASL and sends the Flight
 * user as the session user name.
 */
final class LoopbackConfig {
  static final String HOST = "impala.flight-sql.loopback.host";
  static final String PORT = "impala.flight-sql.loopback.port";
  static final String SSL = "impala.flight-sql.loopback.ssl";
  static final String CA_CERTIFICATE = "impala.flight-sql.loopback.ca-certificate";
  static final String PRINCIPAL = "impala.flight-sql.loopback.principal";
  static final String KEYTAB = "impala.flight-sql.loopback.keytab";
  static final String CONNECT_TIMEOUT_MS =
      "impala.flight-sql.loopback.connect-timeout-ms";
  static final String SOCKET_TIMEOUT_MS = "impala.flight-sql.loopback.socket-timeout-ms";
  static final String IDLE_TIMEOUT_S = "impala.flight-sql.loopback.idle-timeout-s";
  static final String MAX_SESSIONS_PER_USER =
      "impala.flight-sql.loopback.max-sessions-per-user";
  /** doas (impala.doas.user; default with Kerberos) or username (session user name). */
  static final String IMPERSONATION = "impala.flight-sql.loopback.impersonation";

  final String host;
  final int port;
  final boolean ssl;
  final String caCertificate;
  /** Kerberos principal with _HOST resolved; empty: no SASL. */
  final String principal;
  final String keytab;
  final int connectTimeoutMs;
  final int socketTimeoutMs;
  final long idleTimeoutMs;
  final int maxSessionsPerUser;
  /** True: impala.doas.user; false: the Flight user is sent as the session user name. */
  final boolean doAs;

  /**
   * 'hostname' is the coordinator's FQDN, 'backendPrincipal' impalad's -principal (may be
   * empty when Kerberos is off).
   */
  LoopbackConfig(Configuration conf, String hostname, String backendPrincipal)
      throws IOException {
    host = conf.getTrimmed(HOST, hostname);
    port = conf.getInt(PORT, 21050);
    ssl = conf.getBoolean(SSL, true);
    caCertificate = conf.getTrimmed(CA_CERTIFICATE, "/etc/ssl/certs/ca-certificates.crt");
    String p =
        conf.getTrimmed(PRINCIPAL, backendPrincipal == null ? "" : backendPrincipal);
    principal = p.isEmpty() ? "" : SecurityUtil.getServerPrincipal(p, host);
    keytab = conf.getTrimmed(KEYTAB, "/etc/security/keytabs/impala.service.keytab");
    connectTimeoutMs = conf.getInt(CONNECT_TIMEOUT_MS, 10000);
    // statements block in FetchResults until rows are ready; Impala returns
    // STILL_EXECUTING after fetch_rows_timeout_ms (10 s), so 5 min is generous
    socketTimeoutMs = conf.getInt(SOCKET_TIMEOUT_MS, 300000);
    idleTimeoutMs = conf.getLong(IDLE_TIMEOUT_S, 600) * 1000L;
    maxSessionsPerUser = conf.getInt(MAX_SESSIONS_PER_USER, 8);
    String impersonation =
        conf.getTrimmed(IMPERSONATION, kerberos() ? "doas" : "username");
    Preconditions.checkArgument(
        impersonation.equals("doas") || impersonation.equals("username"),
        "%s must be doas or username", IMPERSONATION);
    doAs = impersonation.equals("doas");
    Preconditions.checkArgument(port > 0, "%s must be set", PORT);
    Preconditions.checkArgument(maxSessionsPerUser > 0, "%s must be positive",
        MAX_SESSIONS_PER_USER);
  }

  boolean kerberos() { return !principal.isEmpty(); }

  /** Service name of the principal (impala for impala/host@REALM). */
  String serviceName() { return principal.split("[/@]", 2)[0]; }

  /** Host part of the principal (the SASL server name). */
  String principalHost() {
    String[] parts = principal.split("[/@]");
    return parts.length >= 3 ? parts[1] : host;
  }
}
