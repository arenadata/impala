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

import org.apache.hadoop.security.SecurityUtil;

import com.google.common.base.Preconditions;

/**
 * Settings of the HS2 loopback: the connection from the Flight SQL endpoint to the
 * coordinator's own HiveServer2 port, where statements run as the Flight user.
 *
 * With a Kerberos principal (impalad's --principal) the loopback logs in from
 * --keytab_file and asks HS2 to delegate the session to the Flight user
 * ({@code impala.doas.user}; impalad's --authorized_proxy_user_config must allow the
 * principal's short name). Without one, it connects without SASL and sends the Flight
 * user as the session user name.
 */
final class LoopbackConfig {
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

  LoopbackConfig(FlightSqlConfig config) throws IOException {
    host = config.loopbackHost;
    port = config.loopbackPort;
    ssl = config.loopbackSsl;
    caCertificate = config.loopbackCaCertificate;
    String p = config.loopbackPrincipal;
    principal = p.isEmpty() ? "" : SecurityUtil.getServerPrincipal(p, host);
    keytab = config.loopbackKeytab;
    connectTimeoutMs = config.loopbackConnectTimeoutMs;
    socketTimeoutMs = config.loopbackSocketTimeoutMs;
    idleTimeoutMs = config.loopbackIdleTimeoutMs;
    maxSessionsPerUser = config.loopbackMaxSessionsPerUser;
    doAs = config.loopbackDoAs != null ? config.loopbackDoAs : kerberos();
    Preconditions.checkArgument(port > 0, "--hs2_port must be set for Flight SQL");
    Preconditions.checkArgument(maxSessionsPerUser > 0,
        "loopback sessions per user must be positive");
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
