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
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.AuthUtilities;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.GeneratedBearerTokenAuthenticator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.impala.authorization.User;
import org.apache.impala.common.InternalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

/**
 * Authenticates Flight SQL calls:
 * <ul>
 *   <li>{@code authorization: Basic <user:password>} - LDAP
 *   ({@link LdapAuthenticator})</li>
 *   <li>{@code authorization: Negotiate <token>} - Kerberos/SPNEGO
 *   ({@link SpnegoValidator}); the principal is mapped to the short name with the
 *   auth_to_local rules, as for HiveServer2</li>
 *   <li>after either, the response carries {@code authorization: Bearer <token>}
 *   ({@link GeneratedBearerTokenAuthenticator}), valid on this coordinator for
 *   {@value #TOKEN_TTL_MINUTES} minutes after its last use</li>
 * </ul>
 * {@value #METHODS} lists the enabled methods: {@code kerberos}, {@code ldap}, or
 * {@code none} alone (no authentication: user from the {@code x-user-name} header or
 * "anonymous"; development only). Not set means misconfigured: the endpoint does not
 * start. Every failure is reported as UNAUTHENTICATED.
 */
public class ImpalaFlightAuthenticator implements CallHeaderAuthenticator {
  private static final Logger LOG =
      LoggerFactory.getLogger(ImpalaFlightAuthenticator.class);

  public static final String METHODS = "impala.flight-sql.auth.methods";
  public static final String TOKEN_TTL_MINUTES =
      "impala.flight-sql.auth.token-ttl-minutes";
  static final int DEFAULT_TOKEN_TTL_MINUTES = 120;

  static final String NEGOTIATE_PREFIX = "Negotiate ";
  static final String USER_HEADER = "x-user-name";
  static final String ANONYMOUS = "anonymous";

  public enum Method { KERBEROS, LDAP, NONE }

  private final Set<Method> methods_;
  private final LdapAuthenticator ldap_;
  private final SpnegoValidator spnego_;

  /** The authenticator for the endpoint: this one wrapped for bearer tokens. */
  public static CallHeaderAuthenticator create(Configuration conf, String hostname)
      throws IOException {
    ImpalaFlightAuthenticator auth = new ImpalaFlightAuthenticator(conf, hostname);
    if (auth.methods_.contains(Method.NONE)) return auth;
    int ttl = conf.getInt(TOKEN_TTL_MINUTES, DEFAULT_TOKEN_TTL_MINUTES);
    Preconditions.checkArgument(ttl > 0, "%s must be positive", TOKEN_TTL_MINUTES);
    return new GeneratedBearerTokenAuthenticator(auth, ttl);
  }

  @VisibleForTesting
  ImpalaFlightAuthenticator(Configuration conf, String hostname) throws IOException {
    methods_ = parseMethods(conf.getTrimmed(METHODS, ""));
    ldap_ = methods_.contains(Method.LDAP) ? new LdapAuthenticator(conf) : null;
    spnego_ = methods_.contains(Method.KERBEROS)
        ? new SpnegoValidator(conf, hostname) : null;
    if (spnego_ != null && !KerberosName.hasRulesBeenSet()) {
      // BackendConfig sets the cluster's auth_to_local rules when enabled
      KerberosName.setRules("DEFAULT");
    }
  }

  static Set<Method> parseMethods(String value) {
    Preconditions.checkArgument(!value.isEmpty(),
        "%s must be set (kerberos, ldap or none)", METHODS);
    Set<Method> methods = EnumSet.noneOf(Method.class);
    for (String m : Splitter.on(',').trimResults().omitEmptyStrings().split(value)) {
      try {
        methods.add(Method.valueOf(m.toUpperCase(Locale.ROOT)));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            METHODS + ": unknown method '" + m + "' (kerberos, ldap or none)");
      }
    }
    Preconditions.checkArgument(!methods.isEmpty(),
        "%s must be set (kerberos, ldap or none)", METHODS);
    Preconditions.checkArgument(!methods.contains(Method.NONE) || methods.size() == 1,
        "%s: none cannot be combined with other methods", METHODS);
    return methods;
  }

  public Set<Method> getMethods() { return methods_; }

  @Override
  public AuthResult authenticate(CallHeaders headers) {
    String basic =
        AuthUtilities.getValueFromAuthHeader(headers, Auth2Constants.BASIC_PREFIX);
    String negotiate = AuthUtilities.getValueFromAuthHeader(headers, NEGOTIATE_PREFIX);
    try {
      if (basic != null && ldap_ != null) return loggedIn(ldap(basic), "ldap");
      if (negotiate != null && spnego_ != null) {
        return loggedIn(kerberos(negotiate), "kerberos");
      }
      if (methods_.contains(Method.NONE)) {
        String user = headers.get(USER_HEADER);
        return result(user == null || user.trim().isEmpty() ? ANONYMOUS : user.trim());
      }
      if (basic != null) throw unauthenticated("LDAP authentication is not enabled");
      if (negotiate != null) {
        throw unauthenticated("Kerberos authentication is not enabled");
      }
      throw unauthenticated(
          "Missing Flight SQL authorization header (" + accepted() + ")");
    } catch (FlightLoginException e) {
      LOG.info("Flight SQL login failed: {}", e.getMessage());
      throw unauthenticated(e.getMessage());
    } catch (FlightRuntimeException e) {
      throw e;
    } catch (RuntimeException e) {
      // never let an internal error surface as UNKNOWN
      LOG.warn("Flight SQL login failed", e);
      throw unauthenticated("Authentication failed");
    }
  }

  private String ldap(String encoded) throws FlightLoginException {
    final String decoded;
    try {
      decoded = new String(Base64.getDecoder().decode(encoded.trim()),
          StandardCharsets.UTF_8);
    } catch (IllegalArgumentException e) {
      throw new FlightLoginException("Malformed Basic credentials");
    }
    int colon = decoded.indexOf(':');
    if (colon < 0) throw new FlightLoginException("Malformed Basic credentials");
    return ldap_.authenticate(decoded.substring(0, colon), decoded.substring(colon + 1));
  }

  private String kerberos(String token) throws FlightLoginException {
    String principal = spnego_.validate(token);
    try {
      return new User(principal).getShortName();
    } catch (InternalException e) {
      throw new FlightLoginException("No user name for principal " + principal, e);
    }
  }

  private String accepted() {
    StringBuilder sb = new StringBuilder();
    if (ldap_ != null) sb.append("Basic");
    if (spnego_ != null) sb.append(sb.length() > 0 ? " or " : "").append("Negotiate");
    return sb.toString();
  }

  private static AuthResult loggedIn(String user, String method) {
    LOG.info("Flight SQL login: user={} method={}", user, method);
    return result(user);
  }

  private static AuthResult result(String user) {
    return () -> user;
  }

  private static FlightRuntimeException unauthenticated(String message) {
    return CallStatus.UNAUTHENTICATED.withDescription(message).toRuntimeException();
  }
}
