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
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KeyTab;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;

import com.google.common.base.Preconditions;

/**
 * Validates single-round SPNEGO tokens ({@code authorization: Negotiate <base64>}) with
 * the service key of {@value #PRINCIPAL} from {@value #KEYTAB}, e.g. the HTTP/_HOST entry
 * of the impala keytab. Returns the client principal; no reply token is sent (clients
 * get a bearer token instead). Same approach as Kyuubi's SparkConnectKerberosValidator.
 */
public class SpnegoValidator {
  public static final String PRINCIPAL = "impala.flight-sql.spnego.principal";
  public static final String KEYTAB = "impala.flight-sql.spnego.keytab";

  private static final Oid SPNEGO = oid("1.3.6.1.5.5.2");
  private static final Oid KRB5 = oid("1.2.840.113554.1.2.2");
  private static final Oid KRB5_PRINCIPAL_NAME = oid("1.2.840.113554.1.2.2.1");

  private final String principal_;
  private final Subject subject_;
  private final GSSManager gss_ = GSSManager.getInstance();

  /** 'hostname' replaces _HOST in the configured principal. */
  public SpnegoValidator(Configuration conf, String hostname) throws IOException {
    String configured = conf.getTrimmed(PRINCIPAL, "");
    String keytab = conf.getTrimmed(KEYTAB, "");
    Preconditions.checkArgument(!configured.isEmpty(), "%s must be set for Kerberos",
        PRINCIPAL);
    Preconditions.checkArgument(!keytab.isEmpty(), "%s must be set for Kerberos", KEYTAB);
    File keytabFile = new File(keytab);
    Preconditions.checkArgument(keytabFile.canRead(), "%s: cannot read %s", KEYTAB,
        keytab);
    principal_ = SecurityUtil.getServerPrincipal(configured, hostname);
    KerberosPrincipal kp = new KerberosPrincipal(principal_);
    KeyTab kt = KeyTab.getInstance(kp, keytabFile);
    Preconditions.checkArgument(kt.getKeys(kp).length > 0, "%s: no key for %s in %s",
        KEYTAB, principal_, keytab);
    subject_ = new Subject();
    subject_.getPrincipals().add(kp);
    subject_.getPrivateCredentials().add(kt);
  }

  public String getPrincipal() { return principal_; }

  /** Returns the client principal for a base64 SPNEGO (or raw Kerberos) token. */
  public String validate(String base64Token) throws FlightLoginException {
    final byte[] token;
    try {
      token = Base64.getDecoder().decode(base64Token.trim());
    } catch (IllegalArgumentException e) {
      throw new FlightLoginException("Malformed Negotiate token");
    }
    try {
      return Subject.doAs(
          subject_, (PrivilegedExceptionAction<String>) () -> accept(token));
    } catch (PrivilegedActionException e) {
      throw new FlightLoginException(
          "Kerberos authentication failed", e.getException());
    }
  }

  private String accept(byte[] token) throws GSSException {
    GSSCredential creds = null;
    GSSContext ctx = null;
    try {
      creds = gss_.createCredential(gss_.createName(principal_, KRB5_PRINCIPAL_NAME),
          GSSCredential.INDEFINITE_LIFETIME, new Oid[] {SPNEGO, KRB5},
          GSSCredential.ACCEPT_ONLY);
      ctx = gss_.createContext(creds);
      ctx.acceptSecContext(token, 0, token.length);
      if (!ctx.isEstablished()) {
        throw new GSSException(GSSException.DEFECTIVE_TOKEN, -1,
            "multi-round SPNEGO is not supported");
      }
      return ctx.getSrcName().toString();
    } finally {
      if (ctx != null) ctx.dispose();
      if (creds != null) creds.dispose();
    }
  }

  private static Oid oid(String value) {
    try {
      return new Oid(value);
    } catch (GSSException e) {
      throw new IllegalStateException(e);
    }
  }
}
