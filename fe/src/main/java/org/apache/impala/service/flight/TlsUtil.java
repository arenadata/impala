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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

import com.google.common.base.Preconditions;

/** TLS helpers of the Flight SQL endpoint. */
final class TlsUtil {
  private TlsUtil() {}

  /**
   * TLS socket factory trusting the PEM certificate(s) in 'pem' (one CA or a bundle such
   * as /etc/ssl/certs/ca-certificates.crt). 'key' names the setting for error messages.
   */
  static SSLSocketFactory socketFactoryTrusting(File pem, String key) throws IOException {
    Preconditions.checkArgument(pem.canRead(), "%s: cannot read %s", key, pem);
    try (InputStream in = new FileInputStream(pem)) {
      KeyStore trust = KeyStore.getInstance(KeyStore.getDefaultType());
      trust.load(null, null);
      int i = 0;
      for (Certificate cert : CertificateFactory.getInstance("X.509")
          .generateCertificates(in)) {
        trust.setCertificateEntry("ca-" + i++, cert);
      }
      Preconditions.checkArgument(i > 0, "%s: no certificate in %s", key, pem);
      TrustManagerFactory tmf =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(trust);
      SSLContext ssl = SSLContext.getInstance("TLS");
      ssl.init(null, tmf.getTrustManagers(), null);
      return ssl.getSocketFactory();
    } catch (GeneralSecurityException e) {
      throw new IOException("Cannot load " + pem + " (" + key + ")", e);
    }
  }
}
