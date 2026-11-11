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
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.base.Preconditions;

/**
 * Kerberos login of the HS2 loopback from its keytab, separate from impalad's own login
 * (which comes from the ticket cache and is not renewed, see the Impala ticket expiry
 * issue). Relogs in from the keytab before new connections when the TGT is old.
 */
final class LoopbackLogin {
  private final UserGroupInformation ugi_;

  LoopbackLogin(LoopbackConfig config) throws IOException {
    Preconditions.checkArgument(new File(config.keytab).canRead(), "%s: cannot read %s",
        LoopbackConfig.KEYTAB, config.keytab);
    ugi_ = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        config.principal, config.keytab);
  }

  <T> T doAs(PrivilegedExceptionAction<T> action) throws Exception {
    ugi_.checkTGTAndReloginFromKeytab();
    return ugi_.doAs(action);
  }
}
