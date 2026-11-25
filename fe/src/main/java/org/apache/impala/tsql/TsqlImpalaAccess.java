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

package org.apache.impala.tsql;

import org.apache.impala.authorization.Privilege;
import org.apache.impala.authorization.PrivilegeRequest;
import org.apache.impala.authorization.PrivilegeRequestBuilder;
import org.apache.impala.authorization.User;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.common.InternalException;
import org.apache.impala.service.Frontend;

/**
 * CREATE and ALL are database privileges Ranger already understands,
 * so the Hive service definition does not grow a procedure resource.
 */
public final class TsqlImpalaAccess implements TsqlAccess {
  private final Frontend frontend;

  public TsqlImpalaAccess(Frontend frontend) {
    this.frontend = frontend;
  }

  @Override
  public boolean canCreate(String database, String user) {
    return has(database, user, Privilege.CREATE);
  }

  @Override
  public boolean canReplaceAny(String database, String user) {
    // With authorization off hasAccess() is always true, which would let anyone
    // replace a procedure they do not own. The owner check then stands alone.
    return frontend.getAuthzFactory().getAuthorizationConfig().isEnabled()
        && has(database, user, Privilege.ALL);
  }

  private boolean has(String database, String user, Privilege privilege) {
    FeDb db = frontend.getCatalog().getDb(database);
    String dbOwner = db == null || db.getMetaStoreDb() == null
        ? null : db.getMetaStoreDb().getOwnerName();
    PrivilegeRequest request = new PrivilegeRequestBuilder(
        frontend.getAuthzFactory().getAuthorizableFactory())
        .onDb(database, dbOwner).allOf(privilege).build();
    try {
      return frontend.getAuthzChecker().hasAccess(new User(user), request);
    } catch (InternalException e) {
      throw new TsqlParser.ParseException(e.getMessage());
    }
  }
}
