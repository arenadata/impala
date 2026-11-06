package org.apache.impala.tsql;

import org.apache.impala.authorization.Authorizable;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;

/**
 * ADH-TSQL. Builds the Ranger resource for a procedure. The Hive service
 * definition is not changed here. Until that definition gains a procedure
 * resource, a Ranger-enabled cluster fails closed for this type.
 */
public final class TsqlAuthorizer {
  private TsqlAuthorizer() {}

  public static void addResource(java.util.List<RangerAccessResourceImpl> resources,
      Authorizable authorizable) {
    RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
    resource.setValue("database", authorizable.getDbName());
    resource.setValue("procedure", authorizable.getName());
    if (authorizable.getOwnerUser() != null) {
      resource.setOwnerUser(authorizable.getOwnerUser());
    }
    resources.add(resource);
  }
}
