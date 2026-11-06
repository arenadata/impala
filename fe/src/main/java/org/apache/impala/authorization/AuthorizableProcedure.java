package org.apache.impala.authorization;

/**
 * ADH-TSQL. A stored procedure. The name is {@code database.procedure}.
 */
public class AuthorizableProcedure extends Authorizable {
  private final String dbName;
  private final String procedureName;
  private final String owner;

  public AuthorizableProcedure(String dbName, String procedureName, String owner) {
    this.dbName = dbName;
    this.procedureName = procedureName;
    this.owner = owner;
  }

  @Override
  public String getName() { return procedureName; }

  @Override
  public Type getType() { return Type.PROCEDURE; }

  @Override
  public String getDbName() { return dbName; }

  @Override
  public String getOwnerUser() { return owner; }
}
