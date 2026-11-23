package org.apache.impala.tsql;

/**
 * ADH-TSQL. Privilege checks for creating and replacing a procedure.
 * Child statements are authorized on their own, so calling a procedure is not
 * checked here.
 */
public interface TsqlAccess {
  boolean canCreate(String database, String user);

  /** True when the user may replace a procedure another user owns. */
  boolean canReplaceAny(String database, String user);

  /** Anyone may create. Only the stored owner may replace. */
  TsqlAccess OWNER_ONLY = new TsqlAccess() {
    @Override
    public boolean canCreate(String database, String user) { return true; }

    @Override
    public boolean canReplaceAny(String database, String user) { return false; }
  };
}
