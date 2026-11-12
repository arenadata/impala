package org.apache.impala.tsql;

/**
 * In-memory or HMS-backed procedure catalog. Names are case-insensitive.
 */
public interface TsqlProcedureStore {
  void upsert(String database, String name, String owner, boolean executeAsOwner, String source);

  /** Source including the leading "tsql\n" marker, or null when the procedure is absent. */
  String getSource(String database, String name);

  java.util.List<String> list(String database);
}
