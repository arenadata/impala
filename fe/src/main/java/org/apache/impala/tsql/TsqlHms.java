package org.apache.impala.tsql;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.StoredProcedure;
import org.apache.hadoop.hive.metastore.api.StoredProcedureRequest;
import org.apache.impala.catalog.MetaStoreClientPool;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.thrift.TException;

/**
 * ADH-TSQL. Reads and writes procedure source through the coordinator HMS pool.
 * Hive 4 {@code create_stored_procedure} is create-or-update. The source starts
 * with a {@code tsql} line so it is not treated as HPL/SQL.
 */
public final class TsqlHms implements TsqlProcedureStore {
  static final String MARKER = "tsql\n";
  private static final String CATALOG = "hive";

  private final MetaStoreClientPool pool;

  public TsqlHms(MetaStoreClientPool pool) {
    this.pool = pool;
  }

  @Override
  public void upsert(String database, String name, String owner, boolean executeAsOwner,
      String source) {
    if (pool == null) return;
    StoredProcedure proc = new StoredProcedure();
    proc.setCatName(CATALOG);
    proc.setDbName(database);
    proc.setName(name.toLowerCase(Locale.ROOT));
    proc.setOwnerName(owner == null ? "" : owner);
    String body = source != null && source.startsWith(MARKER) ? source : MARKER + source;
    if (executeAsOwner && !body.contains("\nWITH EXECUTE AS OWNER\n")) {
      body = MARKER + "WITH EXECUTE AS OWNER\n" + body.substring(MARKER.length());
    }
    proc.setSource(body);
    try (MetaStoreClient client = pool.getClient()) {
      client.getHiveClient().getThriftClient().getClient().create_stored_procedure(proc);
    } catch (TException e) {
      throw new TsqlParser.ParseException("Could not store procedure " + name
          + ": " + e.getMessage());
    }
  }

  @Override
  public String getSource(String database, String name) {
    if (pool == null) return null;
    StoredProcedureRequest request =
        new StoredProcedureRequest(CATALOG, database, name.toLowerCase(Locale.ROOT));
    try (MetaStoreClient client = pool.getClient()) {
      StoredProcedure proc = client.getHiveClient().getThriftClient().getClient()
          .get_stored_procedure(request);
      return proc == null ? null : proc.getSource();
    } catch (NoSuchObjectException e) {
      return null;
    } catch (MetaException e) {
      throw new TsqlParser.ParseException(e.getMessage());
    } catch (TException e) {
      throw new TsqlParser.ParseException(e.getMessage());
    }
  }

  @Override
  public List<String> list(String database) {
    return new ArrayList<String>();
  }
}
