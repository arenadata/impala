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

import java.util.Locale;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.StoredProcedure;
import org.apache.hadoop.hive.metastore.api.StoredProcedureRequest;
import org.apache.impala.catalog.MetaStoreClientPool;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.thrift.TException;

/**
 * Reads and writes procedure source through the coordinator HMS pool.
 * Hive 4 {@code create_stored_procedure} is create-or-update. The stored text is
 * the client batch, which starts with {@code TSQL} or {@code LPSQL}.
 */
public final class TsqlHms implements TsqlProcedureStore {
  private static final String CATALOG = "hive";

  private final MetaStoreClientPool pool;

  public TsqlHms(MetaStoreClientPool pool) {
    this.pool = pool;
  }

  @Override
  public void upsert(String database, String name, String owner, String source) {
    if (pool == null) return;
    StoredProcedure proc = new StoredProcedure();
    proc.setCatName(CATALOG);
    proc.setDbName(database);
    proc.setName(name.toLowerCase(Locale.ROOT));
    proc.setOwnerName(owner == null ? "" : owner);
    proc.setSource(source == null ? "" : source);
    try (MetaStoreClient client = pool.getClient()) {
      client.getHiveClient().getThriftClient().getClient().create_stored_procedure(proc);
    } catch (TException e) {
      throw new TsqlParser.ParseException("Could not store procedure " + name
          + ": " + e.getMessage());
    }
  }

  @Override
  public String getSource(String database, String name) {
    StoredProcedure proc = fetch(database, name);
    return proc == null ? null : proc.getSource();
  }

  @Override
  public String getOwner(String database, String name) {
    StoredProcedure proc = fetch(database, name);
    if (proc == null) return null;
    return proc.getOwnerName() == null ? "" : proc.getOwnerName();
  }

  private StoredProcedure fetch(String database, String name) {
    if (pool == null) return null;
    StoredProcedureRequest request =
        new StoredProcedureRequest(CATALOG, database, name.toLowerCase(Locale.ROOT));
    try (MetaStoreClient client = pool.getClient()) {
      return client.getHiveClient().getThriftClient().getClient()
          .get_stored_procedure(request);
    } catch (NoSuchObjectException e) {
      return null;
    } catch (MetaException e) {
      throw new TsqlParser.ParseException(e.getMessage());
    } catch (TException e) {
      throw new TsqlParser.ParseException(e.getMessage());
    }
  }

  @Override
  public void drop(String database, String name) {
    if (pool == null) return;
    StoredProcedureRequest request =
        new StoredProcedureRequest(CATALOG, database, name.toLowerCase(Locale.ROOT));
    try (MetaStoreClient client = pool.getClient()) {
      client.getHiveClient().getThriftClient().getClient().drop_stored_procedure(request);
    } catch (NoSuchObjectException e) {
      return;
    } catch (TException e) {
      throw new TsqlParser.ParseException(e.getMessage());
    }
  }
}
