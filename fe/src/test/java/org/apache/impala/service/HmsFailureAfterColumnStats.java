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

package org.apache.impala.service;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.catalog.MetaStoreClientPool;

/**
 * Makes the HMS clients of 'catalog' fail a call that follows a successful
 * updateTableColumnStatistics, until close(). The clients come from a pool of their own
 * whose Hive client is replaced through reflection, since MetaStoreClient cannot be
 * built outside its pool.
 */
final class HmsFailureAfterColumnStats implements AutoCloseable {
  /** Which HMS calls fail once the column statistics are written. */
  private enum Mode { TAIL_ALTER_TABLE, RELOAD, TAIL_ALTER_TABLE_AND_READS, READS }

  private final Catalog catalog_;
  private final Mode mode_;
  private final MetaStoreClientPool original_;
  private final MetaStoreClientPool failing_;
  private final AtomicBoolean columnStatsWritten_ = new AtomicBoolean();
  private final AtomicBoolean tableWritten_ = new AtomicBoolean();
  private final AtomicInteger failures_ = new AtomicInteger();
  private final Set<MetaStoreClientPool.MetaStoreClient> wrapped_ =
      Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));

  private HmsFailureAfterColumnStats(Catalog catalog, Mode mode) {
    catalog_ = catalog;
    mode_ = mode;
    original_ = catalog.getMetaStoreClientPool();
    failing_ = new MetaStoreClientPool(0, 0) {
      @Override
      public MetaStoreClient getClient() {
        MetaStoreClient client = super.getClient();
        wrap(client);
        return client;
      }
    };
  }

  /**
   * Every alter_table fails: the final table write of a statistics update without
   * Iceberg parameters fails after its column statistics were written.
   */
  static HmsFailureAfterColumnStats failTailAlterTable(Catalog catalog) {
    return install(new HmsFailureAfterColumnStats(catalog, Mode.TAIL_ALTER_TABLE));
  }

  /**
   * The first getTable after a successful alter_table fails: the statistics update
   * succeeded, the reload of the table that follows it fails.
   */
  static HmsFailureAfterColumnStats failReload(Catalog catalog) {
    return install(new HmsFailureAfterColumnStats(catalog, Mode.RELOAD));
  }

  /**
   * Every alter_table and getTable fails: HMS goes down after the column statistics, so
   * the table write and the reads that follow it fail together.
   */
  static HmsFailureAfterColumnStats failTailAlterTableAndReads(Catalog catalog) {
    return install(new HmsFailureAfterColumnStats(catalog,
        Mode.TAIL_ALTER_TABLE_AND_READS));
  }

  /**
   * Every getTable fails: the statistics update succeeded, the reload after it and every
   * later read of the table fail.
   */
  static HmsFailureAfterColumnStats failReads(Catalog catalog) {
    return install(new HmsFailureAfterColumnStats(catalog, Mode.READS));
  }

  private static HmsFailureAfterColumnStats install(HmsFailureAfterColumnStats res) {
    res.catalog_.setMetaStoreClientPool(res.failing_);
    return res;
  }

  int failures() { return failures_.get(); }

  @Override
  public void close() {
    catalog_.setMetaStoreClientPool(original_);
    failing_.close();
  }

  private boolean shouldFail(String method) {
    if (!columnStatsWritten_.get()) return false;
    switch (mode_) {
      case TAIL_ALTER_TABLE: return method.equals("alter_table");
      case RELOAD:
        return method.equals("getTable") && tableWritten_.get() && failures_.get() == 0;
      case TAIL_ALTER_TABLE_AND_READS:
        return method.equals("alter_table") || method.equals("getTable");
      case READS: return method.equals("getTable");
      default: throw new IllegalStateException(mode_.toString());
    }
  }

  private void wrap(MetaStoreClientPool.MetaStoreClient client) {
    if (!wrapped_.add(client)) return;
    try {
      Field field = MetaStoreClientPool.MetaStoreClient.class
          .getDeclaredField("hiveClient_");
      field.setAccessible(true);
      IMetaStoreClient hive = (IMetaStoreClient) field.get(client);
      field.set(client, Proxy.newProxyInstance(IMetaStoreClient.class.getClassLoader(),
          new Class<?>[] {IMetaStoreClient.class}, (proxy, method, args) -> {
            String name = method.getName();
            if (shouldFail(name)) {
              failures_.incrementAndGet();
              throw new MetaException("injected " + name + " failure");
            }
            Object res;
            try {
              res = method.invoke(hive, args);
            } catch (InvocationTargetException e) {
              throw e.getCause();
            }
            if (name.equals("updateTableColumnStatistics")) {
              columnStatsWritten_.set(true);
            } else if (name.equals("alter_table") && columnStatsWritten_.get()) {
              tableWritten_.set(true);
            }
            return res;
          }));
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }
}
