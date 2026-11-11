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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hive.service.rpc.thrift.TOperationHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Statements started by GetFlightInfo and waiting for their DoGet, by random ticket id.
 * A statement holds its loopback session until it is fetched, cancelled or expires
 * (not fetched within the ticket TTL); then the operation is closed and the session goes
 * back to the pool.
 */
final class QueryRegistry implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(QueryRegistry.class);

  static final class Query {
    final String id;
    final String owner;
    /** null for statements that already finished (no result set). */
    final LoopbackSession session;
    final TOperationHandle operation;
    final Schema schema;
    final long createdMs = System.currentTimeMillis();

    Query(String id, String owner, LoopbackSession session, TOperationHandle operation,
        Schema schema) {
      this.id = id;
      this.owner = owner;
      this.session = session;
      this.operation = operation;
      this.schema = schema;
    }
  }

  private final Map<String, Query> queries_ = new ConcurrentHashMap<>();
  private final LoopbackSessions sessions_;
  private final long ttlMs_;
  private final ScheduledExecutorService reaper_;

  QueryRegistry(LoopbackSessions sessions, long ttlMs) {
    sessions_ = sessions;
    ttlMs_ = ttlMs;
    reaper_ = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "flight-sql-ticket-reaper");
      t.setDaemon(true);
      return t;
    });
    long period = Math.max(1000, Math.min(30000, ttlMs / 4));
    reaper_.scheduleWithFixedDelay(() -> expire(System.currentTimeMillis()), period,
        period, TimeUnit.MILLISECONDS);
  }

  /** Registers a statement; returns its ticket id. */
  String register(String owner, LoopbackSession session, TOperationHandle operation,
      Schema schema) {
    String id = UUID.randomUUID().toString();
    queries_.put(id, new Query(id, owner, session, operation, schema));
    return id;
  }

  /**
   * Removes and returns the statement for 'id' if it belongs to 'owner'; null otherwise
   * (unknown, expired, already fetched, or someone else's - not distinguished).
   */
  Query take(String id, String owner) {
    Query q = queries_.get(id);
    if (q == null || !q.owner.equals(owner)) return null;
    return queries_.remove(id, q) ? q : null;
  }

  /** Closes the operation of a taken statement and gives its session back. */
  void finish(Query q) {
    if (q.session == null) return;
    try {
      if (q.operation != null) q.session.closeOperation(q.operation);
    } catch (LoopbackException e) {
      LOG.debug("Closing Flight SQL statement {} failed: {}", q.id, e.getMessage());
    } finally {
      sessions_.release(q.session);
    }
  }

  /** Finishes statements not fetched within the TTL; returns how many. */
  @VisibleForTesting
  int expire(long nowMs) {
    List<Query> expired = new ArrayList<>();
    for (Query q : queries_.values()) {
      if (nowMs - q.createdMs >= ttlMs_ && queries_.remove(q.id, q)) expired.add(q);
    }
    for (Query q : expired) {
      LOG.info("Flight SQL statement of {} not fetched within {} s, closing it", q.owner,
          ttlMs_ / 1000);
      if (q.session != null && q.operation != null) q.session.cancel(q.operation);
      finish(q);
    }
    return expired.size();
  }

  @VisibleForTesting
  int size() { return queries_.size(); }

  @Override
  public void close() {
    reaper_.shutdownNow();
    List<Query> all = new ArrayList<>(queries_.values());
    queries_.clear();
    all.forEach(this::finish);
  }
}
