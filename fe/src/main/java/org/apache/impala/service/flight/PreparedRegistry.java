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
import java.util.function.Consumer;

import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Prepared statements by random handle. A query is started when it is prepared (Impala
 * knows the result schema only once a statement runs); that first result is kept and
 * handed to the first execution, later executions run the query again (as in Kyuubi).
 * A kept result not used within the cached-result TTL is discarded (the statement stays,
 * its next execution runs the query); handles idle longer than the idle TTL are dropped.
 */
final class PreparedRegistry implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(PreparedRegistry.class);

  static final class Prepared {
    final String id;
    final String owner;
    final String sql;
    final boolean isQuery;
    final Schema schema;
    private Pending pending_;
    private volatile long lastUseMs_ = System.currentTimeMillis();

    Prepared(String id, String owner, String sql, boolean isQuery, Schema schema,
        StartedStatement cached) {
      this.id = id;
      this.owner = owner;
      this.sql = sql;
      this.isQuery = isQuery;
      this.schema = schema;
      pending_ = cached == null ? Pending.NONE
          : Pending.cached(cached, System.currentTimeMillis());
    }

    /** The pending work; reset to none. */
    synchronized Pending takePending() {
      Pending pending = pending_;
      pending_ = Pending.NONE;
      return pending;
    }

    synchronized void setPending(Pending pending) { pending_ = pending; }

    /** Takes a result started at prepare time if it is older than 'cutoffMs'. */
    private synchronized StartedStatement takeCachedOlderThan(long cutoffMs) {
      if (pending_.kind != Pending.Kind.CACHED || pending_.atMs > cutoffMs) return null;
      return takePending().cached;
    }

    void touch() { lastUseMs_ = System.currentTimeMillis(); }
  }

  /** Pending work of a prepared statement; taken (and reset to NONE) by an execution. */
  static final class Pending {
    enum Kind { NONE, CACHED, UPDATE_RAN }

    static final Pending NONE = new Pending(Kind.NONE, null, 0);

    final Kind kind;
    /** The result started at prepare time (CACHED), else null. */
    final StartedStatement cached;
    /** CACHED: when the result was started; UPDATE_RAN: when the update ran. */
    final long atMs;

    private Pending(Kind kind, StartedStatement cached, long atMs) {
      this.kind = kind;
      this.cached = cached;
      this.atMs = atMs;
    }

    static Pending cached(StartedStatement cached, long atMs) {
      return new Pending(Kind.CACHED, cached, atMs);
    }

    static Pending updateRan(long atMs) {
      return new Pending(Kind.UPDATE_RAN, null, atMs);
    }
  }

  /** What GetFlightInfo of a prepared statement does. */
  enum Execution { USE_CACHED, EMPTY_RESULT, RUN }

  /**
   * The Arrow Flight SQL JDBC driver (DBeaver) runs an update with DoPut and then also
   * asks for its result with GetFlightInfo, right away. That request must not run the
   * statement again (DDL would fail, DML would be applied twice): within this window
   * after the update it gets an empty result; a later execution runs it again.
   */
  static final long UPDATE_RESULT_WINDOW_MS = 60_000;

  static Execution nextExecution(Pending pending, boolean isQuery, long nowMs) {
    switch (pending.kind) {
      case CACHED:
        return Execution.USE_CACHED;
      case UPDATE_RAN:
        return !isQuery && nowMs - pending.atMs <= UPDATE_RESULT_WINDOW_MS
            ? Execution.EMPTY_RESULT : Execution.RUN;
      default:
        return Execution.RUN;
    }
  }

  private final Map<String, Prepared> prepared_ = new ConcurrentHashMap<>();
  private final Consumer<StartedStatement> discard_;
  private final long cachedTtlMs_;
  private final long idleTtlMs_;
  private final ScheduledExecutorService reaper_;

  /** 'discard' cancels and closes an unused started statement, releases its session. */
  PreparedRegistry(Consumer<StartedStatement> discard, long cachedTtlMs, long idleTtlMs) {
    discard_ = discard;
    cachedTtlMs_ = cachedTtlMs;
    idleTtlMs_ = idleTtlMs;
    reaper_ = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "flight-sql-prepared-reaper");
      t.setDaemon(true);
      return t;
    });
    long period = Math.max(1000, Math.min(30000, cachedTtlMs / 4));
    reaper_.scheduleWithFixedDelay(() -> expire(System.currentTimeMillis()), period,
        period, TimeUnit.MILLISECONDS);
  }

  String register(String owner, String sql, boolean isQuery, Schema schema,
      StartedStatement cached) {
    String id = UUID.randomUUID().toString();
    prepared_.put(id, new Prepared(id, owner, sql, isQuery, schema, cached));
    return id;
  }

  /** Prepared statement 'id' of 'owner', or null (unknown, expired, someone else's). */
  Prepared get(String id, String owner) {
    Prepared p = prepared_.get(id);
    if (p == null || !p.owner.equals(owner)) return null;
    p.touch();
    return p;
  }

  /** Removes a prepared statement, discards its unused result; false if not found. */
  boolean close(String id, String owner) {
    Prepared p = get(id, owner);
    if (p == null || !prepared_.remove(id, p)) return false;
    discardQuietly(p.takePending().cached);
    return true;
  }

  /** Discards kept results older than the cached TTL and drops idle handles. */
  @VisibleForTesting
  int expire(long nowMs) {
    int n = 0;
    for (Prepared p : new ArrayList<>(prepared_.values())) {
      if (nowMs - p.lastUseMs_ >= idleTtlMs_ && prepared_.remove(p.id, p)) {
        LOG.info("Flight SQL prepared statement of {} idle, dropping it", p.owner);
        discardQuietly(p.takePending().cached);
        n++;
      } else {
        StartedStatement old = p.takeCachedOlderThan(nowMs - cachedTtlMs_);
        if (old != null) {
          LOG.info("Prepared Flight SQL query of {} not executed within {} s, "
              + "stopping it", p.owner, cachedTtlMs_ / 1000);
          discardQuietly(old);
          n++;
        }
      }
    }
    return n;
  }

  private void discardQuietly(StartedStatement s) {
    if (s == null) return;
    try {
      discard_.accept(s);
    } catch (RuntimeException e) {
      LOG.debug("Discarding a prepared result failed: {}", e.toString());
    }
  }

  @VisibleForTesting
  int size() { return prepared_.size(); }

  @Override
  public void close() {
    reaper_.shutdownNow();
    List<Prepared> all = new ArrayList<>(prepared_.values());
    prepared_.clear();
    for (Prepared p : all) discardQuietly(p.takePending().cached);
  }
}
