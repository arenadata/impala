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

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLSocketFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Pool of HS2 loopback sessions per Flight user. A call borrows a session exclusively
 * and releases it; idle sessions are reused by the same user's later calls and closed
 * after the idle timeout. At most maxSessionsPerUser sessions (idle or in use) per user.
 */
final class LoopbackSessions implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(LoopbackSessions.class);

  /** Opens a new session for a user. */
  interface Opener {
    LoopbackSession open(String user) throws LoopbackException;
  }

  /** Over the per-user session limit (reported as UNAVAILABLE). */
  static final class TooManySessionsException extends LoopbackException {
    TooManySessionsException(String message) { super(message); }
  }

  private final Opener opener_;
  private final long idleTimeoutMs_;
  private final int maxPerUser_;
  // guarded by this
  private final Map<String, Deque<LoopbackSession>> idle_ = new HashMap<>();
  private final Map<String, Integer> open_ = new HashMap<>();
  private boolean closed_;
  private final ScheduledExecutorService reaper_;

  /** Sessions to the coordinator's HS2 port as configured. */
  static LoopbackSessions create(LoopbackConfig config) throws IOException {
    LoopbackLogin login = config.kerberos() ? new LoopbackLogin(config) : null;
    SSLSocketFactory tls = config.ssl
        ? TlsUtil.socketFactoryTrusting(new File(config.caCertificate),
              LoopbackConfig.CA_CERTIFICATE)
        : null;
    LOG.info("HS2 loopback: {}:{} ({}{}, {})", config.host, config.port,
        tls != null ? "TLS" : "plaintext",
        config.kerberos() ? ", Kerberos as " + config.principal : "",
        config.doAs ? "impala.doas.user" : "session user name");
    return new LoopbackSessions(
        user -> LoopbackSession.open(config, login, tls, user),
        config.idleTimeoutMs, config.maxSessionsPerUser);
  }

  @VisibleForTesting
  LoopbackSessions(Opener opener, long idleTimeoutMs, int maxPerUser) {
    opener_ = opener;
    idleTimeoutMs_ = idleTimeoutMs;
    maxPerUser_ = maxPerUser;
    reaper_ = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "flight-sql-loopback-reaper");
      t.setDaemon(true);
      return t;
    });
    long period = Math.max(1000, Math.min(60000, idleTimeoutMs / 2));
    reaper_.scheduleWithFixedDelay(() -> reapIdle(System.currentTimeMillis()), period,
        period, TimeUnit.MILLISECONDS);
  }

  /** A session of 'user' for exclusive use; give it back with release(). */
  LoopbackSession borrow(String user) throws LoopbackException {
    List<LoopbackSession> broken = new ArrayList<>();
    try {
      synchronized (this) {
        if (closed_) throw new LoopbackException("Flight SQL endpoint is shutting down");
        Deque<LoopbackSession> idle = idle_.get(user);
        while (idle != null && !idle.isEmpty()) {
          LoopbackSession s = idle.pollFirst();
          if (!s.isBroken()) return s;
          broken.add(s);
          decrement(user);
        }
        int n = open_.getOrDefault(user, 0);
        if (n >= maxPerUser_) {
          throw new TooManySessionsException("Too many concurrent Flight SQL calls of "
              + user + " (limit " + maxPerUser_ + ")");
        }
        open_.put(user, n + 1);
      }
    } finally {
      broken.forEach(LoopbackSession::close);
    }
    try {
      return opener_.open(user);
    } catch (LoopbackException | RuntimeException e) {
      synchronized (this) { decrement(user); }
      throw e;
    }
  }

  /** Gives a borrowed session back; broken ones (and all after close()) are closed. */
  void release(LoopbackSession session) {
    boolean keep;
    synchronized (this) {
      keep = !closed_ && !session.isBroken();
      if (keep) {
        session.touch();
        idle_.computeIfAbsent(session.user, u -> new ArrayDeque<>()).addFirst(session);
      } else {
        decrement(session.user);
      }
    }
    if (!keep) session.close();
  }

  /** Closes sessions idle for longer than the idle timeout; returns how many. */
  @VisibleForTesting
  int reapIdle(long nowMs) {
    List<LoopbackSession> expired = new ArrayList<>();
    synchronized (this) {
      for (Iterator<Map.Entry<String, Deque<LoopbackSession>>> it =
               idle_.entrySet().iterator(); it.hasNext();) {
        Map.Entry<String, Deque<LoopbackSession>> e = it.next();
        for (Iterator<LoopbackSession> s = e.getValue().iterator(); s.hasNext();) {
          LoopbackSession session = s.next();
          if (nowMs - session.lastUsedMs() >= idleTimeoutMs_) {
            s.remove();
            expired.add(session);
            decrement(e.getKey());
          }
        }
        if (e.getValue().isEmpty()) it.remove();
      }
    }
    expired.forEach(LoopbackSession::close);
    if (!expired.isEmpty()) {
      LOG.debug("Closed {} idle HS2 loopback sessions", expired.size());
    }
    return expired.size();
  }

  /** Sessions of 'user', idle or in use. */
  @VisibleForTesting
  synchronized int openSessions(String user) { return open_.getOrDefault(user, 0); }

  @VisibleForTesting
  synchronized int idleSessions(String user) {
    Deque<LoopbackSession> idle = idle_.get(user);
    return idle == null ? 0 : idle.size();
  }

  private void decrement(String user) {
    int n = open_.getOrDefault(user, 0) - 1;
    if (n > 0) open_.put(user, n); else open_.remove(user);
  }

  @Override
  public void close() {
    List<LoopbackSession> all = new ArrayList<>();
    synchronized (this) {
      closed_ = true;
      idle_.values().forEach(all::addAll);
      idle_.clear();
      for (LoopbackSession s : all) decrement(s.user);
    }
    reaper_.shutdownNow();
    all.forEach(LoopbackSession::close);
  }
}
