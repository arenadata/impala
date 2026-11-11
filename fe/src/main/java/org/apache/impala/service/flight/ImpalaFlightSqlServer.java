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
import java.util.concurrent.TimeUnit;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Arrow Flight SQL endpoint of a coordinator, running in the impalad JVM.
 *
 * Configured through Hadoop configuration keys (core-site.xml in the impalad conf
 * directory), not gflags, so the backend is not involved:
 * <ul>
 *   <li>{@value #PORT}: listening port; 0 (default) disables the endpoint</li>
 *   <li>{@value #BIND_HOST}: listening address, default all interfaces</li>
 *   <li>{@value #SSL_ENABLED}: TLS, default true; then {@value #SSL_CERTIFICATE} (PEM
 *   certificate chain) and {@value #SSL_PRIVATE_KEY} (PEM PKCS#8 key) are required</li>
 *   <li>{@value #MEMORY_LIMIT}: limit of the Arrow allocator</li>
 *   <li>{@value #FETCH_ROWS}: rows per HS2 fetch and Arrow batch; {@value #TICKET_TTL_S}:
 *   seconds a started statement waits for its DoGet (also a prepared query's first
 *   result); {@value #PREPARED_IDLE_S}: seconds an unused prepared statement is kept</li>
 *   <li>authentication: see {@link ImpalaFlightAuthenticator} (required; the endpoint
 *   does not start without it)</li>
 * </ul>
 */
public class ImpalaFlightSqlServer implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ImpalaFlightSqlServer.class);

  public static final String PORT = "impala.flight-sql.port";
  public static final String BIND_HOST = "impala.flight-sql.bind-host";
  public static final String SSL_ENABLED = "impala.flight-sql.ssl.enabled";
  public static final String SSL_CERTIFICATE = "impala.flight-sql.ssl.certificate";
  public static final String SSL_PRIVATE_KEY = "impala.flight-sql.ssl.private-key";
  public static final String MEMORY_LIMIT = "impala.flight-sql.memory.limit-bytes";
  public static final String FETCH_ROWS = "impala.flight-sql.fetch-rows";
  public static final String TICKET_TTL_S = "impala.flight-sql.ticket-ttl-s";
  public static final String PREPARED_IDLE_S = "impala.flight-sql.prepared-idle-s";

  static final String DEFAULT_BIND_HOST = "0.0.0.0";
  static final long DEFAULT_MEMORY_LIMIT = 256L << 20;

  // The endpoint started by startIfConfigured(), closed by the shutdown hook.
  private static ImpalaFlightSqlServer instance_;

  private final BufferAllocator allocator_;
  private final LoopbackSessions sessions_;
  private final QueryRegistry queries_;
  private final PreparedRegistry prepared_;
  private final FlightServer server_;
  private final boolean tls_;

  /**
   * Starts the endpoint when {@value #PORT} is set. Never throws: impalad keeps running
   * without the endpoint if it cannot start (the error is logged).
   */
  public static synchronized void startIfConfigured(Configuration conf, String hostname,
      String principal) {
    if (instance_ != null) return;
    int port = conf.getInt(PORT, 0);
    if (port <= 0) return;
    try {
      instance_ = start(conf, hostname, principal);
      Runtime.getRuntime().addShutdownHook(
          new Thread(ImpalaFlightSqlServer::stopRunning, "flight-sql-shutdown"));
      LOG.info("Flight SQL endpoint listening on {}:{} ({}, authentication: {})",
          conf.getTrimmed(BIND_HOST, DEFAULT_BIND_HOST), instance_.getPort(),
          instance_.tls_ ? "TLS" : "plaintext",
          conf.getTrimmed(ImpalaFlightAuthenticator.METHODS, ""));
    } catch (Exception e) {
      LOG.error("Failed to start the Flight SQL endpoint on port " + port, e);
    }
  }

  /** The endpoint started by startIfConfigured(), or null. */
  public static synchronized ImpalaFlightSqlServer running() { return instance_; }

  static synchronized void stopRunning() {
    if (instance_ == null) return;
    try {
      instance_.close();
    } catch (Exception e) {
      LOG.warn("Failed to stop the Flight SQL endpoint", e);
    } finally {
      instance_ = null;
    }
  }

  /**
   * Starts an endpoint; port 0 picks a free port (see getPort()). 'hostname' (the
   * coordinator's FQDN) replaces _HOST in the SPNEGO principal.
   */
  public static ImpalaFlightSqlServer start(Configuration conf, String hostname)
      throws IOException {
    return start(conf, hostname, "");
  }

  /** As start(conf, hostname); 'principal' is impalad's -principal (HS2 loopback). */
  public static ImpalaFlightSqlServer start(Configuration conf, String hostname,
      String principal) throws IOException {
    return new ImpalaFlightSqlServer(conf, hostname, principal);
  }

  private ImpalaFlightSqlServer(Configuration conf, String hostname, String principal)
      throws IOException {
    String host = conf.getTrimmed(BIND_HOST, DEFAULT_BIND_HOST);
    int port = conf.getInt(PORT, 0);
    tls_ = conf.getBoolean(SSL_ENABLED, true);
    File certificate = tls_ ? requiredFile(conf, SSL_CERTIFICATE) : null;
    File privateKey = tls_ ? requiredFile(conf, SSL_PRIVATE_KEY) : null;
    // misconfigured authentication fails here, before anything listens
    CallHeaderAuthenticator authenticator =
        ImpalaFlightAuthenticator.create(conf, hostname);
    sessions_ = LoopbackSessions.create(new LoopbackConfig(conf, hostname, principal));
    allocator_ = new RootAllocator(conf.getLong(MEMORY_LIMIT, DEFAULT_MEMORY_LIMIT));
    long ticketTtlMs = conf.getLong(TICKET_TTL_S, 300) * 1000L;
    queries_ = new QueryRegistry(sessions_, ticketTtlMs);
    // a query started at prepare time keeps its result as long as an unfetched ticket
    prepared_ = new PreparedRegistry(
        s -> ImpalaFlightSqlProducer.discard(sessions_, s), ticketTtlMs,
        conf.getLong(PREPARED_IDLE_S, 7200) * 1000L);
    int fetchRows = conf.getInt(FETCH_ROWS, 10000);
    try {
      Location location = tls_ ? Location.forGrpcTls(host, port)
                               : Location.forGrpcInsecure(host, port);
      FlightServer.Builder builder = FlightServer.builder(
          allocator_, location, new ImpalaFlightSqlProducer(
          impalaVersion(), sessions_, queries_, prepared_, allocator_, fetchRows))
          .headerAuthenticator(authenticator);
      if (tls_) builder.useTls(certificate, privateKey);
      server_ = builder.build().start();
    } catch (IOException | RuntimeException e) {
      prepared_.close();
      queries_.close();
      sessions_.close();
      allocator_.close();
      throw e;
    }
  }

  private static File requiredFile(Configuration conf, String key) {
    String path = conf.getTrimmed(key, "");
    Preconditions.checkArgument(!path.isEmpty(),
        "%s must be set when %s is true", key, SSL_ENABLED);
    File file = new File(path);
    Preconditions.checkArgument(file.canRead(), "%s: cannot read %s", key, path);
    return file;
  }

  /** Version of the frontend jar (Implementation-Version), e.g. 4.5.0.4-4.5.0-0. */
  static String impalaVersion() {
    String version = ImpalaFlightSqlServer.class.getPackage().getImplementationVersion();
    return version != null ? version : "unknown";
  }

  public int getPort() { return server_.getPort(); }

  @Override
  public void close() throws InterruptedException {
    try {
      server_.shutdown();
      if (!server_.awaitTermination(5, TimeUnit.SECONDS)) {
        LOG.warn("Flight SQL endpoint did not stop within 5 s, closing it");
      }
      server_.close();
    } finally {
      prepared_.close();
      queries_.close();
      sessions_.close();
      allocator_.close();
    }
  }
}
