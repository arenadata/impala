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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Arrow Flight SQL endpoint of a coordinator, running in the impalad JVM. Settings:
 * {@link FlightSqlConfig} (impalad's --flight_sql_* flags and the HS2 TLS, Kerberos
 * and LDAP flags). Authentication is required: see {@link ImpalaFlightAuthenticator}.
 */
public class ImpalaFlightSqlServer implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ImpalaFlightSqlServer.class);

  // The endpoint started by startIfConfigured(), closed by the shutdown hook.
  private static ImpalaFlightSqlServer instance_;

  private final BufferAllocator allocator_;
  private final LoopbackSessions sessions_;
  private final QueryRegistry queries_;
  private final PreparedRegistry prepared_;
  private final FlightServer server_;
  private final boolean tls_;

  /**
   * Starts the endpoint when --flight_sql_port is set. Never throws: impalad keeps
   * running without the endpoint if it cannot start (the error is logged).
   */
  public static synchronized void startIfConfigured(FlightSqlConfig config) {
    if (instance_ != null) return;
    if (config.port <= 0) return;
    try {
      instance_ = start(config);
      Runtime.getRuntime().addShutdownHook(
          new Thread(ImpalaFlightSqlServer::stopRunning, "flight-sql-shutdown"));
      LOG.info("Flight SQL endpoint listening on {}:{} ({}, authentication: {})",
          config.bindHost, instance_.getPort(), instance_.tls_ ? "TLS" : "plaintext",
          config.authMethods);
      if (config.authMethods.trim().equalsIgnoreCase("none")) {
        LOG.warn("Flight SQL endpoint has no authentication ({}=none): any client can "
            + "run statements as any user", FlightSqlConfig.AUTH_METHODS_FLAG);
      }
    } catch (Exception e) {
      LOG.error("Failed to start the Flight SQL endpoint on port " + config.port, e);
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
   * Starts an endpoint; port 0 picks a free port (see getPort()).
   */
  public static ImpalaFlightSqlServer start(FlightSqlConfig config) throws IOException {
    return new ImpalaFlightSqlServer(config);
  }

  private ImpalaFlightSqlServer(FlightSqlConfig config) throws IOException {
    tls_ = config.tls();
    File certificate = tls_
        ? requiredFile(config.sslCertificate, "--ssl_server_certificate") : null;
    File privateKey = tls_
        ? requiredFile(config.sslPrivateKey, "--ssl_private_key") : null;
    // misconfigured authentication fails here, before anything listens
    CallHeaderAuthenticator authenticator = ImpalaFlightAuthenticator.create(config);
    sessions_ = LoopbackSessions.create(new LoopbackConfig(config));
    allocator_ = new RootAllocator(config.memoryLimitBytes);
    queries_ = new QueryRegistry(sessions_, config.ticketTtlMs);
    // a query started at prepare time keeps its result as long as an unfetched ticket
    prepared_ = new PreparedRegistry(
        s -> ImpalaFlightSqlProducer.discard(sessions_, s), config.ticketTtlMs,
        config.preparedIdleMs);
    try {
      Location location = tls_ ? Location.forGrpcTls(config.bindHost, config.port)
                               : Location.forGrpcInsecure(config.bindHost, config.port);
      FlightServer.Builder builder = FlightServer.builder(
          allocator_, location, new ImpalaFlightSqlProducer(impalaVersion(), sessions_,
          queries_, prepared_, allocator_, config.fetchRows))
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

  /** A file of the TLS pair: both or neither of the flags must be set. */
  private static File requiredFile(String path, String flag) {
    Preconditions.checkArgument(!path.isEmpty(),
        "%s must be set for TLS (--ssl_server_certificate and --ssl_private_key go "
            + "together)", flag);
    File file = new File(path);
    Preconditions.checkArgument(file.canRead(), "%s: cannot read %s", flag, path);
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
