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

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.security.sasl.Sasl;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.hive.service.rpc.thrift.TCancelOperationReq;
import org.apache.hive.service.rpc.thrift.TCloseSessionReq;
import org.apache.hive.service.rpc.thrift.TExecuteStatementReq;
import org.apache.hive.service.rpc.thrift.TExecuteStatementResp;
import org.apache.hive.service.rpc.thrift.TFetchOrientation;
import org.apache.hive.service.rpc.thrift.TFetchResultsReq;
import org.apache.hive.service.rpc.thrift.TFetchResultsResp;
import org.apache.hive.service.rpc.thrift.TGetColumnsReq;
import org.apache.hive.service.rpc.thrift.TGetColumnsResp;
import org.apache.hive.service.rpc.thrift.TGetCrossReferenceReq;
import org.apache.hive.service.rpc.thrift.TGetCrossReferenceResp;
import org.apache.hive.service.rpc.thrift.TGetOperationStatusReq;
import org.apache.hive.service.rpc.thrift.TGetOperationStatusResp;
import org.apache.hive.service.rpc.thrift.TGetPrimaryKeysReq;
import org.apache.hive.service.rpc.thrift.TGetPrimaryKeysResp;
import org.apache.hive.service.rpc.thrift.TGetResultSetMetadataReq;
import org.apache.hive.service.rpc.thrift.TGetResultSetMetadataResp;
import org.apache.hive.service.rpc.thrift.TGetSchemasReq;
import org.apache.hive.service.rpc.thrift.TGetSchemasResp;
import org.apache.hive.service.rpc.thrift.TGetTableTypesReq;
import org.apache.hive.service.rpc.thrift.TGetTableTypesResp;
import org.apache.hive.service.rpc.thrift.TGetTablesReq;
import org.apache.hive.service.rpc.thrift.TGetTablesResp;
import org.apache.hive.service.rpc.thrift.TGetTypeInfoReq;
import org.apache.hive.service.rpc.thrift.TGetTypeInfoResp;
import org.apache.hive.service.rpc.thrift.TOpenSessionReq;
import org.apache.hive.service.rpc.thrift.TOpenSessionResp;
import org.apache.hive.service.rpc.thrift.TOperationHandle;
import org.apache.hive.service.rpc.thrift.TOperationState;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.apache.hive.service.rpc.thrift.TSessionHandle;
import org.apache.hive.service.rpc.thrift.TStatus;
import org.apache.hive.service.rpc.thrift.TStatusCode;
import org.apache.hive.service.rpc.thrift.TTableSchema;
import org.apache.impala.thrift.ImpalaHiveServer2Service;
import org.apache.impala.thrift.TCloseImpalaOperationReq;
import org.apache.impala.thrift.TCloseImpalaOperationResp;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One connection to the coordinator's HS2 port with one HS2 session, opened for a Flight
 * user (see {@link LoopbackConfig}). Not thread-safe: used by one call at a time
 * ({@link LoopbackSessions} hands it out exclusively).
 */
final class LoopbackSession implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(LoopbackSession.class);

  static final String DOAS_KEY = "impala.doas.user";
  private static final int FETCH_ROWS = 1024;

  final String user;
  private final TTransport transport_;
  private final ImpalaHiveServer2Service.Client client_;
  private final TSessionHandle session_;
  private volatile boolean broken_;
  private volatile long lastUsedMs_ = System.currentTimeMillis();

  private LoopbackSession(String user, TTransport transport,
      ImpalaHiveServer2Service.Client client, TSessionHandle session) {
    this.user = user;
    transport_ = transport;
    client_ = client;
    session_ = session;
  }

  /**
   * Connects (TLS with 'tls' if set, Kerberos SASL with 'login' if the config has a
   * principal) and opens an HS2 session for 'user'.
   */
  static LoopbackSession open(LoopbackConfig config, LoopbackLogin login,
      SSLSocketFactory tls, String user) throws LoopbackException {
    TTransport transport = null;
    try {
      transport = connect(config, login, tls);
      ImpalaHiveServer2Service.Client client =
          new ImpalaHiveServer2Service.Client(new TBinaryProtocol(transport));
      TOpenSessionReq req =
          new TOpenSessionReq(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6);
      Map<String, String> sessionConf = new HashMap<>();
      if (config.doAs) {
        sessionConf.put(DOAS_KEY, user);
      } else {
        req.setUsername(user);
      }
      req.setConfiguration(sessionConf);
      TOpenSessionResp resp = client.OpenSession(req);
      check(resp.getStatus());
      LOG.debug("HS2 loopback session opened for {}", user);
      return new LoopbackSession(user, transport, client, resp.getSessionHandle());
    } catch (LoopbackException e) {
      closeQuietly(transport);
      throw e;
    } catch (Exception e) {
      closeQuietly(transport);
      throw new LoopbackException("Cannot open an HS2 session on " + config.host + ":"
          + config.port + ": " + e, true, e);
    }
  }

  private static TTransport connect(LoopbackConfig config, LoopbackLogin login,
      SSLSocketFactory tls) throws Exception {
    InetSocketAddress address = new InetSocketAddress(config.host, config.port);
    Socket socket;
    if (tls != null) {
      SSLSocket ssl = (SSLSocket) tls.createSocket();
      ssl.connect(address, config.connectTimeoutMs);
      SSLParameters params = ssl.getSSLParameters();
      // the certificate must match the host name
      params.setEndpointIdentificationAlgorithm("HTTPS");
      ssl.setSSLParameters(params);
      ssl.setSoTimeout(config.socketTimeoutMs);
      ssl.startHandshake();
      socket = ssl;
    } else {
      socket = new Socket();
      socket.connect(address, config.connectTimeoutMs);
    }
    socket.setSoTimeout(config.socketTimeoutMs);
    TTransport plain = new TSocket(socket);
    if (!config.kerberos()) return plain;
    Map<String, String> props = new HashMap<>();
    props.put(Sasl.QOP, "auth");
    props.put(Sasl.SERVER_AUTH, "true");
    TSaslClientTransport sasl = new TSaslClientTransport("GSSAPI", null,
        config.serviceName(), config.principalHost(), props, null, plain);
    login.doAs(() -> {
      sasl.open();
      return null;
    });
    return sasl;
  }

  boolean isBroken() { return broken_; }
  long lastUsedMs() { return lastUsedMs_; }
  void touch() { lastUsedMs_ = System.currentTimeMillis(); }

  /** Starts 'sql'; Impala plans it before returning, so the result schema is known. */
  TOperationHandle execute(String sql) throws LoopbackException {
    try {
      TExecuteStatementResp exec =
          client_.ExecuteStatement(new TExecuteStatementReq(session_, sql));
      check(exec.getStatus());
      return exec.getOperationHandle();
    } catch (TException e) {
      throw broken(e);
    } finally {
      touch();
    }
  }

  /** Result schema of a statement with a result set. */
  TTableSchema resultSchema(TOperationHandle op) throws LoopbackException {
    try {
      TGetResultSetMetadataResp resp =
          client_.GetResultSetMetadata(new TGetResultSetMetadataReq(op));
      check(resp.getStatus());
      return resp.getSchema();
    } catch (TException e) {
      throw broken(e);
    }
  }

  /**
   * Next rows (at most 'maxRows'); blocks until rows are available (Impala answers
   * STILL_EXECUTING after fetch_rows_timeout_ms, then this asks again).
   */
  TFetchResultsResp fetch(TOperationHandle op, int maxRows) throws LoopbackException {
    try {
      while (true) {
        TFetchResultsResp fetch = client_.FetchResults(
            new TFetchResultsReq(op, TFetchOrientation.FETCH_NEXT, maxRows));
        if (fetch.getStatus().getStatusCode() != TStatusCode.STILL_EXECUTING_STATUS) {
          check(fetch.getStatus());
          return fetch;
        }
      }
    } catch (TException e) {
      throw broken(e);
    } finally {
      touch();
    }
  }

  /** Waits until a statement without a result set (DDL, DML) has finished. */
  void waitForCompletion(TOperationHandle op) throws LoopbackException {
    long sleepMs = 10;
    try {
      while (true) {
        TGetOperationStatusResp status =
            client_.GetOperationStatus(new TGetOperationStatusReq(op));
        check(status.getStatus());
        TOperationState state = status.getOperationState();
        if (state == TOperationState.FINISHED_STATE) return;
        if (state == TOperationState.ERROR_STATE
            || state == TOperationState.CANCELED_STATE
            || state == TOperationState.CLOSED_STATE) {
          throw new LoopbackException(status.isSetErrorMessage()
              ? status.getErrorMessage() : "Statement " + state.name());
        }
        Thread.sleep(sleepMs);
        sleepMs = Math.min(sleepMs * 2, 500);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new LoopbackException("Interrupted while waiting for the statement");
    } catch (TException e) {
      throw broken(e);
    } finally {
      touch();
    }
  }

  // ----------------------------------------------------------------------- metadata

  /** HS2 metadata request returning an operation handle. */
  interface MetadataCall {
    TOperationHandle start(ImpalaHiveServer2Service.Client client, TSessionHandle session)
        throws TException, LoopbackException;
  }

  static MetadataCall schemas(String schemaPattern) {
    return (client, session) -> {
      TGetSchemasReq req = new TGetSchemasReq(session);
      req.setSchemaName(orAll(schemaPattern));
      TGetSchemasResp resp = client.GetSchemas(req);
      check(resp.getStatus());
      return resp.getOperationHandle();
    };
  }

  static MetadataCall tables(String schemaPattern, String tablePattern,
      List<String> tableTypes) {
    return (client, session) -> {
      TGetTablesReq req = new TGetTablesReq(session);
      req.setSchemaName(orAll(schemaPattern));
      req.setTableName(orAll(tablePattern));
      if (tableTypes != null && !tableTypes.isEmpty()) req.setTableTypes(tableTypes);
      TGetTablesResp resp = client.GetTables(req);
      check(resp.getStatus());
      return resp.getOperationHandle();
    };
  }

  static MetadataCall columns(String schemaPattern, String tablePattern) {
    return (client, session) -> {
      TGetColumnsReq req = new TGetColumnsReq(session);
      req.setSchemaName(orAll(schemaPattern));
      req.setTableName(orAll(tablePattern));
      req.setColumnName("%");
      TGetColumnsResp resp = client.GetColumns(req);
      check(resp.getStatus());
      return resp.getOperationHandle();
    };
  }

  static MetadataCall tableTypes() {
    return (client, session) -> {
      TGetTableTypesResp resp = client.GetTableTypes(new TGetTableTypesReq(session));
      check(resp.getStatus());
      return resp.getOperationHandle();
    };
  }

  static MetadataCall typeInfo() {
    return (client, session) -> {
      TGetTypeInfoResp resp = client.GetTypeInfo(new TGetTypeInfoReq(session));
      check(resp.getStatus());
      return resp.getOperationHandle();
    };
  }

  static MetadataCall primaryKeys(String schema, String table) {
    return (client, session) -> {
      TGetPrimaryKeysReq req = new TGetPrimaryKeysReq(session);
      if (schema != null) req.setSchemaName(schema);
      req.setTableName(table);
      TGetPrimaryKeysResp resp = client.GetPrimaryKeys(req);
      check(resp.getStatus());
      return resp.getOperationHandle();
    };
  }

  /** Foreign keys; either side may be null (exported / imported keys). */
  static MetadataCall crossReference(String parentSchema, String parentTable,
      String foreignSchema, String foreignTable) {
    return (client, session) -> {
      TGetCrossReferenceReq req = new TGetCrossReferenceReq(session);
      if (parentSchema != null) req.setParentSchemaName(parentSchema);
      if (parentTable != null) req.setParentTableName(parentTable);
      if (foreignSchema != null) req.setForeignSchemaName(foreignSchema);
      if (foreignTable != null) req.setForeignTableName(foreignTable);
      TGetCrossReferenceResp resp = client.GetCrossReference(req);
      check(resp.getStatus());
      return resp.getOperationHandle();
    };
  }

  private static String orAll(String pattern) {
    return pattern == null || pattern.isEmpty() ? "%" : pattern;
  }

  /**
   * Runs a metadata request and returns all rows as lower-case column name -> value
   * (strings as String). Metadata results are small, so they are collected.
   */
  List<Map<String, Object>> metadata(MetadataCall call, BufferAllocator allocator,
      int fetchRows) throws LoopbackException {
    TOperationHandle op;
    try {
      op = call.start(client_, session_);
    } catch (TException e) {
      throw broken(e);
    }
    try {
      Schema schema = HiveRowSetConverter.arrowSchema(resultSchema(op));
      List<Map<String, Object>> rows = new ArrayList<>();
      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        while (true) {
          TFetchResultsResp fetch = fetch(op, fetchRows);
          int n = HiveRowSetConverter.fill(root, fetch.getResults());
          for (int r = 0; r < n; r++) {
            Map<String, Object> row = new HashMap<>();
            for (FieldVector vector : root.getFieldVectors()) {
              Object value = vector.getObject(r);
              row.put(vector.getName().toLowerCase(Locale.ROOT),
                  value instanceof Text ? value.toString() : value);
            }
            rows.add(row);
          }
          if (!fetch.isHasMoreRows() || n == 0) return rows;
        }
      }
    } finally {
      try {
        closeOperation(op);
      } catch (LoopbackException e) {
        LOG.debug("Closing an HS2 metadata operation failed: {}", e.getMessage());
      }
    }
  }

  void cancel(TOperationHandle op) {
    try {
      client_.CancelOperation(new TCancelOperationReq(op));
    } catch (TException e) {
      broken_ = true;
      LOG.debug("Cancelling an HS2 loopback operation failed: {}", e.toString());
    }
  }

  /**
   * Closes the operation; returns the rows modified by a DML statement (sum over
   * partitions from Impala's CloseImpalaOperation), or -1 when there is none.
   */
  long closeOperation(TOperationHandle op) throws LoopbackException {
    try {
      TCloseImpalaOperationResp resp =
          client_.CloseImpalaOperation(new TCloseImpalaOperationReq(op));
      check(resp.getStatus());
      if (!resp.isSetDml_result() || !resp.getDml_result().isSetRows_modified()) {
        return -1;
      }
      long rows = 0;
      for (long n : resp.getDml_result().getRows_modified().values()) rows += n;
      return rows;
    } catch (TException e) {
      throw broken(e);
    } finally {
      touch();
    }
  }

  private LoopbackException broken(TException e) {
    broken_ = true;
    return new LoopbackException("HS2 loopback call failed: " + e, true, e);
  }

  static void check(TStatus status) throws LoopbackException {
    TStatusCode code = status.getStatusCode();
    if (code == TStatusCode.SUCCESS_STATUS
        || code == TStatusCode.SUCCESS_WITH_INFO_STATUS) {
      return;
    }
    String message = status.isSetErrorMessage() ? status.getErrorMessage() : code.name();
    throw new LoopbackException(message);
  }

  @Override
  public void close() {
    try {
      if (!broken_) client_.CloseSession(new TCloseSessionReq(session_));
    } catch (TException e) {
      LOG.debug("Closing the HS2 loopback session of {} failed: {}", user, e.toString());
    } finally {
      closeQuietly(transport_);
    }
  }

  private static void closeQuietly(TTransport transport) {
    if (transport != null) transport.close();
  }
}
