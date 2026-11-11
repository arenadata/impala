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

import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hive.service.rpc.thrift.TBoolColumn;
import org.apache.hive.service.rpc.thrift.TCancelOperationResp;
import org.apache.hive.service.rpc.thrift.TCloseOperationResp;
import org.apache.hive.service.rpc.thrift.TCloseSessionResp;
import org.apache.hive.service.rpc.thrift.TColumn;
import org.apache.hive.service.rpc.thrift.TColumnDesc;
import org.apache.hive.service.rpc.thrift.TExecuteStatementReq;
import org.apache.hive.service.rpc.thrift.TExecuteStatementResp;
import org.apache.hive.service.rpc.thrift.TFetchResultsReq;
import org.apache.hive.service.rpc.thrift.TFetchResultsResp;
import org.apache.hive.service.rpc.thrift.TGetOperationStatusResp;
import org.apache.hive.service.rpc.thrift.TGetResultSetMetadataReq;
import org.apache.hive.service.rpc.thrift.TGetResultSetMetadataResp;
import org.apache.hive.service.rpc.thrift.THandleIdentifier;
import org.apache.hive.service.rpc.thrift.TI32Column;
import org.apache.hive.service.rpc.thrift.TOpenSessionReq;
import org.apache.hive.service.rpc.thrift.TOpenSessionResp;
import org.apache.hive.service.rpc.thrift.TOperationHandle;
import org.apache.hive.service.rpc.thrift.TOperationState;
import org.apache.hive.service.rpc.thrift.TOperationType;
import org.apache.hive.service.rpc.thrift.TPrimitiveTypeEntry;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.apache.hive.service.rpc.thrift.TRowSet;
import org.apache.hive.service.rpc.thrift.TSessionHandle;
import org.apache.hive.service.rpc.thrift.TStatus;
import org.apache.hive.service.rpc.thrift.TStatusCode;
import org.apache.hive.service.rpc.thrift.TStringColumn;
import org.apache.hive.service.rpc.thrift.TTableSchema;
import org.apache.hive.service.rpc.thrift.TTypeDesc;
import org.apache.hive.service.rpc.thrift.TTypeEntry;
import org.apache.hive.service.rpc.thrift.TTypeId;
import org.apache.impala.thrift.ImpalaHiveServer2Service;
import org.apache.impala.thrift.TCloseImpalaOperationReq;
import org.apache.impala.thrift.TCloseImpalaOperationResp;
import org.apache.impala.thrift.TDmlResult;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;

/**
 * Fake coordinator HS2 (ImpalaHiveServer2Service) for the Flight SQL tests: plain socket,
 * no SASL. Statements are scripted with on(); unknown statements return one string row
 * (user(), effective user) like "select user(), effective_user()" on Impala.
 */
final class FakeHs2 implements AutoCloseable {

  /** Scripted result of a statement. */
  static final class Script {
    TTableSchema schema; // null: no result set
    List<TRowSet> pages = new ArrayList<>();
    String executeError;
    String fetchError;
    Map<String, Long> dmlRows; // CloseImpalaOperation result for DML

    Script schema(TColumnDesc... columns) {
      schema = new TTableSchema(Arrays.asList(columns));
      return this;
    }

    Script page(TColumn... columns) {
      TRowSet rows = new TRowSet(0, Collections.emptyList());
      rows.setColumns(Arrays.asList(columns));
      pages.add(rows);
      return this;
    }
  }

  private static final class Operation {
    final Script script;
    int nextPage;
    Operation(Script script) { this.script = script; }
  }

  final List<TOpenSessionReq> opened = new CopyOnWriteArrayList<>();
  final List<String> statements = new CopyOnWriteArrayList<>();
  final AtomicInteger closedSessions = new AtomicInteger();
  final AtomicInteger closedOperations = new AtomicInteger();
  final AtomicInteger cancelledOperations = new AtomicInteger();
  volatile String openError;
  private final Map<String, Script> scripts_ = new ConcurrentHashMap<>();
  /** Metadata method name (GetTables, ...) -> script. */
  private final Map<String, Script> metadata_ = new ConcurrentHashMap<>();
  /** Metadata requests received, by method name. */
  final Map<String, List<Object>> metadataRequests = new ConcurrentHashMap<>();
  private final Map<Integer, Operation> operations_ = new ConcurrentHashMap<>();
  private final AtomicInteger nextOperation_ = new AtomicInteger();
  private final TServer server_;
  final int port;

  FakeHs2() throws Exception {
    ImpalaHiveServer2Service.Iface iface = (ImpalaHiveServer2Service.Iface)
        Proxy.newProxyInstance(getClass().getClassLoader(),
            new Class<?>[] {ImpalaHiveServer2Service.Iface.class},
            (proxy, method, args) -> call(method.getName(), args));
    TServerSocket socket = new TServerSocket(0);
    port = socket.getServerSocket().getLocalPort();
    server_ = new TThreadPoolServer(new TThreadPoolServer.Args(socket)
        .processor(new ImpalaHiveServer2Service.Processor<>(iface)).minWorkerThreads(2));
    Thread t = new Thread(server_::serve, "fake-hs2");
    t.setDaemon(true);
    t.start();
    while (!server_.isServing()) Thread.sleep(10);
  }

  Script on(String sql) {
    Script script = new Script();
    scripts_.put(sql, script);
    return script;
  }

  /** Script for a metadata call (GetSchemas, GetTables, GetColumns, ...). */
  Script onMetadata(String method) {
    Script script = new Script();
    metadata_.put(method, script);
    return script;
  }

  static TColumn ints(Integer... values) {
    byte[] nulls = new byte[(values.length + 7) / 8];
    List<Integer> list = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      if (values[i] == null) nulls[i / 8] |= (byte) (1 << (i % 8));
      list.add(values[i] == null ? 0 : values[i]);
    }
    return TColumn.i32Val(new TI32Column(list, ByteBuffer.wrap(nulls)));
  }

  static TColumn bools(Boolean... values) {
    byte[] nulls = new byte[(values.length + 7) / 8];
    List<Boolean> list = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      if (values[i] == null) nulls[i / 8] |= (byte) (1 << (i % 8));
      list.add(values[i] != null && values[i]);
    }
    return TColumn.boolVal(new TBoolColumn(list, ByteBuffer.wrap(nulls)));
  }

  static TColumnDesc column(String name, TTypeId type) {
    return new TColumnDesc(name, new TTypeDesc(Collections.singletonList(
        TTypeEntry.primitiveEntry(new TPrimitiveTypeEntry(type)))), 1);
  }

  static TColumn strings(String... values) {
    byte[] nulls = new byte[(values.length + 7) / 8];
    List<String> list = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      if (values[i] == null) nulls[i / 8] |= (byte) (1 << (i % 8));
      list.add(values[i] == null ? "" : values[i]);
    }
    return TColumn.stringVal(new TStringColumn(list, ByteBuffer.wrap(nulls)));
  }

  private static TStatus ok() { return new TStatus(TStatusCode.SUCCESS_STATUS); }

  private static TStatus error(String message) {
    TStatus status = new TStatus(TStatusCode.ERROR_STATUS);
    status.setErrorMessage(message);
    return status;
  }

  private static THandleIdentifier id(int n) {
    ByteBuffer guid = ByteBuffer.allocate(16).putInt(0, n);
    return new THandleIdentifier(guid, ByteBuffer.wrap(new byte[16]));
  }

  private Operation operation(TOperationHandle handle) {
    return operations_.get(handle.getOperationId().bufferForGuid().getInt(0));
  }

  private Script scriptFor(String sql) {
    Script script = scripts_.get(sql);
    if (script != null) return script;
    // default: one row (connected user, effective user) of the last session
    TOpenSessionReq last = opened.get(opened.size() - 1);
    Map<String, String> conf = last.getConfiguration();
    String effective = conf != null && conf.containsKey(LoopbackSession.DOAS_KEY)
        ? conf.get(LoopbackSession.DOAS_KEY) : last.getUsername();
    return new Script().schema(column("user()", TTypeId.STRING_TYPE),
        column("effective_user()", TTypeId.STRING_TYPE))
        .page(strings("impala/host@REALM"), strings(effective));
  }

  private Object call(String method, Object[] args) throws TException {
    switch (method) {
      case "OpenSession": {
        opened.add((TOpenSessionReq) args[0]);
        if (openError != null) {
          return new TOpenSessionResp(
              error(openError), TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6);
        }
        TOpenSessionResp resp =
            new TOpenSessionResp(ok(), TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6);
        resp.setSessionHandle(new TSessionHandle(id(-1)));
        return resp;
      }
      case "ExecuteStatement": {
        String sql = ((TExecuteStatementReq) args[0]).getStatement();
        statements.add(sql);
        // the client sees a TApplicationException, like a broken connection
        if ("break".equals(sql)) throw new TException("connection broken");
        Script script = scriptFor(sql);
        if (script.executeError != null) {
          return new TExecuteStatementResp(error(script.executeError));
        }
        int n = nextOperation_.incrementAndGet();
        operations_.put(n, new Operation(script));
        TExecuteStatementResp exec = new TExecuteStatementResp(ok());
        exec.setOperationHandle(new TOperationHandle(
            id(n), TOperationType.EXECUTE_STATEMENT, script.schema != null));
        return exec;
      }
      case "GetResultSetMetadata": {
        Operation op =
            operation(((TGetResultSetMetadataReq) args[0]).getOperationHandle());
        TGetResultSetMetadataResp resp = new TGetResultSetMetadataResp(ok());
        resp.setSchema(op.script.schema);
        return resp;
      }
      case "FetchResults": {
        Operation op = operation(((TFetchResultsReq) args[0]).getOperationHandle());
        if (op.script.fetchError != null) {
          return new TFetchResultsResp(error(op.script.fetchError));
        }
        TFetchResultsResp fetch = new TFetchResultsResp(ok());
        if (op.nextPage < op.script.pages.size()) {
          fetch.setResults(op.script.pages.get(op.nextPage++));
        } else {
          fetch.setResults(new TRowSet(0, Collections.emptyList()));
        }
        fetch.setHasMoreRows(op.nextPage < op.script.pages.size());
        return fetch;
      }
      case "GetOperationStatus": {
        TGetOperationStatusResp resp = new TGetOperationStatusResp(ok());
        resp.setOperationState(TOperationState.FINISHED_STATE);
        return resp;
      }
      case "CancelOperation":
        cancelledOperations.incrementAndGet();
        return new TCancelOperationResp(ok());
      case "CloseOperation":
        closedOperations.incrementAndGet();
        return new TCloseOperationResp(ok());
      case "CloseImpalaOperation": {
        closedOperations.incrementAndGet();
        Operation op =
            operation(((TCloseImpalaOperationReq) args[0]).getOperationHandle());
        TCloseImpalaOperationResp resp = new TCloseImpalaOperationResp(ok());
        if (op != null && op.script.dmlRows != null) {
          resp.setDml_result(new TDmlResult(op.script.dmlRows));
        }
        return resp;
      }
      case "CloseSession":
        closedSessions.incrementAndGet();
        return new TCloseSessionResp(ok());
      case "GetSchemas":
      case "GetTables":
      case "GetColumns":
      case "GetTableTypes":
      case "GetTypeInfo":
      case "GetPrimaryKeys":
      case "GetCrossReference":
        return metadataCall(method, args[0]);
      default:
        throw new TException("fake HS2 does not implement " + method);
    }
  }

  /** Starts a scripted metadata operation; the response type matches the method. */
  private Object metadataCall(String method, Object request) throws TException {
    metadataRequests.computeIfAbsent(method, m -> new CopyOnWriteArrayList<>())
        .add(request);
    Script script = metadata_.get(method);
    if (script == null) script = new Script().schema(column("X", TTypeId.STRING_TYPE));
    TStatus status = script.executeError != null ? error(script.executeError) : ok();
    TOperationHandle handle = null;
    if (script.executeError == null) {
      int n = nextOperation_.incrementAndGet();
      operations_.put(n, new Operation(script));
      handle = new TOperationHandle(id(n), TOperationType.EXECUTE_STATEMENT, true);
    }
    try {
      Object resp =
          Class.forName("org.apache.hive.service.rpc.thrift.T" + method + "Resp")
              .getConstructor(TStatus.class).newInstance(status);
      if (handle != null) {
        resp.getClass().getMethod("setOperationHandle", TOperationHandle.class)
            .invoke(resp, handle);
      }
      return resp;
    } catch (ReflectiveOperationException e) {
      throw new TException(e);
    }
  }

  @Override
  public void close() { server_.stop(); }
}
