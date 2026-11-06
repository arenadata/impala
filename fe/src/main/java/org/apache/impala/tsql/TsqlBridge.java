package org.apache.impala.tsql;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.impala.service.Frontend;
import org.apache.impala.thrift.TSqlCell;
import org.apache.impala.thrift.TSqlFeed;
import org.apache.impala.thrift.TSqlNextRequest;
import org.apache.impala.thrift.TSqlRequest;
import org.apache.impala.thrift.TSqlStartResponse;
import org.apache.impala.thrift.TSqlStep;
import org.apache.impala.thrift.TSqlStepKind;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

/**
 * ADH-TSQL. JNI entry points. The parser and runtime stay free of Thrift;
 * this class is the boundary the coordinator calls.
 */
public final class TsqlBridge {
  private static final AtomicLong IDS = new AtomicLong(1);
  private static final ConcurrentHashMap<Long, TsqlRuntime> RUNTIMES =
      new ConcurrentHashMap<Long, TsqlRuntime>();
  private static volatile Frontend frontend;

  private TsqlBridge() {}

  /** Called once from the coordinator when the frontend is constructed. */
  public static void bindFrontend(Frontend value) {
    frontend = value;
  }

  public static byte[] start(byte[] requestBytes) {
    TSqlStartResponse response = new TSqlStartResponse();
    try {
      TSqlRequest request = new TSqlRequest();
      new TDeserializer(new TBinaryProtocol.Factory())
          .deserialize(request, requestBytes);
      TsqlProcedureStore store = frontend == null || frontend.getMetaStoreClientPool() == null
          ? new TsqlRuntime.MemoryStore()
          : new TsqlHms(frontend.getMetaStoreClientPool());
      TsqlRuntime runtime = new TsqlRuntime(request.source, request.database, request.owner,
          store);
      long id = IDS.getAndIncrement();
      RUNTIMES.put(id, runtime);
      response.setHandle(id);
    } catch (RuntimeException e) {
      response.setHandle(0);
      response.setError_message(e.getMessage());
    } catch (TException e) {
      response.setHandle(0);
      response.setError_message(e.getMessage());
    }
    return serialize(response);
  }

  public static byte[] next(byte[] requestBytes) {
    try {
      TSqlNextRequest request = new TSqlNextRequest();
      new TDeserializer(new TBinaryProtocol.Factory())
          .deserialize(request, requestBytes);
      TsqlRuntime runtime = RUNTIMES.get(request.handle);
      if (runtime == null) {
        return serialize(TsqlRuntime.Step.failed("Unknown T-SQL execution"));
      }
      TsqlSqlRunner.Result previous = toResult(request.feed);
      return serialize(runtime.next(previous));
    } catch (RuntimeException e) {
      return serialize(TsqlRuntime.Step.failed(e.getMessage()));
    } catch (TException e) {
      return serialize(TsqlRuntime.Step.failed(e.getMessage()));
    }
  }

  public static void close(long handle) {
    RUNTIMES.remove(handle);
  }

  private static TsqlSqlRunner.Result toResult(TSqlFeed feed) {
    if (feed == null) return null;
    List<String> columns = feed.column_names == null
        ? new ArrayList<String>() : feed.column_names;
    List<List<TsqlSqlRunner.Cell>> rows = new ArrayList<List<TsqlSqlRunner.Cell>>();
    if (feed.rows != null) {
      for (List<TSqlCell> row : feed.rows) {
        List<TsqlSqlRunner.Cell> cells = new ArrayList<TsqlSqlRunner.Cell>();
        for (TSqlCell cell : row) {
          cells.add(cell.is_null ? TsqlSqlRunner.Cell.nul()
              : TsqlSqlRunner.Cell.of(cell.value));
        }
        rows.add(cells);
      }
    }
    int code = feed.isSetSqlcode() ? feed.sqlcode : 0;
    return new TsqlSqlRunner.Result(columns, rows, code, feed.error_message);
  }

  private static byte[] serialize(TsqlRuntime.Step step) {
    TSqlStep out = new TSqlStep();
    switch (step.kind) {
      case SQL: out.setKind(TSqlStepKind.SQL); break;
      case CLIENT_RESULT: out.setKind(TSqlStepKind.CLIENT_RESULT); break;
      case FINISHED: out.setKind(TSqlStepKind.FINISHED); break;
      case FAILED: out.setKind(TSqlStepKind.FAILED); break;
      default: out.setKind(TSqlStepKind.FAILED); break;
    }
    if (step.sql != null) out.setSql(step.sql);
    if (step.errorMessage != null) out.setError_message(step.errorMessage);
    out.setSqlcode(step.sqlcode);
    if (step.kind == TsqlRuntime.Kind.FINISHED && step.errorMessage != null) {
      List<List<String>> summary = new ArrayList<List<String>>();
      List<String> row = new ArrayList<String>();
      row.add(step.errorMessage);
      summary.add(row);
      out.setSummary_rows(summary);
    }
    if (step.procedureOptions != null) {
      for (Map.Entry<String, String> entry : step.procedureOptions.entrySet()) {
        out.putToProcedure_options(entry.getKey(), entry.getValue());
      }
    }
    return serialize(out);
  }

  private static byte[] serialize(org.apache.thrift.TBase<?, ?> obj) {
    try {
      return new TSerializer(new TBinaryProtocol.Factory()).serialize(obj);
    } catch (TException e) {
      return ByteBuffer.allocate(0).array();
    }
  }
}
