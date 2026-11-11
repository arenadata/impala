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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.arrow.flight.BackpressureStrategy;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.CancelFlightInfoRequest;
import org.apache.arrow.flight.CancelStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.NoOpFlightSqlProducer;
import org.apache.arrow.flight.sql.SqlInfoBuilder;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementResult;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCatalogs;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCrossReference;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetDbSchemas;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetExportedKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetImportedKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetPrimaryKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetSqlInfo;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTableTypes;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetXdbcTypeInfo;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementUpdate;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementUpdate;
import org.apache.arrow.flight.sql.impl.FlightSql.DoPutUpdateResult;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlNullOrdering;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedCaseSensitivity;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedTransaction;
import org.apache.arrow.flight.sql.impl.FlightSql.TicketStatementQuery;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hive.service.rpc.thrift.TFetchResultsResp;
import org.apache.hive.service.rpc.thrift.TOperationHandle;
import org.apache.impala.shaded.flight.com.google.protobuf.Any;
import org.apache.impala.shaded.flight.com.google.protobuf.ByteString;
import org.apache.impala.shaded.flight.com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flight SQL producer of the coordinator endpoint: GetSqlInfo, statements (queries via
 * GetFlightInfo + DoGet, updates via DoPut), cancel, and metadata (catalogs, schemas,
 * tables with schemas, table types, keys, XDBC type info), run as the caller on the HS2
 * loopback, and parameterless prepared statements.
 */
public class ImpalaFlightSqlProducer extends NoOpFlightSqlProducer {
  private static final Logger LOG =
      LoggerFactory.getLogger(ImpalaFlightSqlProducer.class);

  /** Arrow version bundled in impala-arrow-flight (java/shaded-deps/arrow-flight). */
  static final String ARROW_VERSION = "16.0.0";
  private static final Schema EMPTY_SCHEMA = new Schema(Collections.emptyList());
  /** Impala keywords beyond SQL-92 (SQL_KEYWORDS). */
  private static final String[] IMPALA_KEYWORDS = {"CACHED", "COMPUTE", "FORMAT",
      "INCREMENTAL", "INVALIDATE", "KUDU", "LOCATION", "METADATA", "OVERWRITE",
      "PARQUET", "PARTITIONED", "REFRESH", "STATS", "STORED", "TBLPROPERTIES",
      "UNCACHED", "UPSERT"};
  /** Longest wait for a slow client to take the next batch. */
  private static final long CLIENT_WAIT_MS = 300_000;

  private final SqlInfoBuilder sqlInfo_;
  private final LoopbackSessions sessions_;
  private final QueryRegistry queries_;
  private final PreparedRegistry prepared_;
  private final BufferAllocator allocator_;
  private final int fetchRows_;

  public ImpalaFlightSqlProducer(String impalaVersion, LoopbackSessions sessions,
      QueryRegistry queries, PreparedRegistry prepared, BufferAllocator allocator,
      int fetchRows) {
    sessions_ = sessions;
    queries_ = queries;
    prepared_ = prepared;
    allocator_ = allocator;
    fetchRows_ = fetchRows;
    sqlInfo_ = new SqlInfoBuilder()
        .withFlightSqlServerName("Impala")
        .withFlightSqlServerVersion(impalaVersion)
        .withFlightSqlServerArrowVersion(ARROW_VERSION)
        .withFlightSqlServerReadOnly(false)
        .withFlightSqlServerSql(true)
        .withFlightSqlServerSubstrait(false)
        .withFlightSqlServerTransaction(
            SqlSupportedTransaction.SQL_SUPPORTED_TRANSACTION_NONE)
        .withFlightSqlServerCancel(true)
        .withSqlIdentifierQuoteChar("`")
        // Impala lower-cases identifiers, quoted or not, and compares them ignoring case
        .withSqlIdentifierCase(SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_LOWERCASE)
        .withSqlQuotedIdentifierCase(
            SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_CASE_INSENSITIVE)
        // NULLs sort as the largest values (last ascending, first descending)
        .withSqlNullOrdering(SqlNullOrdering.SQL_NULLS_SORTED_HIGH)
        .withSqlDdlCatalog(false)
        .withSqlDdlSchema(true)
        .withSqlDdlTable(true)
        .withSqlAllTablesAreSelectable(false)
        .withSqlSchemaTerm("database")
        .withSqlCatalogTerm("")
        .withSqlProcedureTerm("")
        .withSqlSearchStringEscape("\\")
        .withSqlSupportsColumnAliasing(true)
        .withSqlNullPlusNullIsNull(true)
        .withSqlSupportsExpressionsInOrderBy(true)
        .withSqlSupportsNonNullableColumns(true)
        .withSqlTransactionsSupported(false)
        .withSqlKeywords(IMPALA_KEYWORDS);
  }

  // ------------------------------------------------------------------------ SqlInfo

  @Override
  public FlightInfo getFlightInfoSqlInfo(CommandGetSqlInfo request, CallContext context,
      FlightDescriptor descriptor) {
    // The descriptor command is the packed CommandGetSqlInfo; DoGet with it as the
    // ticket is dispatched back to getStreamSqlInfo().
    Ticket ticket = new Ticket(descriptor.getCommand());
    return new FlightInfo(Schemas.GET_SQL_INFO_SCHEMA, descriptor,
        Collections.singletonList(new FlightEndpoint(ticket)), -1, -1);
  }

  @Override
  public void getStreamSqlInfo(CommandGetSqlInfo command, CallContext context,
      ServerStreamListener listener) {
    sqlInfo_.send(command.getInfoList(), listener);
  }

  // --------------------------------------------------------------------- statements

  /**
   * Starts the statement and returns its schema with a ticket. Statements without a
   * result set (DDL, DML) run to completion here; their stream is empty.
   */
  @Override
  public FlightInfo getFlightInfoStatement(CommandStatementQuery command,
      CallContext context, FlightDescriptor descriptor) {
    String user = context.peerIdentity();
    return register(user, start(user, command.getQuery()), descriptor);
  }

  /**
   * Starts 'sql' as 'user'. With a result set the statement is left running for its
   * DoGet; without one (DDL, DML) it runs to completion and the session is released.
   */
  private StartedStatement start(String user, String sql) {
    LoopbackSession session = borrow(user);
    TOperationHandle op = null;
    try {
      op = session.execute(sql);
      StartedStatement started;
      if (op.isHasResultSet()) {
        started = new StartedStatement(session, op,
            HiveRowSetConverter.arrowSchema(session.resultSchema(op)));
      } else {
        session.waitForCompletion(op);
        session.closeOperation(op);
        sessions_.release(session);
        started = new StartedStatement(null, null, EMPTY_SCHEMA);
      }
      session = null; // handed over (or released)
      op = null;
      return started;
    } catch (LoopbackException | RuntimeException e) {
      throw flightError(e);
    } finally {
      abandon(session, op);
    }
  }

  /** Registers a started statement for its DoGet; returns its FlightInfo. */
  private FlightInfo register(String user, StartedStatement s,
      FlightDescriptor descriptor) {
    String id = queries_.register(user, s.session, s.operation, s.schema);
    return new FlightInfo(s.schema, descriptor,
        Collections.singletonList(new FlightEndpoint(ticket(id))), -1, -1);
  }

  /** Runs an update as 'user'; returns the rows modified (-1 for DDL). */
  private long runUpdate(String user, String sql) throws LoopbackException {
    LoopbackSession session = null;
    TOperationHandle op = null;
    try {
      session = borrow(user);
      op = session.execute(sql);
      session.waitForCompletion(op);
      long rows = session.closeOperation(op);
      op = null;
      return rows;
    } finally {
      abandon(session, op);
    }
  }

  /** Cancels and closes an unused started statement and releases its session. */
  static void discard(LoopbackSessions sessions, StartedStatement s) {
    if (s == null || s.session == null) return;
    try {
      if (s.operation != null) {
        s.session.cancel(s.operation);
        s.session.closeOperation(s.operation);
      }
    } catch (LoopbackException e) {
      LOG.debug("Closing an unused Flight SQL statement failed: {}", e.getMessage());
    } finally {
      sessions.release(s.session);
    }
  }

  /**
   * Schema of a query: Impala plans statements only when it runs them, so a query is
   * started and closed again. Other statements are not run (no side effects): empty
   * schema.
   */
  @Override
  public SchemaResult getSchemaStatement(CommandStatementQuery command,
      CallContext context, FlightDescriptor descriptor) {
    if (!FlightSqlStatements.isQuery(command.getQuery())) {
      return new SchemaResult(EMPTY_SCHEMA);
    }
    LoopbackSession session = borrow(context.peerIdentity());
    TOperationHandle op = null;
    try {
      op = session.execute(command.getQuery());
      Schema schema = op.isHasResultSet()
          ? HiveRowSetConverter.arrowSchema(session.resultSchema(op))
          : EMPTY_SCHEMA;
      session.cancel(op);
      return new SchemaResult(schema);
    } catch (LoopbackException | RuntimeException e) {
      throw flightError(e);
    } finally {
      abandon(session, op);
    }
  }

  @Override
  public void getStreamStatement(TicketStatementQuery ticket, CallContext context,
      ServerStreamListener listener) {
    QueryRegistry.Query q =
        queries_.take(ticket.getStatementHandle().toStringUtf8(), context.peerIdentity());
    if (q == null) {
      listener.error(CallStatus.NOT_FOUND
          .withDescription("Unknown, expired or already fetched Flight SQL ticket")
          .toRuntimeException());
      return;
    }
    BackpressureStrategy backpressure =
        new BackpressureStrategy.CallbackBackpressureStrategy();
    backpressure.register(listener);
    try (BufferAllocator child =
             allocator_.newChildAllocator("flight-sql-" + q.id, 0, Long.MAX_VALUE);
         VectorSchemaRoot root = VectorSchemaRoot.create(q.schema, child)) {
      listener.start(root);
      boolean stopped = false;
      while (q.operation != null && !stopped) {
        TFetchResultsResp fetch = q.session.fetch(q.operation, fetchRows_);
        int rows = HiveRowSetConverter.fill(root, fetch.getResults());
        if (rows > 0) {
          BackpressureStrategy.WaitResult wait =
              backpressure.waitForListener(CLIENT_WAIT_MS);
          if (wait != BackpressureStrategy.WaitResult.READY) {
            LOG.info("Flight SQL stream {} of {} stopped: {}", q.id, q.owner, wait);
            q.session.cancel(q.operation);
            stopped = true;
            continue;
          }
          listener.putNext();
        }
        stopped = !fetch.isHasMoreRows() || rows == 0;
      }
      if (!listener.isCancelled()) listener.completed();
    } catch (LoopbackException | RuntimeException e) {
      if (!listener.isCancelled()) listener.error(flightError(e));
    } finally {
      queries_.finish(q);
    }
  }

  /**
   * Runs an update (DDL / DML) and reports the rows modified (-1 for DDL). The client
   * stream is not read: CommandStatementUpdate carries no parameters, and the Arrow Java
   * client waits for the result without closing its side.
   */
  @Override
  public Runnable acceptPutStatement(CommandStatementUpdate command, CallContext context,
      FlightStream flightStream, StreamListener<PutResult> ackStream) {
    return () -> {
      try {
        sendUpdateCount(ackStream, runUpdate(context.peerIdentity(), command.getQuery()));
      } catch (LoopbackException | RuntimeException e) {
        ackStream.onError(flightError(e));
      }
    };
  }

  /** Cancels a statement whose ticket has not been fetched yet. */
  @Override
  public void cancelFlightInfo(CancelFlightInfoRequest request, CallContext context,
      StreamListener<CancelStatus> listener) {
    try {
      QueryRegistry.Query q = null;
      for (FlightEndpoint endpoint : request.getInfo().getEndpoints()) {
        String id = ticketId(endpoint.getTicket());
        if (id != null) q = queries_.take(id, context.peerIdentity());
        if (q != null) break;
      }
      if (q == null) {
        listener.onNext(CancelStatus.NOT_CANCELLABLE);
      } else {
        if (q.session != null && q.operation != null) q.session.cancel(q.operation);
        queries_.finish(q);
        listener.onNext(CancelStatus.CANCELLED);
      }
      listener.onCompleted();
    } catch (RuntimeException e) {
      listener.onError(flightError(e));
    }
  }

  // ------------------------------------------------------------ prepared statements
  //
  // The Arrow Flight SQL JDBC driver (DBeaver) runs every statement as a prepared
  // statement and takes the update path when the dataset schema is empty. Impala knows a
  // result schema only once a statement runs, so queries start at prepare time and the
  // first execution gets that result; other statements run only when executed (as in
  // Kyuubi). No parameters.

  @Override
  public void createPreparedStatement(ActionCreatePreparedStatementRequest request,
      CallContext context, StreamListener<Result> listener) {
    try {
      String user = context.peerIdentity();
      String sql = request.getQuery();
      boolean isQuery = FlightSqlStatements.isQuery(sql);
      StartedStatement cached = isQuery ? start(user, sql) : null;
      Schema schema = cached != null ? cached.schema : EMPTY_SCHEMA;
      String id = prepared_.register(user, sql, isQuery, schema, cached);
      ActionCreatePreparedStatementResult result =
          ActionCreatePreparedStatementResult.newBuilder()
              .setPreparedStatementHandle(ByteString.copyFromUtf8(id))
              .setDatasetSchema(ByteString.copyFrom(schema.serializeAsMessage()))
              .setParameterSchema(ByteString.copyFrom(EMPTY_SCHEMA.serializeAsMessage()))
              .build();
      listener.onNext(new Result(Any.pack(result).toByteArray()));
      listener.onCompleted();
    } catch (RuntimeException e) {
      listener.onError(flightError(e));
    }
  }

  @Override
  public void closePreparedStatement(ActionClosePreparedStatementRequest request,
      CallContext context, StreamListener<Result> listener) {
    // unknown or already closed handles are fine to close
    prepared_.close(request.getPreparedStatementHandle().toStringUtf8(),
        context.peerIdentity());
    listener.onCompleted();
  }

  private PreparedRegistry.Prepared prepared(ByteString handle, CallContext context) {
    PreparedRegistry.Prepared p =
        prepared_.get(handle.toStringUtf8(), context.peerIdentity());
    if (p == null) {
      throw CallStatus.NOT_FOUND
          .withDescription("Unknown or expired Flight SQL prepared statement")
          .toRuntimeException();
    }
    return p;
  }

  @Override
  public FlightInfo getFlightInfoPreparedStatement(CommandPreparedStatementQuery command,
      CallContext context, FlightDescriptor descriptor) {
    PreparedRegistry.Prepared p = prepared(command.getPreparedStatementHandle(), context);
    PreparedRegistry.Pending pending = p.takePending();
    StartedStatement started;
    long now = System.currentTimeMillis();
    switch (PreparedRegistry.nextExecution(pending, p.isQuery, now)) {
      case USE_CACHED:
        started = pending.cached;
        break;
      case EMPTY_RESULT:
        // an update that just ran through DoPut: empty result, not run again
        started = new StartedStatement(null, null, EMPTY_SCHEMA);
        break;
      default:
        started = start(p.owner, p.sql);
    }
    return register(p.owner, started, descriptor);
  }

  @Override
  public SchemaResult getSchemaPreparedStatement(CommandPreparedStatementQuery command,
      CallContext context, FlightDescriptor descriptor) {
    return new SchemaResult(
        prepared(command.getPreparedStatementHandle(), context).schema);
  }

  /** Update through a prepared statement (DDL / DML, or a query sent as update). */
  @Override
  public Runnable acceptPutPreparedStatementUpdate(
      CommandPreparedStatementUpdate command, CallContext context,
      FlightStream flightStream, StreamListener<PutResult> ackStream) {
    return () -> {
      try {
        // the prepared path completes the client stream: reading it does not block
        rejectParameters(flightStream);
        PreparedRegistry.Prepared p =
            prepared(command.getPreparedStatementHandle(), context);
        // a query sent through the update path: its prepare-time result is not needed
        discard(sessions_, p.takePending().cached);
        long rows = runUpdate(p.owner, p.sql);
        p.setPending(PreparedRegistry.Pending.updateRan(System.currentTimeMillis()));
        sendUpdateCount(ackStream, rows);
      } catch (LoopbackException | RuntimeException e) {
        ackStream.onError(flightError(e));
      }
    };
  }

  /** Binds parameters: only an empty parameter set is accepted. */
  @Override
  public Runnable acceptPutPreparedStatementQuery(
      CommandPreparedStatementQuery command, CallContext context,
      FlightStream flightStream, StreamListener<PutResult> ackStream) {
    return () -> {
      try {
        rejectParameters(flightStream);
        prepared(command.getPreparedStatementHandle(), context);
        ackStream.onCompleted();
      } catch (RuntimeException e) {
        ackStream.onError(flightError(e));
      }
    };
  }

  private static void rejectParameters(FlightStream stream) {
    while (stream.next()) {
      if (stream.getRoot().getRowCount() > 0) {
        throw CallStatus.INVALID_ARGUMENT
            .withDescription("Query parameters are not supported by Impala's Flight SQL")
            .toRuntimeException();
      }
    }
  }

  // ----------------------------------------------------------------------- metadata

  /** Collects metadata rows on the caller's loopback session. */
  @FunctionalInterface
  private interface Collector {
    List<Map<String, Object>> collect(LoopbackSession session, BufferAllocator allocator)
        throws LoopbackException;
  }

  private static Collector hs2(LoopbackSession.MetadataCall call, int fetchRows) {
    return (session, allocator) -> session.metadata(call, allocator, fetchRows);
  }

  /** Impala has no catalogs: only no catalog or "" matches. */
  private static boolean otherCatalog(boolean hasCatalog, String catalog) {
    return hasCatalog && !catalog.isEmpty();
  }

  /** Metadata tickets are the packed command itself: DoGet runs it (stateless). */
  private static FlightInfo metadataInfo(Schema schema, FlightDescriptor descriptor) {
    return new FlightInfo(schema, descriptor, Collections.singletonList(
        new FlightEndpoint(new Ticket(descriptor.getCommand()))), -1, -1);
  }

  /**
   * Runs 'rows' (and 'columns' for table schemas, may be null) as the caller and sends
   * the result as one batch of 'schema'. 'rows' null: empty result without HS2.
   */
  private void streamMetadata(CallContext context, ServerStreamListener listener,
      Schema schema, FlightMetadata.Sources sources, Collector rows, Collector columns) {
    try (BufferAllocator child =
             allocator_.newChildAllocator("flight-sql-metadata", 0, Long.MAX_VALUE)) {
      List<Map<String, Object>> result = Collections.emptyList();
      Map<String, byte[]> tableSchemas = Collections.emptyMap();
      if (rows != null) {
        LoopbackSession session = borrow(context.peerIdentity());
        try {
          result = rows.collect(session, child);
          if (columns != null && !result.isEmpty()) {
            tableSchemas = FlightMetadata.tableSchemas(columns.collect(session, child));
          }
        } finally {
          sessions_.release(session);
        }
      }
      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, child)) {
        listener.start(root);
        FlightMetadata.fill(root, result, sources, tableSchemas);
        if (!result.isEmpty()) listener.putNext();
        listener.completed();
      }
    } catch (LoopbackException | RuntimeException e) {
      listener.error(flightError(e));
    }
  }

  @Override
  public FlightInfo getFlightInfoCatalogs(CommandGetCatalogs request, CallContext context,
      FlightDescriptor descriptor) {
    return metadataInfo(Schemas.GET_CATALOGS_SCHEMA, descriptor);
  }

  @Override
  public void getStreamCatalogs(CallContext context, ServerStreamListener listener) {
    // Impala has no catalogs
    streamMetadata(context, listener, Schemas.GET_CATALOGS_SCHEMA,
        new FlightMetadata.Sources(), null, null);
  }

  @Override
  public FlightInfo getFlightInfoSchemas(CommandGetDbSchemas request, CallContext context,
      FlightDescriptor descriptor) {
    return metadataInfo(Schemas.GET_SCHEMAS_SCHEMA, descriptor);
  }

  @Override
  public void getStreamSchemas(CommandGetDbSchemas command, CallContext context,
      ServerStreamListener listener) {
    Collector rows = otherCatalog(command.hasCatalog(), command.getCatalog()) ? null
        : hs2(LoopbackSession.schemas(command.hasDbSchemaFilterPattern()
            ? command.getDbSchemaFilterPattern() : null), fetchRows_);
    streamMetadata(context, listener, Schemas.GET_SCHEMAS_SCHEMA, FlightMetadata.SCHEMAS,
        rows, null);
  }

  @Override
  public FlightInfo getFlightInfoTables(CommandGetTables request, CallContext context,
      FlightDescriptor descriptor) {
    return metadataInfo(request.getIncludeSchema() ? Schemas.GET_TABLES_SCHEMA
        : Schemas.GET_TABLES_SCHEMA_NO_SCHEMA, descriptor);
  }

  @Override
  public void getStreamTables(CommandGetTables command, CallContext context,
      ServerStreamListener listener) {
    String schema = command.hasDbSchemaFilterPattern()
        ? command.getDbSchemaFilterPattern() : null;
    String table = command.hasTableNameFilterPattern()
        ? command.getTableNameFilterPattern() : null;
    boolean other = otherCatalog(command.hasCatalog(), command.getCatalog());
    Collector rows = other ? null : hs2(LoopbackSession.tables(schema, table,
        new ArrayList<>(command.getTableTypesList())), fetchRows_);
    Collector columns = other || !command.getIncludeSchema() ? null
        : hs2(LoopbackSession.columns(schema, table), fetchRows_);
    streamMetadata(context, listener, command.getIncludeSchema()
        ? Schemas.GET_TABLES_SCHEMA : Schemas.GET_TABLES_SCHEMA_NO_SCHEMA,
        FlightMetadata.TABLES, rows, columns);
  }

  @Override
  public FlightInfo getFlightInfoTableTypes(CommandGetTableTypes request,
      CallContext context, FlightDescriptor descriptor) {
    return metadataInfo(Schemas.GET_TABLE_TYPES_SCHEMA, descriptor);
  }

  @Override
  public void getStreamTableTypes(CallContext context, ServerStreamListener listener) {
    streamMetadata(context, listener, Schemas.GET_TABLE_TYPES_SCHEMA,
        FlightMetadata.TABLE_TYPES, hs2(LoopbackSession.tableTypes(), fetchRows_), null);
  }

  @Override
  public FlightInfo getFlightInfoPrimaryKeys(CommandGetPrimaryKeys request,
      CallContext context, FlightDescriptor descriptor) {
    return metadataInfo(Schemas.GET_PRIMARY_KEYS_SCHEMA, descriptor);
  }

  @Override
  public void getStreamPrimaryKeys(CommandGetPrimaryKeys command, CallContext context,
      ServerStreamListener listener) {
    Collector rows = otherCatalog(command.hasCatalog(), command.getCatalog()) ? null
        : hs2(LoopbackSession.primaryKeys(
            command.hasDbSchema() ? command.getDbSchema() : null, command.getTable()),
            fetchRows_);
    streamMetadata(context, listener, Schemas.GET_PRIMARY_KEYS_SCHEMA,
        FlightMetadata.PRIMARY_KEYS, rows, null);
  }

  @Override
  public FlightInfo getFlightInfoExportedKeys(CommandGetExportedKeys request,
      CallContext context, FlightDescriptor descriptor) {
    return metadataInfo(Schemas.GET_EXPORTED_KEYS_SCHEMA, descriptor);
  }

  /** Foreign keys referencing the table: the table is the parent side. */
  @Override
  public void getStreamExportedKeys(CommandGetExportedKeys command, CallContext context,
      ServerStreamListener listener) {
    Collector rows = otherCatalog(command.hasCatalog(), command.getCatalog()) ? null
        : hs2(LoopbackSession.crossReference(
            command.hasDbSchema() ? command.getDbSchema() : null, command.getTable(),
            null, null), fetchRows_);
    streamMetadata(context, listener, Schemas.GET_EXPORTED_KEYS_SCHEMA,
        FlightMetadata.FOREIGN_KEYS, rows, null);
  }

  @Override
  public FlightInfo getFlightInfoImportedKeys(CommandGetImportedKeys request,
      CallContext context, FlightDescriptor descriptor) {
    return metadataInfo(Schemas.GET_IMPORTED_KEYS_SCHEMA, descriptor);
  }

  /** Foreign keys of the table: the table is the foreign side. */
  @Override
  public void getStreamImportedKeys(CommandGetImportedKeys command, CallContext context,
      ServerStreamListener listener) {
    Collector rows = otherCatalog(command.hasCatalog(), command.getCatalog()) ? null
        : hs2(LoopbackSession.crossReference(null, null,
            command.hasDbSchema() ? command.getDbSchema() : null, command.getTable()),
            fetchRows_);
    streamMetadata(context, listener, Schemas.GET_IMPORTED_KEYS_SCHEMA,
        FlightMetadata.FOREIGN_KEYS, rows, null);
  }

  @Override
  public FlightInfo getFlightInfoCrossReference(CommandGetCrossReference request,
      CallContext context, FlightDescriptor descriptor) {
    return metadataInfo(Schemas.GET_CROSS_REFERENCE_SCHEMA, descriptor);
  }

  @Override
  public void getStreamCrossReference(CommandGetCrossReference command,
      CallContext context, ServerStreamListener listener) {
    boolean other = otherCatalog(command.hasPkCatalog(), command.getPkCatalog())
        || otherCatalog(command.hasFkCatalog(), command.getFkCatalog());
    Collector rows = other ? null : hs2(LoopbackSession.crossReference(
        command.hasPkDbSchema() ? command.getPkDbSchema() : null, command.getPkTable(),
        command.hasFkDbSchema() ? command.getFkDbSchema() : null, command.getFkTable()),
        fetchRows_);
    streamMetadata(context, listener, Schemas.GET_CROSS_REFERENCE_SCHEMA,
        FlightMetadata.FOREIGN_KEYS, rows, null);
  }

  @Override
  public FlightInfo getFlightInfoTypeInfo(CommandGetXdbcTypeInfo request,
      CallContext context, FlightDescriptor descriptor) {
    return metadataInfo(Schemas.GET_TYPE_INFO_SCHEMA, descriptor);
  }

  @Override
  public void getStreamTypeInfo(CommandGetXdbcTypeInfo command, CallContext context,
      ServerStreamListener listener) {
    Collector all = hs2(LoopbackSession.typeInfo(), fetchRows_);
    Collector rows = !command.hasDataType() ? all : (session, allocator) -> {
      List<Map<String, Object>> filtered = new ArrayList<>();
      for (Map<String, Object> row : all.collect(session, allocator)) {
        Object type = row.get("data_type");
        if (type instanceof Number
            && ((Number) type).intValue() == command.getDataType()) {
          filtered.add(row);
        }
      }
      return filtered;
    };
    streamMetadata(context, listener, Schemas.GET_TYPE_INFO_SCHEMA,
        FlightMetadata.TYPE_INFO, rows, null);
  }

  // ------------------------------------------------------------------------ helpers

  private LoopbackSession borrow(String user) {
    try {
      return sessions_.borrow(user);
    } catch (LoopbackException e) {
      throw flightError(e);
    }
  }

  /** Closes a statement left behind by an error and gives the session back. */
  private void abandon(LoopbackSession session, TOperationHandle op) {
    if (session == null) return;
    try {
      if (op != null) session.closeOperation(op);
    } catch (LoopbackException e) {
      LOG.debug("Closing an abandoned Flight SQL statement failed: {}", e.getMessage());
    } finally {
      sessions_.release(session);
    }
  }

  private static Ticket ticket(String id) {
    TicketStatementQuery query = TicketStatementQuery.newBuilder()
        .setStatementHandle(ByteString.copyFromUtf8(id)).build();
    return new Ticket(Any.pack(query).toByteArray());
  }

  private static String ticketId(Ticket ticket) {
    try {
      Any any = Any.parseFrom(ticket.getBytes());
      if (!any.is(TicketStatementQuery.class)) return null;
      return any.unpack(TicketStatementQuery.class).getStatementHandle().toStringUtf8();
    } catch (InvalidProtocolBufferException e) {
      return null;
    }
  }

  /**
   * Sends the update count; an unknown count (-1: DDL, ...) is sent as 0, as JDBC's
   * executeUpdate does for statements that return nothing. The Arrow Flight SQL JDBC
   * driver treats -1 as "a result set follows": it then fails (prepared executeUpdate:
   * NullPointerException) or, since 19.0.0, retries and so runs the statement again.
   */
  private void sendUpdateCount(StreamListener<PutResult> ackStream, long rows) {
    byte[] bytes = DoPutUpdateResult.newBuilder().setRecordCount(Math.max(rows, 0L))
        .build().toByteArray();
    try (ArrowBuf buffer = allocator_.buffer(bytes.length)) {
      buffer.writeBytes(bytes);
      ackStream.onNext(PutResult.metadata(buffer));
    }
    ackStream.onCompleted();
  }

  /**
   * Flight error for a failed call, by Impala's error text: analysis and parse errors are
   * the client's (INVALID_ARGUMENT), authorization errors UNAUTHORIZED, cancelled
   * statements CANCELLED, too many sessions UNAVAILABLE (retry later), others INTERNAL.
   */
  static FlightRuntimeException flightError(Exception e) {
    if (e instanceof FlightRuntimeException) return (FlightRuntimeException) e;
    String message = e.getMessage() == null ? e.toString() : e.getMessage();
    CallStatus status;
    if (e instanceof LoopbackSessions.TooManySessionsException) {
      status = CallStatus.UNAVAILABLE;
    } else if (message.contains("AnalysisException") || message.contains("ParseException")
        || message.contains("Syntax error")) {
      status = CallStatus.INVALID_ARGUMENT;
    } else if (message.contains("AuthorizationException")) {
      status = CallStatus.UNAUTHORIZED;
    } else if (message.contains("Cancelled")) {
      status = CallStatus.CANCELLED;
    } else {
      status = CallStatus.INTERNAL;
    }
    return status.withDescription(message).withCause(e).toRuntimeException();
  }
}
