package org.apache.impala.tsql;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.impala.authorization.AuthorizationException;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.lpsql.LpsqlParser;
import org.apache.impala.service.Frontend;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TExecRequest;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.thrift.TSqlRequest;
import org.apache.impala.thrift.TStmtType;
import org.apache.impala.util.TSessionStateUtil;

/**
 * ADH-TSQL. The only class {@code Frontend} names. Returns a finished
 * {@link TExecRequest} for a T-SQL or LPSQL batch and null for ordinary SQL.
 */
public final class TsqlFrontend {
  private TsqlFrontend() {}

  public static TExecRequest tryExecRequest(Frontend frontend, TQueryCtx queryCtx)
      throws ImpalaException {
    if (queryCtx == null || queryCtx.client_request == null
        || queryCtx.client_request.stmt == null) {
      return null;
    }
    String sql = queryCtx.client_request.stmt;
    TsqlAst.Script script;
    if (LpsqlParser.looksLikeLpsql(sql)) {
      script = rejectUnlessParsed(sql, true);
    } else if (TsqlParser.looksLikeTsql(sql)) {
      script = rejectUnlessParsed(sql, false);
    } else {
      return null;
    }

    String user = TSessionStateUtil.getEffectiveUser(queryCtx.session);
    String database = queryCtx.session.database;
    TsqlAccess access = frontend == null ? TsqlAccess.OWNER_ONLY : new TsqlImpalaAccess(frontend);
    for (TsqlAst.Stmt stmt : script.statements) {
      if (!(stmt instanceof TsqlAst.CreateProc)) continue;
      if (access.canCreate(database, user)) continue;
      throw new AuthorizationException(String.format(
          "User '%s' does not have privileges to CREATE procedures in database '%s'",
          user, database));
    }
    TSqlRequest tsql = new TSqlRequest();
    tsql.setSource(sql);
    tsql.setDatabase(queryCtx.session.database);
    tsql.setOwner(user);
    tsql.setInvoker(user);
    tsql.setArg_literals(Collections.<String>emptyList());

    TColumn placeholder = new TColumn();
    placeholder.setColumnName("result");
    placeholder.setColumnType(ScalarType.createType(PrimitiveType.STRING).toThrift());
    TResultSetMetadata metadata = new TResultSetMetadata();
    metadata.setColumns(new ArrayList<TColumn>());
    metadata.addToColumns(placeholder);

    TExecRequest request = new TExecRequest();
    request.setStmt_type(TStmtType.TSQL);
    request.setQuery_options(queryCtx.client_request.query_options);
    request.setAnalysis_warnings(new ArrayList<String>());
    request.setResult_set_metadata(metadata);
    request.setTsql_request(tsql);
    return request;
  }

  private static TsqlAst.Script rejectUnlessParsed(String sql, boolean lpsql)
      throws AnalysisException {
    // Reject the batch here so a syntax error is a normal analysis failure
    // rather than a coordinator crash after the handle is returned.
    try {
      if (lpsql) return LpsqlParser.parse(sql);
      return TsqlParser.parse(sql);
    } catch (TsqlParser.ParseException e) {
      throw new AnalysisException(e.getMessage());
    }
  }
}
