// ADH-TSQL
#include "tsql/tsql-child-query.h"

#include "gen-cpp/TCLIService_types.h"
#include "service/client-request-state.h"
#include "service/impala-server.h"
#include "util/debug-util.h"

#include "common/names.h"

using namespace apache::hive::service::cli::thrift;

namespace impala {

TsqlChildQuery::TsqlChildQuery(ClientRequestState* parent, ImpalaServer* server,
    const std::string& sql, const std::string& effective_user)
  : parent_(parent), server_(server), sql_(sql), effective_user_(effective_user) {}

Status TsqlChildQuery::Fetch(int max_rows, bool* hit_cap) {
  DCHECK(hit_cap != nullptr);
  *hit_cap = false;
  const TUniqueId& session_id = parent_->session_id();
  TExecuteStatementReq req;
  ImpalaServer::TUniqueIdToTHandleIdentifier(
      session_id, parent_->session()->secret, &req.sessionHandle.sessionId);
  req.__set_statement(sql_);
  if (!options_.empty()) {
    req.__set_confOverlay(options_);
  }

  TExecuteStatementResp exec_resp;
  {
    lock_guard<SpinLock> l(lock_);
    if (cancelled_) return Status::CANCELLED;
  }
  // Friend call. effective_user_ is applied inside the server, not via a client overlay.
  server_->ExecuteStatementCommon(exec_resp, req, nullptr,
      effective_user_.empty() ? nullptr : &effective_user_);
  RETURN_IF_ERROR(Status(exec_resp.status));
  handle_ = exec_resp.operationHandle;
  {
    lock_guard<SpinLock> l(lock_);
    running_ = true;
  }

  TGetResultSetMetadataReq meta_req;
  meta_req.operationHandle = handle_;
  TGetResultSetMetadataResp meta_resp;
  server_->GetResultSetMetadata(meta_resp, meta_req);
  RETURN_IF_ERROR(Status(meta_resp.status));
  for (const TColumnDesc& col : meta_resp.schema.columns) {
    columns_.push_back(col.columnName);
  }

  TFetchResultsReq fetch_req;
  fetch_req.operationHandle = handle_;
  fetch_req.maxRows = 1024;
  int fetched = 0;
  while (true) {
    {
      lock_guard<SpinLock> l(lock_);
      if (cancelled_) return Status::CANCELLED;
    }
    TFetchResultsResp fetch_resp;
    server_->FetchResults(fetch_resp, fetch_req);
    RETURN_IF_ERROR(Status(fetch_resp.status));
    for (const TRow& row : fetch_resp.results.rows) {
      if (fetched >= max_rows) {
        *hit_cap = true;
        break;
      }
      vector<string> vals;
      vector<bool> nils;
      for (const ::apache::hive::service::cli::thrift::TColumnValue& cell : row.colVals) {
        if (cell.__isset.stringVal && cell.stringVal.__isset.value) {
          nils.push_back(false);
          vals.push_back(cell.stringVal.value);
        } else if (cell.__isset.i64Val && cell.i64Val.__isset.value) {
          nils.push_back(false);
          vals.push_back(std::to_string(cell.i64Val.value));
        } else if (cell.__isset.i32Val && cell.i32Val.__isset.value) {
          nils.push_back(false);
          vals.push_back(std::to_string(cell.i32Val.value));
        } else if (cell.__isset.doubleVal && cell.doubleVal.__isset.value) {
          nils.push_back(false);
          vals.push_back(std::to_string(cell.doubleVal.value));
        } else if (cell.__isset.boolVal && cell.boolVal.__isset.value) {
          nils.push_back(false);
          vals.push_back(cell.boolVal.value ? "true" : "false");
        } else {
          nils.push_back(true);
          vals.push_back("");
        }
      }
      values_.push_back(vals);
      nulls_.push_back(nils);
      ++fetched;
    }
    if (*hit_cap || !fetch_resp.hasMoreRows) break;
  }

  TCloseOperationReq close_req;
  close_req.operationHandle = handle_;
  TCloseOperationResp close_resp;
  server_->CloseOperation(close_resp, close_req);
  {
    lock_guard<SpinLock> l(lock_);
    running_ = false;
  }
  if (*hit_cap) {
    return Status(strings::Substitute(
        "T-SQL result exceeded $0 rows", max_rows));
  }
  return Status::OK();
}

void TsqlChildQuery::Cancel() {
  lock_guard<SpinLock> l(lock_);
  cancelled_ = true;
  if (!running_) return;
  TCancelOperationReq req;
  req.operationHandle = handle_;
  TCancelOperationResp resp;
  server_->CancelOperation(resp, req);
}

} // namespace impala
