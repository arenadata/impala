// ADH-TSQL
#include "tsql/tsql-child-query.h"

#include <cstdint>

#include "gen-cpp/TCLIService_types.h"
#include "service/client-request-state.h"
#include "service/impala-server.h"
#include "service/impala-server.inline.h"
#include "util/debug-util.h"

#include "common/names.h"

using namespace apache::hive::service::cli::thrift;

namespace impala {

namespace {

bool IsNumNodesKey(const string& key) {
  static const char* name = "num_nodes";
  if (key.size() != 9) return false;
  for (size_t i = 0; i < key.size(); ++i) {
    char c = key[i];
    if (c >= 'A' && c <= 'Z') c = static_cast<char>(c - 'A' + 'a');
    if (c != name[i]) return false;
  }
  return true;
}

bool NullBit(const string& nulls, uint32_t row_idx) {
  if (nulls.size() <= row_idx / 8) return false;
  return (nulls[row_idx / 8] & (1 << (row_idx % 8))) != 0;
}

// Impala's normal HS2 session returns results column-wise. Row-wise results are
// only used when the query has a parent id, which this child query does not set.
void AppendColumnar(
    const vector<::apache::hive::service::cli::thrift::TColumn>& cols, int max_rows,
    int* fetched, bool* hit_cap, vector<vector<string>>* values,
    vector<vector<bool>>* nulls) {
  if (cols.empty()) return;
  size_t nrows = 0;
  if (cols[0].__isset.stringVal) nrows = cols[0].stringVal.values.size();
  else if (cols[0].__isset.i32Val) nrows = cols[0].i32Val.values.size();
  else if (cols[0].__isset.i64Val) nrows = cols[0].i64Val.values.size();
  else if (cols[0].__isset.i16Val) nrows = cols[0].i16Val.values.size();
  else if (cols[0].__isset.boolVal) nrows = cols[0].boolVal.values.size();
  else if (cols[0].__isset.byteVal) nrows = cols[0].byteVal.values.size();
  else if (cols[0].__isset.doubleVal) nrows = cols[0].doubleVal.values.size();
  else if (cols[0].__isset.binaryVal) nrows = cols[0].binaryVal.values.size();
  for (size_t r = 0; r < nrows; ++r) {
    if (*fetched >= max_rows) {
      *hit_cap = true;
      return;
    }
    vector<string> vals;
    vector<bool> nils;
    for (const ::apache::hive::service::cli::thrift::TColumn& col : cols) {
      bool is_null = false;
      string text;
      if (col.__isset.stringVal) {
        is_null = NullBit(col.stringVal.nulls, r);
        if (!is_null && r < col.stringVal.values.size()) text = col.stringVal.values[r];
      } else if (col.__isset.i32Val) {
        is_null = NullBit(col.i32Val.nulls, r);
        if (!is_null && r < col.i32Val.values.size()) text = std::to_string(col.i32Val.values[r]);
      } else if (col.__isset.i64Val) {
        is_null = NullBit(col.i64Val.nulls, r);
        if (!is_null && r < col.i64Val.values.size()) text = std::to_string(col.i64Val.values[r]);
      } else if (col.__isset.i16Val) {
        is_null = NullBit(col.i16Val.nulls, r);
        if (!is_null && r < col.i16Val.values.size()) text = std::to_string(col.i16Val.values[r]);
      } else if (col.__isset.byteVal) {
        is_null = NullBit(col.byteVal.nulls, r);
        if (!is_null && r < col.byteVal.values.size()) text = std::to_string(col.byteVal.values[r]);
      } else if (col.__isset.doubleVal) {
        is_null = NullBit(col.doubleVal.nulls, r);
        if (!is_null && r < col.doubleVal.values.size()) {
          text = std::to_string(col.doubleVal.values[r]);
        }
      } else if (col.__isset.boolVal) {
        is_null = NullBit(col.boolVal.nulls, r);
        if (!is_null && r < col.boolVal.values.size()) {
          text = col.boolVal.values[r] ? "true" : "false";
        }
      } else if (col.__isset.binaryVal) {
        is_null = NullBit(col.binaryVal.nulls, r);
        if (!is_null && r < col.binaryVal.values.size()) text = col.binaryVal.values[r];
      } else {
        is_null = true;
      }
      nils.push_back(is_null);
      vals.push_back(is_null ? "" : text);
    }
    values->push_back(vals);
    nulls->push_back(nils);
    ++(*fetched);
  }
}

} // namespace

TsqlChildQuery::TsqlChildQuery(ClientRequestState* parent, ImpalaServer* server,
    const std::string& sql, const std::string& effective_user)
  : parent_(parent), server_(server), sql_(sql), effective_user_(effective_user) {}

Status TsqlChildQuery::Fetch(int max_rows, bool* hit_cap) {
  DCHECK(hit_cap != nullptr);
  *hit_cap = false;
  Status status = FetchInternal(max_rows, hit_cap);
  if (running_) Close();
  return status;
}

void TsqlChildQuery::Close() {
  TCloseOperationReq close_req;
  close_req.operationHandle = handle_;
  TCloseOperationResp close_resp;
  server_->CloseOperation(close_resp, close_req);
  lock_guard<SpinLock> l(lock_);
  running_ = false;
}

Status TsqlChildQuery::FetchInternal(int max_rows, bool* hit_cap) {
  const TUniqueId& session_id = parent_->session_id();
  TExecuteStatementReq req;
  ImpalaServer::TUniqueIdToTHandleIdentifier(
      session_id, parent_->session()->secret, &req.sessionHandle.sessionId);
  req.__set_statement(sql_);
  // Newly created tables have no stats, so Impala skips the small-query path and
  // plans a distributed fragment. That plan duplicated rows for filters, semi-joins,
  // scalar subqueries, and partitioned CTAS. One node keeps a single writer and a
  // single scan. A procedure SET NUM_NODES still wins.
  map<string, string> overlay = options_;
  bool procedure_set_num_nodes = false;
  for (const auto& opt : overlay) {
    if (IsNumNodesKey(opt.first)) {
      procedure_set_num_nodes = true;
      break;
    }
  }
  if (!procedure_set_num_nodes) overlay["NUM_NODES"] = "1";
  req.__set_confOverlay(overlay);

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
    if (fetch_resp.results.rows.empty() && fetch_resp.results.__isset.columns) {
      AppendColumnar(fetch_resp.results.columns, max_rows, &fetched, hit_cap,
          &values_, &nulls_);
    }
    if (*hit_cap || !fetch_resp.hasMoreRows) break;
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
