// ADH-TSQL
#include "tsql/tsql-executor.h"

#include "tsql/tsql-child-query.h"
#include "tsql/tsql-flags.h"

#include <exception>

#include "gen-cpp/Tsql_types.h"
#include "rpc/thrift-util.h"
#include "runtime/exec-env.h"
#include "runtime/types.h"
#include "service/client-request-state.h"
#include "service/impala-server.h"
#include "util/jni-util.h"
#include "util/string-util.h"

#include "common/names.h"

namespace impala {

static jclass bridge_class = nullptr;
static jmethodID start_method = nullptr;
static jmethodID next_method = nullptr;
static jmethodID close_method = nullptr;

static Status InitBridge() {
  if (bridge_class != nullptr) return Status::OK();
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == nullptr) return Status("No JNIEnv for T-SQL");
  RETURN_IF_ERROR(JniUtil::GetGlobalClassRef(
      env, "org/apache/impala/tsql/TsqlBridge", &bridge_class));
  JniMethodDescriptor start_desc = {"start", "([B)[B", &start_method};
  JniMethodDescriptor next_desc = {"next", "([B)[B", &next_method};
  JniMethodDescriptor close_desc = {"close", "(J)V", &close_method};
  RETURN_IF_ERROR(JniUtil::LoadStaticJniMethod(env, bridge_class, &start_desc));
  RETURN_IF_ERROR(JniUtil::LoadStaticJniMethod(env, bridge_class, &next_desc));
  RETURN_IF_ERROR(JniUtil::LoadStaticJniMethod(env, bridge_class, &close_desc));
  return Status::OK();
}

static Status CallBytes(jmethodID method, const uint8_t* data, uint32_t len,
    string* response) {
  JNIEnv* env = JniUtil::GetJNIEnv();
  JniLocalFrame frame;
  RETURN_IF_ERROR(frame.push(env));
  jbyteArray arg = env->NewByteArray(len);
  RETURN_ERROR_IF_EXC(env);
  env->SetByteArrayRegion(arg, 0, len, reinterpret_cast<const jbyte*>(data));
  RETURN_ERROR_IF_EXC(env);
  jobject result = env->CallStaticObjectMethod(bridge_class, method, arg);
  RETURN_ERROR_IF_EXC(env);
  if (result == nullptr) return Status("T-SQL JNI returned null");
  jbyteArray bytes = static_cast<jbyteArray>(result);
  jsize n = env->GetArrayLength(bytes);
  response->resize(n);
  env->GetByteArrayRegion(bytes, 0, n, reinterpret_cast<jbyte*>(&(*response)[0]));
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

TsqlExecutor::TsqlExecutor(ClientRequestState* parent, ImpalaServer* server)
  : parent_(parent), server_(server) {}

TsqlExecutor::~TsqlExecutor() {
  if (handle_ != 0 && close_method != nullptr) {
    JNIEnv* env = JniUtil::GetJNIEnv();
    if (env != nullptr) {
      env->CallStaticVoidMethod(bridge_class, close_method, static_cast<jlong>(handle_));
    }
  }
  delete active_;
}

void TsqlExecutor::Cancel() {
  lock_guard<SpinLock> l(lock_);
  if (active_ != nullptr) active_->Cancel();
}

void TsqlExecutor::Run() {
  Status status;
  try {
    status = RunInternal();
  } catch (const std::exception& e) {
    const char* what = e.what();
    status = Status(what == nullptr ? "T-SQL execution failed" : what);
  }
  if (!status.ok()) {
    lock_guard<mutex> guard(*parent_->lock());
    discard_result(parent_->UpdateQueryStatus(status));
  }
}

static TColumn StringResultColumn(const string& name) {
  TColumn col;
  col.__set_columnName(name);
  col.__set_columnType(ColumnType(TYPE_STRING).ToThrift());
  return col;
}

void TsqlExecutor::PublishClientResult(const TsqlChildQuery& child) {
  vector<string> names = child.columns();
  if (names.empty()) names.push_back("result");
  TResultSetMetadata meta;
  for (const string& name : names) meta.columns.push_back(StringResultColumn(name));

  vector<TResultRow> rows;
  rows.reserve(child.values().size());
  for (size_t r = 0; r < child.values().size(); ++r) {
    TResultRow row;
    row.__isset.colVals = true;
    row.colVals.resize(names.size());
    for (size_t c = 0; c < names.size(); ++c) {
      bool missing = c >= child.values()[r].size();
      bool is_null = missing
          || (r < child.nulls().size() && c < child.nulls()[r].size()
              && child.nulls()[r][c]);
      if (!is_null) row.colVals[c].__set_string_val(child.values()[r][c]);
    }
    rows.push_back(std::move(row));
  }
  {
    lock_guard<mutex> guard(*parent_->lock());
    parent_->SetResultSet(meta, std::move(rows));
  }
}

Status TsqlExecutor::RunInternal() {
  RETURN_IF_ERROR(InitBridge());
  const TSqlRequest& request = parent_->exec_request().tsql_request;
  ThriftSerializer serializer(false);
  vector<uint8_t> bytes;
  RETURN_IF_ERROR(serializer.SerializeToVector(&request, &bytes));
  string raw;
  RETURN_IF_ERROR(CallBytes(start_method, bytes.data(), bytes.size(), &raw));
  uint32_t len = raw.size();
  TSqlStartResponse started;
  RETURN_IF_ERROR(DeserializeThriftMsg(reinterpret_cast<const uint8_t*>(raw.data()),
      &len, false, &started));
  if (started.__isset.error_message && !started.error_message.empty()) {
    return Status(started.error_message);
  }
  handle_ = started.handle;

  TSqlFeed feed;
  while (true) {
    TSqlNextRequest next_req;
    next_req.__set_handle(handle_);
    next_req.__set_feed(feed);
    vector<uint8_t> next_bytes;
    RETURN_IF_ERROR(serializer.SerializeToVector(&next_req, &next_bytes));
    string step_raw;
    RETURN_IF_ERROR(CallBytes(next_method, next_bytes.data(), next_bytes.size(), &step_raw));
    uint32_t step_len = step_raw.size();
    TSqlStep step;
    RETURN_IF_ERROR(DeserializeThriftMsg(reinterpret_cast<const uint8_t*>(step_raw.data()),
        &step_len, false, &step));
    if (step.kind == TSqlStepKind::FAILED) {
      return Status(step.error_message);
    }
    if (step.kind == TSqlStepKind::FINISHED && !step.__isset.sql) {
      vector<string> summary;
      if (step.__isset.summary_rows && !step.summary_rows.empty()
          && !step.summary_rows[0].empty()) {
        summary.push_back(step.summary_rows[0][0]);
      } else {
        summary.push_back("OK");
      }
      {
        lock_guard<mutex> guard(*parent_->lock());
        parent_->SetResultSet(summary);
      }
      return Status::OK();
    }
    if (step.kind == TSqlStepKind::SQL || step.kind == TSqlStepKind::CLIENT_RESULT
        || step.kind == TSqlStepKind::FINISHED) {
      int cap = step.kind == TSqlStepKind::SQL
          ? FLAGS_tsql_max_cursor_rows : FLAGS_tsql_max_result_rows;
      string user;
      const TSqlRequest& tsql = parent_->exec_request().tsql_request;
      if (tsql.owner != tsql.invoker) user = tsql.owner;
      std::unique_ptr<TsqlChildQuery> child(new TsqlChildQuery(
          parent_, server_, step.sql, user));
      if (step.__isset.procedure_options) child->set_procedure_options(step.procedure_options);
      {
        lock_guard<SpinLock> l(lock_);
        active_ = child.get();
      }
      bool hit_cap = false;
      Status child_status = child->Fetch(cap, &hit_cap);
      {
        lock_guard<SpinLock> l(lock_);
        active_ = nullptr;
      }
      if (!child_status.ok()) {
        // Cancellation is not a T-SQL error. TRY/CATCH must not swallow it.
        if (child_status.IsCancelled()) return child_status;
        feed = TSqlFeed();
        feed.__set_sqlcode(-1);
        string detail = child_status.GetDetail();
        if (detail.empty()) detail = "SQL failed";
        feed.__set_error_message(detail);
        continue;
      }
      if (step.kind != TSqlStepKind::SQL) {
        PublishClientResult(*child);
        return Status::OK();
      }
      feed = TSqlFeed();
      feed.__set_sqlcode(0);
      feed.__set_column_names(child->columns());
      vector<vector<TSqlCell>> rows;
      for (size_t r = 0; r < child->values().size(); ++r) {
        vector<TSqlCell> cells;
        for (size_t c = 0; c < child->values()[r].size(); ++c) {
          TSqlCell cell;
          bool is_null = r < child->nulls().size() && c < child->nulls()[r].size()
              && child->nulls()[r][c];
          cell.__set_is_null(is_null);
          if (!is_null) cell.__set_value(child->values()[r][c]);
          cells.push_back(cell);
        }
        rows.push_back(cells);
      }
      feed.__set_rows(rows);
    }
  }
}

} // namespace impala
