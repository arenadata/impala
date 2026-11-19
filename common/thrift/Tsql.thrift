// ADH-TSQL. Step protocol between the coordinator and the T-SQL runtime.
// Field ids in this file are independent of Frontend.thrift.

namespace cpp impala
namespace java org.apache.impala.thrift

enum TSqlStepKind {
  SQL = 0
  CLIENT_RESULT = 1
  FINISHED = 2
  FAILED = 3
}

struct TSqlRequest {
  1: required string source
  2: required string database
  3: optional string procedure_name
  4: required string owner
  5: required string invoker
  6: required list<string> arg_literals
}

struct TSqlCell {
  1: required bool is_null
  2: optional string value
}

struct TSqlFeed {
  1: optional i32 sqlcode
  2: optional string error_message
  3: optional list<string> column_names
  4: optional list<list<TSqlCell>> rows
}

struct TSqlStep {
  1: required TSqlStepKind kind
  2: optional string sql
  3: optional string error_message
  4: optional i32 sqlcode
  // FINISHED only. One row of OUTPUT params, or a one-column summary.
  5: optional list<list<string>> summary_rows
  // Query options that apply only to statements inside the current procedure.
  6: optional map<string, string> procedure_options
}

struct TSqlStartResponse {
  1: required i64 handle
  2: optional string error_message
}

struct TSqlNextRequest {
  1: required i64 handle
  2: optional TSqlFeed feed
}
