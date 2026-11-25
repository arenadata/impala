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

// Step protocol between the coordinator and the T-SQL runtime.
// Field ids in this file are independent of Frontend.thrift.

namespace cpp impala
namespace java org.apache.impala.thrift

enum TSqlStepKind {
  SQL = 0
  CLIENT_RESULT = 1
  FINISHED = 2
  FAILED = 3
  // The runtime paused so the coordinator can observe cancellation.
  YIELD = 4
}

// How a child-query cell should be substituted back into Impala SQL.
// TEXT is quoted. NUMBER is copied exactly, never through a double.
enum TSqlValueKind {
  TEXT = 0
  NUMBER = 1
  BOOLEAN = 2
}

struct TSqlRequest {
  1: required string source
  2: required string database
  3: optional string procedure_name
  4: required string owner
  6: required list<string> arg_literals
  // 0 means unlimited. Filled by the coordinator from --tsql_max_steps.
  7: optional i64 max_steps
  8: optional i32 max_nesting
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
  // Parallel to column_names. Absent means the runtime infers from the text.
  5: optional list<TSqlValueKind> column_kinds
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
