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

// ADH-TSQL
#pragma once

#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "common/status.h"
#include "gen-cpp/TCLIService_types.h"
#include "gen-cpp/Tsql_types.h"
#include "gutil/macros.h"

namespace impala {

class ClientRequestState;
class ImpalaServer;

// Runs one Impala statement for a T-SQL step and keeps every fetched batch.
// Does not use ChildQuery: that class keeps only the last 1024-row fetch and
// copies the parent query-option snapshot into the child.
class TsqlChildQuery {
 public:
  TsqlChildQuery(ClientRequestState* parent, ImpalaServer* server, const std::string& sql);

  // Fetches up to max_rows. Returns an error if the statement fails or the cap is hit.
  Status Fetch(int max_rows, bool* hit_cap);
  void Cancel();

  const std::vector<std::string>& columns() const { return columns_; }
  const std::vector<TSqlValueKind::type>& column_kinds() const { return column_kinds_; }
  const std::vector<std::vector<std::string>>& values() const { return values_; }
  const std::vector<std::vector<bool>>& nulls() const { return nulls_; }

  void set_procedure_options(const std::map<std::string, std::string>& options) {
    options_ = options;
  }

 private:
  // Runs the statement and appends fetched rows. Does not close the operation.
  Status FetchInternal(int max_rows, bool* hit_cap);

  // Closes the HS2 operation after it has been opened. Safe to call when the
  // statement failed; a second close is ignored by the server.
  void Close();
  ClientRequestState* parent_;
  ImpalaServer* server_;
  std::string sql_;
  // mutex, not a spinlock: Cancel() calls CancelOperation while holding it.
  std::mutex lock_;
  bool cancelled_ = false;
  apache::hive::service::cli::thrift::TOperationHandle handle_;
  bool running_ = false;
  std::map<std::string, std::string> options_;
  std::vector<std::string> columns_;
  std::vector<TSqlValueKind::type> column_kinds_;
  std::vector<std::vector<std::string>> values_;
  std::vector<std::vector<bool>> nulls_;

  DISALLOW_COPY_AND_ASSIGN(TsqlChildQuery);
};

} // namespace impala
