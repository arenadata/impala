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

#include <cstdint>
#include <mutex>

#include "common/status.h"
#include "gen-cpp/Tsql_types.h"
#include "gutil/macros.h"

namespace impala {

class ClientRequestState;
class ImpalaServer;
class TsqlChildQuery;

// Drives TsqlBridge on the async-exec thread. Exec() joins Run() before
// returning so the client schema is the procedure's final result.
class TsqlExecutor {
 public:
  TsqlExecutor(ClientRequestState* parent, ImpalaServer* server);
  ~TsqlExecutor();

  void Run();
  void Cancel();
  // Reserves a slot against --tsql_max_concurrent_batches. Released when Run() ends.
  Status Admit();

 private:
  Status RunInternal();
  // One STRING cell per output column. Caller does not hold parent_->lock().
  void PublishClientResult(const TsqlChildQuery& child);
  ClientRequestState* parent_;
  ImpalaServer* server_;
  int64_t handle_ = 0;
  TsqlChildQuery* active_ = nullptr;
  // Stays set once Cancel() runs, including between child statements.
  bool cancelled_ = false;
  bool admitted_ = false;
  void Release();
  // mutex, not a spinlock: Cancel() calls CancelOperation while holding it.
  std::mutex lock_;

  DISALLOW_COPY_AND_ASSIGN(TsqlExecutor);
};

} // namespace impala
