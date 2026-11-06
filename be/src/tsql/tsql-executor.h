// ADH-TSQL
#pragma once

#include <cstdint>

#include "common/status.h"
#include "gen-cpp/Tsql_types.h"
#include "gutil/macros.h"
#include "util/spinlock.h"

namespace impala {

class ClientRequestState;
class ImpalaServer;
class TsqlChildQuery;

// Drives TsqlBridge on the async-exec thread. Exec() only starts Run().
class TsqlExecutor {
 public:
  TsqlExecutor(ClientRequestState* parent, ImpalaServer* server);
  ~TsqlExecutor();

  void Run();
  void Cancel();

 private:
  Status RunInternal();
  ClientRequestState* parent_;
  ImpalaServer* server_;
  int64_t handle_ = 0;
  TsqlChildQuery* active_ = nullptr;
  SpinLock lock_;

  DISALLOW_COPY_AND_ASSIGN(TsqlExecutor);
};

} // namespace impala
