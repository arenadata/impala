// ADH-TSQL
#pragma once

#include <map>
#include <string>
#include <vector>

#include "common/status.h"
#include "gen-cpp/TCLIService_types.h"
#include "gutil/macros.h"
#include "util/spinlock.h"

namespace impala {

class ClientRequestState;
class ImpalaServer;

// Runs one Impala statement for a T-SQL step and keeps every fetched batch.
// Does not use ChildQuery: that class keeps only the last 1024-row fetch and
// copies the parent query-option snapshot into the child.
class TsqlChildQuery {
 public:
  TsqlChildQuery(ClientRequestState* parent, ImpalaServer* server, const std::string& sql,
      const std::string& effective_user);

  // Fetches up to max_rows. Returns an error if the statement fails or the cap is hit.
  Status Fetch(int max_rows, bool* hit_cap);
  void Cancel();

  const std::vector<std::string>& columns() const { return columns_; }
  const std::vector<std::vector<std::string>>& values() const { return values_; }
  const std::vector<std::vector<bool>>& nulls() const { return nulls_; }

  void set_procedure_options(const std::map<std::string, std::string>& options) {
    options_ = options;
  }

 private:
  ClientRequestState* parent_;
  ImpalaServer* server_;
  std::string sql_;
  std::string effective_user_;
  SpinLock lock_;
  bool cancelled_ = false;
  apache::hive::service::cli::thrift::TOperationHandle handle_;
  bool running_ = false;
  std::map<std::string, std::string> options_;
  std::vector<std::string> columns_;
  std::vector<std::vector<std::string>> values_;
  std::vector<std::vector<bool>> nulls_;

  DISALLOW_COPY_AND_ASSIGN(TsqlChildQuery);
};

} // namespace impala
