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

#include "exec/catalog-op-executor.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

namespace impala {

static TCatalogOpRequest MakeComputeStatsRequest() {
  TCatalogOpRequest request;
  request.ddl_params.compute_stats_params.table_name.__set_db_name("db");
  request.ddl_params.compute_stats_params.table_name.__set_table_name("tbl");
  return request;
}

TEST(CatalogOpExecutorTest, UpdateStatsRequestTargetsTableOfComputeStats) {
  TCatalogOpRequest request = CatalogOpExecutor::BuildUpdateStatsRequest(
      TCatalogServiceRequestHeader(), MakeComputeStatsRequest());

  const TAlterTableParams& alter_params = request.ddl_params.alter_table_params;
  EXPECT_EQ(TAlterTableType::UPDATE_STATS, alter_params.alter_type);
  ASSERT_TRUE(alter_params.__isset.update_stats_params);
  EXPECT_EQ("db", alter_params.update_stats_params.table_name.db_name);
  EXPECT_EQ("tbl", alter_params.update_stats_params.table_name.table_name);
}

} // namespace impala
