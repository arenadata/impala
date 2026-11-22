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

#include <map>
#include <string>
#include <vector>

#include "exec/catalog-op-executor.h"
#include "service/child-query.h"
#include "service/client-request-state.h"
#include "service/query-options.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

namespace hs2 = apache::hive::service::cli::thrift;

namespace impala {

namespace {

const string THETA_A("\x01\x00\x80\xff", 4);
const string THETA_B("\x00\x03\xfe", 3);

hs2::TColumnDesc MakeColumn(const string& name) {
  hs2::TPrimitiveTypeEntry primitive;
  primitive.__set_type(hs2::TTypeId::BIGINT_TYPE);
  hs2::TTypeEntry entry;
  entry.__set_primitiveEntry(primitive);
  hs2::TColumnDesc column;
  column.__set_columnName(name);
  column.typeDesc.types.push_back(entry);
  return column;
}

hs2::TTableSchema MakeSchema(int num_columns) {
  hs2::TTableSchema schema;
  for (int i = 0; i < num_columns; ++i) {
    schema.columns.push_back(MakeColumn(Substitute("c$0", i)));
  }
  return schema;
}

hs2::TColumnValue I64(int64_t value) {
  hs2::TI64Value v;
  v.__set_value(value);
  hs2::TColumnValue result;
  result.__set_i64Val(v);
  return result;
}

hs2::TColumnValue Bytes(const string& value) {
  hs2::TStringValue v;
  v.__set_value(value);
  hs2::TColumnValue result;
  result.__set_stringVal(v);
  return result;
}

hs2::TColumnValue NullString() {
  hs2::TColumnValue result;
  result.__set_stringVal(hs2::TStringValue());
  return result;
}

hs2::TRowSet MakeRows(int num_rows, int num_columns) {
  hs2::TRowSet data;
  for (int r = 0; r < num_rows; ++r) {
    hs2::TRow row;
    for (int i = 0; i < num_columns; ++i) row.colVals.push_back(I64(r * 100 + i));
    data.rows.push_back(row);
  }
  return data;
}

TIcebergPuffinStatsParams MakePuffinStats(TIcebergStatsMode::type mode,
    const vector<string>& columns, const vector<int32_t>& theta_field_ids) {
  TIcebergPuffinStatsParams stats;
  stats.__set_mode(mode);
  stats.__set_calc_snapshot_id(42);
  map<string, int32_t> field_ids;
  for (int i = 0; i < columns.size(); ++i) field_ids[columns[i]] = i + 1;
  stats.__set_column_field_ids(field_ids);
  if (!theta_field_ids.empty()) stats.__set_theta_field_ids(theta_field_ids);
  return stats;
}

TComputeStatsParams MakeParams(const TIcebergPuffinStatsParams& stats) {
  TComputeStatsParams params;
  params.__set_iceberg_puffin_stats(stats);
  return params;
}

TComputeStatsParams FullParams() {
  return MakeParams(MakePuffinStats(TIcebergStatsMode::FULL, {"id", "userid"}, {1, 2}));
}

Status Validate(const TComputeStatsParams& params, int num_rows, int num_columns,
    int* end) {
  return CatalogOpExecutor::ValidateIcebergColStatsResult(
      params, MakeSchema(num_columns), MakeRows(num_rows, num_columns), end);
}

string ShapeError(int rows, int columns, int expected) {
  return Substitute("Iceberg column stats query returned $0 row(s) and $1 column(s), "
      "expected 1 row and $2 column(s)", rows, columns, expected);
}

void ExpectShapeError(const TComputeStatsParams& params, int num_rows, int num_columns,
    int expected) {
  int end = -1;
  Status status = Validate(params, num_rows, num_columns, &end);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(ShapeError(num_rows, num_columns, expected), status.msg().msg());
}

} // namespace

TEST(CatalogOpExecutorIcebergTest, FullResultWithThetaTailIsAccepted) {
  int end = -1;
  ASSERT_OK(Validate(FullParams(), 1, 18, &end));
  EXPECT_EQ(16, end);
}

TEST(CatalogOpExecutorIcebergTest, FullResultWithWrongColumnCountIsRejected) {
  ExpectShapeError(FullParams(), 1, 19, 18);
  ExpectShapeError(FullParams(), 1, 17, 18);
  ExpectShapeError(FullParams(), 1, 16, 18);

  int end = -1;
  Status status = CatalogOpExecutor::ValidateIcebergColStatsResult(
      FullParams(), MakeSchema(18), MakeRows(1, 17), &end);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(ShapeError(1, 18, 18), status.msg().msg());
}

TEST(CatalogOpExecutorIcebergTest, FullResultWithWrongRowCountIsRejected) {
  ExpectShapeError(FullParams(), 0, 18, 18);
  ExpectShapeError(FullParams(), 2, 18, 18);
}

TEST(CatalogOpExecutorIcebergTest, LegacyResultHasNoTail) {
  TComputeStatsParams params =
      MakeParams(MakePuffinStats(TIcebergStatsMode::LEGACY, {"id", "userid"}, {}));
  int end = -1;
  ASSERT_OK(Validate(params, 1, 16, &end));
  EXPECT_EQ(16, end);
  ExpectShapeError(params, 1, 17, 16);
}

TEST(CatalogOpExecutorIcebergTest, ThetaQueryLeavesNoTail) {
  TComputeStatsParams params = FullParams();
  params.__set_iceberg_theta_query("SELECT 1");
  int end = -1;
  ASSERT_OK(Validate(params, 1, 16, &end));
  EXPECT_EQ(16, end);
  ExpectShapeError(params, 1, 18, 16);
}

TEST(CatalogOpExecutorIcebergTest, ShapeIsNotCheckedWithoutPuffinStats) {
  TComputeStatsParams params;
  int end = -1;
  // Partitioned HDFS INCREMENTAL: 2 columns x 8 metrics, 1 partition column, 3 rows.
  ASSERT_OK(Validate(params, 3, 17, &end));
  EXPECT_EQ(17, end);
  end = -1;
  ASSERT_OK(Validate(params, 0, 0, &end));
  EXPECT_EQ(0, end);
}

TEST(CatalogOpExecutorIcebergTest, IncrementalWithoutThetaFieldsExpectsMetricsOnly) {
  TComputeStatsParams params =
      MakeParams(MakePuffinStats(TIcebergStatsMode::INCREMENTAL, {"id"}, {}));
  int end = -1;
  ASSERT_OK(Validate(params, 1, 8, &end));
  EXPECT_EQ(8, end);
  ExpectShapeError(params, 1, 10, 8);
  EXPECT_TRUE(CatalogOpExecutor::IcebergColStatsQueryOptions(params).empty());
  EXPECT_FALSE(params.iceberg_puffin_stats.__isset.theta_sketches);
}

TEST(CatalogOpExecutorIcebergTest, EmptyMetricsQueryWithoutThetaIsAccepted) {
  TComputeStatsParams params =
      MakeParams(MakePuffinStats(TIcebergStatsMode::FULL, {}, {}));
  int end = -1;
  ASSERT_OK(Validate(params, 0, 0, &end));
  EXPECT_EQ(0, end);
}

TEST(CatalogOpExecutorIcebergTest, MetricsQueryWithThetaRunsInUtc) {
  map<string, string> options = CatalogOpExecutor::IcebergColStatsQueryOptions(
      FullParams());
  EXPECT_EQ((map<string, string>{{"TIMEZONE", "UTC"}}), options);
}

TEST(CatalogOpExecutorIcebergTest, MetricsQueryWithoutThetaKeepsParentOptions) {
  TComputeStatsParams legacy =
      MakeParams(MakePuffinStats(TIcebergStatsMode::LEGACY, {"id"}, {}));
  EXPECT_TRUE(CatalogOpExecutor::IcebergColStatsQueryOptions(legacy).empty());

  TComputeStatsParams binary_only =
      MakeParams(MakePuffinStats(TIcebergStatsMode::FULL, {"payload"}, {}));
  EXPECT_TRUE(CatalogOpExecutor::IcebergColStatsQueryOptions(binary_only).empty());

  EXPECT_TRUE(
      CatalogOpExecutor::IcebergColStatsQueryOptions(TComputeStatsParams()).empty());

  TComputeStatsParams theta_query = FullParams();
  theta_query.__set_iceberg_theta_query("SELECT 1");
  EXPECT_TRUE(CatalogOpExecutor::IcebergColStatsQueryOptions(theta_query).empty());
}

TEST(CatalogOpExecutorIcebergTest, ThetaTailGoesToSketchesByFieldId) {
  TIcebergPuffinStatsParams stats =
      MakePuffinStats(TIcebergStatsMode::FULL, {"id", "userid"}, {7, 3});
  hs2::TTableSchema schema = MakeSchema(18);
  schema.columns[0].__set_columnName("id");
  schema.columns[8].__set_columnName("userid");
  hs2::TRowSet data = MakeRows(1, 16);
  hs2::TRow& row = data.rows[0];
  row.colVals[2] = hs2::TColumnValue();
  row.colVals[2].__set_i32Val(hs2::TI32Value());
  row.colVals[2].i32Val.__set_value(5);
  row.colVals[3] = hs2::TColumnValue();
  row.colVals[3].__set_doubleVal(hs2::TDoubleValue());
  row.colVals[3].doubleVal.__set_value(2.5);
  row.colVals.push_back(Bytes(THETA_A));
  row.colVals.push_back(Bytes(THETA_B));

  TAlterTableUpdateStatsParams update;
  CatalogOpExecutor::SetColumnStats(schema, data, &update, 16);
  ASSERT_OK(CatalogOpExecutor::SetIcebergThetaSketches(schema, data, 16, &stats));

  ASSERT_EQ(2, update.column_stats.size());
  const TColumnStats& id = update.column_stats["id"];
  EXPECT_EQ(0, id.num_distinct_values);
  EXPECT_EQ(1, id.num_nulls);
  EXPECT_EQ(5, id.max_size);
  EXPECT_EQ(2.5, id.avg_size);
  EXPECT_EQ(6, id.low_value.long_val);
  EXPECT_EQ(7, id.high_value.long_val);
  const TColumnStats& userid = update.column_stats["userid"];
  EXPECT_EQ(8, userid.num_distinct_values);
  EXPECT_EQ(15, userid.high_value.long_val);

  EXPECT_TRUE(stats.__isset.theta_sketches);
  EXPECT_EQ((map<int32_t, string>{{7, THETA_A}, {3, THETA_B}}), stats.theta_sketches);
}

TEST(CatalogOpExecutorIcebergTest, NullThetaSketchBecomesEmptyBytes) {
  TIcebergPuffinStatsParams stats =
      MakePuffinStats(TIcebergStatsMode::FULL, {"id", "name"}, {1, 2});
  hs2::TRowSet data = MakeRows(1, 16);
  data.rows[0].colVals.push_back(NullString());
  data.rows[0].colVals.push_back(Bytes(""));
  ASSERT_OK(CatalogOpExecutor::SetIcebergThetaSketches(
      MakeSchema(18), data, 16, &stats));
  EXPECT_EQ((map<int32_t, string>{{1, ""}, {2, ""}}), stats.theta_sketches);
}

TEST(CatalogOpExecutorIcebergTest, ThetaTailWithWrongRowShapeIsRejected) {
  TIcebergPuffinStatsParams stats =
      MakePuffinStats(TIcebergStatsMode::FULL, {"id", "name"}, {1, 2});
  Status status = CatalogOpExecutor::SetIcebergThetaSketches(
      MakeSchema(18), MakeRows(1, 17), 16, &stats);
  EXPECT_FALSE(status.ok());
  EXPECT_FALSE(stats.__isset.theta_sketches);
}

namespace {

TComputeStatsParams IncrementalThetaParams(const vector<int32_t>& theta_field_ids) {
  TComputeStatsParams params = MakeParams(MakePuffinStats(
      TIcebergStatsMode::INCREMENTAL, {"id", "userid"}, theta_field_ids));
  params.__set_iceberg_theta_query("SELECT ds_theta_sketch(...)");
  return params;
}

hs2::TRowSet ThetaRows(const vector<hs2::TColumnValue>& values) {
  hs2::TRow row;
  row.colVals = values;
  hs2::TRowSet data;
  data.rows.push_back(row);
  return data;
}

string ThetaError(const string& what, int actual, int expected) {
  return Substitute("Theta sketch query returned $0 $1, expected $2", actual, what,
      expected);
}

Status ExecWithTheta(const TComputeStatsParams& params,
    const hs2::TTableSchema& theta_schema, const hs2::TRowSet& theta_data) {
  TCatalogOpRequest request;
  request.ddl_params.__set_compute_stats_params(params);
  hs2::TTableSchema tbl_schema;
  hs2::TRowSet tbl_data;
  // Without ExecEnv any attempt to send the RPC would crash the test.
  CatalogOpExecutor executor(nullptr, nullptr, nullptr);
  return executor.ExecComputeStats(TCatalogServiceRequestHeader(), request, tbl_schema,
      tbl_data, MakeSchema(16), MakeRows(1, 16), &theta_schema, &theta_data);
}

} // namespace

TEST(CatalogOpExecutorIcebergTest, ThetaQueryResultGoesToSketchesByFieldId) {
  TIcebergPuffinStatsParams stats = IncrementalThetaParams({7, 3}).iceberg_puffin_stats;
  ASSERT_OK(CatalogOpExecutor::SetIcebergThetaQuerySketches(
      MakeSchema(2), ThetaRows({Bytes(THETA_A), Bytes(THETA_B)}), &stats));
  EXPECT_TRUE(stats.__isset.theta_sketches);
  EXPECT_EQ((map<int32_t, string>{{7, THETA_A}, {3, THETA_B}}), stats.theta_sketches);
}

TEST(CatalogOpExecutorIcebergTest, ThetaQueryNullBecomesEmptySketch) {
  TIcebergPuffinStatsParams stats = IncrementalThetaParams({7, 3}).iceberg_puffin_stats;
  ASSERT_OK(CatalogOpExecutor::SetIcebergThetaQuerySketches(
      MakeSchema(2), ThetaRows({NullString(), Bytes(THETA_B)}), &stats));
  EXPECT_EQ((map<int32_t, string>{{7, ""}, {3, THETA_B}}), stats.theta_sketches);
}

TEST(CatalogOpExecutorIcebergTest, ThetaQueryWithWrongColumnCountIsRejected) {
  for (int columns : {1, 3}) {
    TIcebergPuffinStatsParams stats =
        IncrementalThetaParams({7, 3}).iceberg_puffin_stats;
    hs2::TRowSet data;
    data.rows.push_back(hs2::TRow());
    for (int i = 0; i < columns; ++i) data.rows[0].colVals.push_back(Bytes(THETA_A));
    Status status = CatalogOpExecutor::SetIcebergThetaQuerySketches(
        MakeSchema(columns), data, &stats);
    ASSERT_FALSE(status.ok());
    EXPECT_EQ(ThetaError("columns", columns, 2), status.msg().msg());
    EXPECT_FALSE(stats.__isset.theta_sketches);
  }
}

TEST(CatalogOpExecutorIcebergTest, ThetaQueryWithWrongRowCountIsRejected) {
  for (int rows : {0, 2}) {
    TIcebergPuffinStatsParams stats =
        IncrementalThetaParams({7, 3}).iceberg_puffin_stats;
    hs2::TRowSet data;
    for (int i = 0; i < rows; ++i) {
      data.rows.push_back(ThetaRows({Bytes(THETA_A), Bytes(THETA_B)}).rows[0]);
    }
    Status status = CatalogOpExecutor::SetIcebergThetaQuerySketches(
        MakeSchema(2), data, &stats);
    ASSERT_FALSE(status.ok());
    EXPECT_EQ(ThetaError("rows", rows, 1), status.msg().msg());
    EXPECT_FALSE(stats.__isset.theta_sketches);
  }
}

TEST(CatalogOpExecutorIcebergTest, ExecComputeStatsRejectsThetaShapeBeforeRpc) {
  TComputeStatsParams params = IncrementalThetaParams({7, 3});
  Status columns = ExecWithTheta(params, MakeSchema(3),
      ThetaRows({Bytes(THETA_A), Bytes(THETA_B), Bytes(THETA_A)}));
  ASSERT_FALSE(columns.ok());
  EXPECT_EQ(ThetaError("columns", 3, 2), columns.msg().msg());

  hs2::TRowSet two_rows = ThetaRows({Bytes(THETA_A), Bytes(THETA_B)});
  two_rows.rows.push_back(two_rows.rows[0]);
  Status rows = ExecWithTheta(params, MakeSchema(2), two_rows);
  ASSERT_FALSE(rows.ok());
  EXPECT_EQ(ThetaError("rows", 2, 1), rows.msg().msg());
}

TEST(CatalogOpExecutorIcebergTest, ExecComputeStatsKeepsMetricsShapeCheckWithThetaQuery) {
  TComputeStatsParams params = IncrementalThetaParams({7, 3});
  TCatalogOpRequest request;
  request.ddl_params.__set_compute_stats_params(params);
  hs2::TTableSchema tbl_schema;
  hs2::TRowSet tbl_data;
  hs2::TTableSchema theta_schema = MakeSchema(2);
  hs2::TRowSet theta_data = ThetaRows({Bytes(THETA_A), Bytes(THETA_B)});
  CatalogOpExecutor executor(nullptr, nullptr, nullptr);
  // 18 columns would be valid for a theta tail, but not when the theta query exists.
  Status status = executor.ExecComputeStats(TCatalogServiceRequestHeader(), request,
      tbl_schema, tbl_data, MakeSchema(18), MakeRows(1, 18), &theta_schema, &theta_data);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(ShapeError(1, 18, 16), status.msg().msg());
}

TEST(CatalogOpExecutorIcebergTest, ThetaQueryResultMissingIsRejectedBeforeRpc) {
  TComputeStatsParams params = IncrementalThetaParams({7, 3});
  TCatalogOpRequest request;
  request.ddl_params.__set_compute_stats_params(params);
  hs2::TTableSchema tbl_schema;
  hs2::TRowSet tbl_data;
  CatalogOpExecutor executor(nullptr, nullptr, nullptr);
  Status status = executor.ExecComputeStats(TCatalogServiceRequestHeader(), request,
      tbl_schema, tbl_data, MakeSchema(16), MakeRows(1, 16));
  ASSERT_FALSE(status.ok());
  EXPECT_EQ("Theta sketch query result is missing", status.msg().msg());
}

TEST(CatalogOpExecutorIcebergTest, UpdateStatsRequestCopiesPuffinStats) {
  TCatalogOpRequest compute_stats;
  compute_stats.ddl_params.compute_stats_params = FullParams();
  TCatalogOpRequest request = CatalogOpExecutor::BuildUpdateStatsRequest(
      TCatalogServiceRequestHeader(), compute_stats);
  const TAlterTableUpdateStatsParams& update =
      request.ddl_params.alter_table_params.update_stats_params;
  ASSERT_TRUE(update.__isset.iceberg_puffin_stats);
  const TIcebergPuffinStatsParams& copied = update.iceberg_puffin_stats;
  const TIcebergPuffinStatsParams& source = FullParams().iceberg_puffin_stats;
  EXPECT_EQ(source.mode, copied.mode);
  EXPECT_EQ(source.calc_snapshot_id, copied.calc_snapshot_id);
  EXPECT_EQ(source.column_field_ids, copied.column_field_ids);
  EXPECT_EQ(source.theta_field_ids, copied.theta_field_ids);
  EXPECT_FALSE(update.iceberg_puffin_stats.__isset.theta_sketches);

  TCatalogOpRequest hdfs = CatalogOpExecutor::BuildUpdateStatsRequest(
      TCatalogServiceRequestHeader(), TCatalogOpRequest());
  EXPECT_FALSE(hdfs.ddl_params.alter_table_params.update_stats_params
      .__isset.iceberg_puffin_stats);
}

namespace {

TQueryOptions ParentOptions() {
  TQueryOptions parent;
  parent.__set_timezone("Europe/Moscow");
  parent.__set_debug_action("CRS_BEFORE_ADMISSION:SLEEP@10");
  parent.__set_request_pool("root.analysts");
  return parent;
}

map<string, string> ParentMap() {
  map<string, string> expected;
  TQueryOptionsToMap(ParentOptions(), &expected);
  return expected;
}

} // namespace

TEST(CatalogOpExecutorIcebergTest, ChildQueryConfOverlayAppliesUtcOverParentZone) {
  ASSERT_EQ("Europe/Moscow", ParentMap()["TIMEZONE"]);
  map<string, string> conf;
  ChildQuery::BuildConfOverlay(ParentOptions(), false,
      CatalogOpExecutor::IcebergColStatsQueryOptions(FullParams()), &conf);
  map<string, string> expected = ParentMap();
  expected.erase("DEBUG_ACTION");
  expected["TIMEZONE"] = "UTC";
  EXPECT_EQ(expected, conf);
}

TEST(CatalogOpExecutorIcebergTest, ChildQueryConfOverlayWithoutOverridesKeepsParent) {
  map<string, string> conf;
  ChildQuery::BuildConfOverlay(ParentOptions(), false, {}, &conf);
  map<string, string> expected = ParentMap();
  expected.erase("DEBUG_ACTION");
  EXPECT_EQ(expected, conf);
  EXPECT_EQ("Europe/Moscow", conf["TIMEZONE"]);
  EXPECT_EQ("root.analysts", conf["REQUEST_POOL"]);
}

TEST(CatalogOpExecutorIcebergTest, ChildQueryConfOverlayDropsFrontendRequestPool) {
  map<string, string> conf;
  ChildQuery::BuildConfOverlay(ParentOptions(), true, {}, &conf);
  map<string, string> expected = ParentMap();
  expected.erase("DEBUG_ACTION");
  expected.erase("REQUEST_POOL");
  EXPECT_EQ(expected, conf);
}

namespace {

TComputeStatsParams WithQueries(TComputeStatsParams params, bool metrics) {
  params.__set_tbl_stats_query("SELECT COUNT(*) FROM t");
  if (metrics) params.__set_col_stats_query("SELECT NDV(id) FROM t");
  return params;
}

void ExpectQuery(const ComputeStatsChildQuery& actual, const string& query,
    const string& profile_name, bool utc) {
  EXPECT_EQ(query, actual.query);
  EXPECT_EQ(profile_name, actual.profile_name);
  EXPECT_EQ(utc, actual.utc);
}

} // namespace

TEST(CatalogOpExecutorIcebergTest, ChildQueriesNoopReturnsMessageWithoutQueries) {
  TComputeStatsParams params = MakeParams(
      MakePuffinStats(TIcebergStatsMode::INCREMENTAL, {"id"}, {1}));
  params.__set_no_child_queries_message("Nothing to compute");
  ComputeStatsChildQueries result = BuildComputeStatsChildQueries(params);
  EXPECT_TRUE(result.queries.empty());
  EXPECT_EQ("Nothing to compute", result.no_queries_message);
}

TEST(CatalogOpExecutorIcebergTest, ChildQueriesFullRunsTableAndMetrics) {
  ComputeStatsChildQueries result = BuildComputeStatsChildQueries(WithQueries(
      FullParams(), true));
  ASSERT_EQ(2, result.queries.size());
  ExpectQuery(result.queries[0], "SELECT COUNT(*) FROM t", "Table Stats Query", false);
  ExpectQuery(result.queries[1], "SELECT NDV(id) FROM t", "Column Stats Query", true);
}

TEST(CatalogOpExecutorIcebergTest, ChildQueriesLegacyKeepsParentOptions) {
  ComputeStatsChildQueries result = BuildComputeStatsChildQueries(WithQueries(
      MakeParams(MakePuffinStats(TIcebergStatsMode::LEGACY, {"id"}, {})), true));
  ASSERT_EQ(2, result.queries.size());
  ExpectQuery(result.queries[0], "SELECT COUNT(*) FROM t", "Table Stats Query", false);
  ExpectQuery(result.queries[1], "SELECT NDV(id) FROM t", "Column Stats Query", false);
}

TEST(CatalogOpExecutorIcebergTest, ChildQueriesIncrementalWithTailRunsTwoQueries) {
  ComputeStatsChildQueries result = BuildComputeStatsChildQueries(WithQueries(
      MakeParams(MakePuffinStats(TIcebergStatsMode::INCREMENTAL, {"id"}, {1})), true));
  ASSERT_EQ(2, result.queries.size());
  EXPECT_FALSE(result.queries[0].utc);
  EXPECT_TRUE(result.queries[1].utc);
}

TEST(CatalogOpExecutorIcebergTest, ChildQueriesIncrementalWithoutThetaKeepsParentOptions) {
  ComputeStatsChildQueries result = BuildComputeStatsChildQueries(WithQueries(
      MakeParams(MakePuffinStats(TIcebergStatsMode::INCREMENTAL, {"id"}, {})), true));
  ASSERT_EQ(2, result.queries.size());
  ExpectQuery(result.queries[0], "SELECT COUNT(*) FROM t", "Table Stats Query", false);
  ExpectQuery(result.queries[1], "SELECT NDV(id) FROM t", "Column Stats Query", false);
  EXPECT_EQ(0, result.table_index);
  EXPECT_EQ(1, result.col_stats_index);
  EXPECT_EQ(-1, result.theta_index);
}

TEST(CatalogOpExecutorIcebergTest, ChildQueriesIncrementalWithThetaQueryRunsThree) {
  TComputeStatsParams params = WithQueries(IncrementalThetaParams({1, 2}), true);
  ComputeStatsChildQueries result = BuildComputeStatsChildQueries(params);
  ASSERT_EQ(3, result.queries.size());
  ExpectQuery(result.queries[0], "SELECT COUNT(*) FROM t", "Table Stats Query", false);
  ExpectQuery(result.queries[1], "SELECT NDV(id) FROM t", "Column Stats Query", false);
  ExpectQuery(result.queries[2], "SELECT ds_theta_sketch(...)", "Theta Sketch Query",
      true);
  EXPECT_EQ(1, result.col_stats_index);
  EXPECT_EQ(2, result.theta_index);
}

TEST(CatalogOpExecutorIcebergTest, ChildQueriesThetaQueryWithoutMetricsRunsTwo) {
  TComputeStatsParams params = WithQueries(IncrementalThetaParams({1, 2}), false);
  ComputeStatsChildQueries result = BuildComputeStatsChildQueries(params);
  ASSERT_EQ(2, result.queries.size());
  EXPECT_EQ("Table Stats Query", result.queries[0].profile_name);
  ExpectQuery(result.queries[1], "SELECT ds_theta_sketch(...)", "Theta Sketch Query",
      true);
  EXPECT_EQ(-1, result.col_stats_index);
  EXPECT_EQ(1, result.theta_index);
}

TEST(CatalogOpExecutorIcebergTest, ChildQueriesFullWithoutColumnsRunsTableOnly) {
  ComputeStatsChildQueries result = BuildComputeStatsChildQueries(WithQueries(
      MakeParams(MakePuffinStats(TIcebergStatsMode::FULL, {}, {})), false));
  ASSERT_EQ(1, result.queries.size());
  ExpectQuery(result.queries[0], "SELECT COUNT(*) FROM t", "Table Stats Query", false);
}

TEST(CatalogOpExecutorIcebergTest, ChildQueriesHdfsRunsUpToTwoQueries) {
  ComputeStatsChildQueries both =
      BuildComputeStatsChildQueries(WithQueries(TComputeStatsParams(), true));
  ASSERT_EQ(2, both.queries.size());
  ExpectQuery(both.queries[0], "SELECT COUNT(*) FROM t", "Table Stats Query", false);
  ExpectQuery(both.queries[1], "SELECT NDV(id) FROM t", "Column Stats Query", false);

  ComputeStatsChildQueries table_only =
      BuildComputeStatsChildQueries(WithQueries(TComputeStatsParams(), false));
  ASSERT_EQ(1, table_only.queries.size());
  EXPECT_FALSE(table_only.queries[0].utc);
}

TEST(CatalogOpExecutorIcebergTest, ChildQueriesHdfsWithoutQueriesKeepsLegacyMessage) {
  ComputeStatsChildQueries result = BuildComputeStatsChildQueries(TComputeStatsParams());
  EXPECT_TRUE(result.queries.empty());
  EXPECT_EQ("No partitions selected for incremental stats update.",
      result.no_queries_message);
}

} // namespace impala
