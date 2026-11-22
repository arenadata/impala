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

#include <algorithm>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <limits>
#include <map>
#include <memory>
#include <sstream>
#include <boost/algorithm/string.hpp>

#include "exprs/aggregate-functions.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/runtime-state.h"
#include "runtime/test-env.h"
#include "runtime/timestamp-value.h"
#include "runtime/timestamp-value.inline.h"
#include "testutil/gtest-util.h"
#include "thirdparty/datasketches/theta_sketch.hpp"
#include "udf/udf.h"
#include "udf/udf-test-harness.h"

#include "common/names.h"

using boost::algorithm::is_any_of;
using boost::algorithm::split;
using namespace impala_udf;

namespace impala {

namespace {

// Nominal entries of the sketches built by ds_theta_sketch_iceberg.
const size_t THETA_K = 4096;
const uint64_t THETA_MAX = std::numeric_limits<int64_t>::max();

struct IcebergVectorValue {
  bool is_null = false;
  // Value of every type but STRING; FLOAT and DOUBLE keep their IEEE bits.
  __int128_t num = 0;
  string bytes;
};

struct IcebergVectorCase {
  string name;
  string type_name;
  FunctionContext::TypeDesc type{FunctionContext::TYPE_INT, 0, 0, 0};
  int first_line = 0;
  // 0 until the S line of the case is read.
  int sketch_line = 0;
  vector<IcebergVectorValue> values;
  string sketch;
};

bool IsCaseName(const string& s) {
  if (s.empty()) return false;
  for (char c : s) {
    if (!(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'z') && c != '_' && c != '-') {
      return false;
    }
  }
  return true;
}

// Accepts -?[0-9]{1,38}, which always fits into 128 bits.
bool ParseDecimalDigits(const string& s, __int128_t* out) {
  size_t pos = s.size() > 0 && s[0] == '-' ? 1 : 0;
  if (s.size() == pos || s.size() - pos > 38) return false;
  __int128_t v = 0;
  for (size_t i = pos; i < s.size(); ++i) {
    if (s[i] < '0' || s[i] > '9') return false;
    v = v * 10 + (s[i] - '0');
  }
  *out = pos == 1 ? -v : v;
  return true;
}

// Lower-case hex of at least one byte.
bool ParseHex(const string& s, string* out) {
  if (s.empty() || s.size() % 2 != 0) return false;
  out->clear();
  for (size_t i = 0; i < s.size(); i += 2) {
    int byte = 0;
    for (size_t j = i; j < i + 2; ++j) {
      char c = s[j];
      int digit;
      if (c >= '0' && c <= '9') {
        digit = c - '0';
      } else if (c >= 'a' && c <= 'f') {
        digit = c - 'a' + 10;
      } else {
        return false;
      }
      byte = byte * 16 + digit;
    }
    out->push_back(static_cast<char>(byte));
  }
  return true;
}

bool ParseType(const string& s, FunctionContext::TypeDesc* type) {
  static const std::map<string, FunctionContext::Type> SIMPLE_TYPES = {
      {"BOOLEAN", FunctionContext::TYPE_BOOLEAN}, {"INT", FunctionContext::TYPE_INT},
      {"BIGINT", FunctionContext::TYPE_BIGINT}, {"FLOAT", FunctionContext::TYPE_FLOAT},
      {"DOUBLE", FunctionContext::TYPE_DOUBLE}, {"DATE", FunctionContext::TYPE_DATE},
      {"TIMESTAMP", FunctionContext::TYPE_TIMESTAMP},
      {"STRING", FunctionContext::TYPE_STRING}};
  *type = FunctionContext::TypeDesc{FunctionContext::TYPE_INT, 0, 0, 0};
  auto it = SIMPLE_TYPES.find(s);
  if (it != SIMPLE_TYPES.end()) {
    type->type = it->second;
    return true;
  }
  const string prefix = "DECIMAL(";
  if (s.compare(0, prefix.size(), prefix) != 0 || s.back() != ')') return false;
  vector<string> parts;
  split(parts, s.substr(prefix.size(), s.size() - prefix.size() - 1), is_any_of(","));
  __int128_t precision, scale;
  if (parts.size() != 2 || parts[0][0] == '-' || parts[1][0] == '-'
      || !ParseDecimalDigits(parts[0], &precision)
      || !ParseDecimalDigits(parts[1], &scale)
      || precision < 1 || precision > 38 || scale > precision) {
    return false;
  }
  type->type = FunctionContext::TYPE_DECIMAL;
  type->precision = static_cast<int>(precision);
  type->scale = static_cast<int>(scale);
  return true;
}

bool InIntRange(__int128_t v, const FunctionContext::TypeDesc& type) {
  switch (type.type) {
    case FunctionContext::TYPE_INT:
    case FunctionContext::TYPE_DATE:
      return v >= std::numeric_limits<int32_t>::min()
          && v <= std::numeric_limits<int32_t>::max();
    case FunctionContext::TYPE_BIGINT:
    case FunctionContext::TYPE_TIMESTAMP:
      return v >= std::numeric_limits<int64_t>::min()
          && v <= std::numeric_limits<int64_t>::max();
    case FunctionContext::TYPE_DECIMAL: {
      __int128_t limit = 1;
      for (int i = 0; i < type.precision; ++i) limit *= 10;
      return v > -limit && v < limit;
    }
    default:
      return false;
  }
}

// Parses 'text' written in the vectors notation for 'type' into 'value'.
bool ParseVectorValue(const string& text, const FunctionContext::TypeDesc& type,
    IcebergVectorValue* value) {
  *value = IcebergVectorValue();
  if (text == "NULL") {
    value->is_null = true;
    return true;
  }
  switch (type.type) {
    case FunctionContext::TYPE_BOOLEAN:
      if (text != "true" && text != "false") return false;
      value->num = text == "true";
      return true;
    case FunctionContext::TYPE_FLOAT:
    case FunctionContext::TYPE_DOUBLE: {
      size_t width = type.type == FunctionContext::TYPE_FLOAT ? 4 : 8;
      string bits;
      if (text.size() != 2 + 2 * width || text.compare(0, 2, "0x") != 0
          || !ParseHex(text.substr(2), &bits)) {
        return false;
      }
      uint64_t v = 0;
      for (char c : bits) v = (v << 8) | static_cast<uint8_t>(c);
      value->num = v;
      return true;
    }
    case FunctionContext::TYPE_STRING:
      return text == "-" || ParseHex(text, &value->bytes);
    default:
      return ParseDecimalDigits(text, &value->num) && InIntRange(value->num, type);
  }
}

// Reads the vectors file. Returns an empty string on success, otherwise the first
// problem as "line N: reason".
string ParseIcebergVectors(std::istream& in, vector<IcebergVectorCase>* cases) {
  std::map<string, size_t> index;
  cases->clear();
  string line;
  int line_no = 0;
  auto error = [&line_no](const string& reason) {
    return "line " + std::to_string(line_no) + ": " + reason;
  };
  while (std::getline(in, line)) {
    ++line_no;
    if (line.empty() || line[0] == '#') continue;
    vector<string> fields;
    split(fields, line, is_any_of("\t"));
    const string& kind = fields[0];
    size_t expected_fields = kind == "S" ? 3 : 5;
    if (kind != "V" && kind != "G" && kind != "S") return error("unknown record " + kind);
    if (fields.size() != expected_fields) {
      return error(kind + " needs " + std::to_string(expected_fields)
          + " TAB-separated fields, got " + std::to_string(fields.size()));
    }
    const string& name = fields[1];
    if (!IsCaseName(name)) return error("bad case name '" + name + "'");
    auto it = index.find(name);
    if (it == index.end()) {
      it = index.emplace(name, cases->size()).first;
      cases->emplace_back();
      cases->back().name = name;
      cases->back().first_line = line_no;
    }
    IcebergVectorCase& c = (*cases)[it->second];
    if (c.sketch_line != 0) {
      return error("case " + name + " goes on after its S line "
          + std::to_string(c.sketch_line));
    }
    if (kind == "S") {
      if (!ParseHex(fields[2], &c.sketch)) return error("bad sketch hex");
      c.sketch_line = line_no;
      continue;
    }
    FunctionContext::TypeDesc type;
    if (!ParseType(fields[2], &type)) return error("unknown type '" + fields[2] + "'");
    if (!c.type_name.empty() && c.type_name != fields[2]) {
      return error("case " + name + " has type " + c.type_name + ", not " + fields[2]);
    }
    c.type_name = fields[2];
    c.type = type;
    if (kind == "V") {
      IcebergVectorValue value;
      if (!ParseVectorValue(fields[3], type, &value)) {
        return error("bad " + fields[2] + " value '" + fields[3] + "'");
      }
      string ignored;
      if (fields[4] != "-" && !ParseHex(fields[4], &ignored)) {
        return error("bad Iceberg bytes hex");
      }
      c.values.push_back(value);
      continue;
    }
    __int128_t start, count;
    if (type.type != FunctionContext::TYPE_INT
        && type.type != FunctionContext::TYPE_BIGINT) {
      return error("G needs INT or BIGINT, not " + fields[2]);
    }
    if (!ParseDecimalDigits(fields[3], &start) || !ParseDecimalDigits(fields[4], &count)
        || count < 0 || count > 1000000 || !InIntRange(start, type)
        || (count > 0 && !InIntRange(start + count - 1, type))) {
      return error("bad G range " + fields[3] + " " + fields[4]);
    }
    for (__int128_t v = start; v < start + count; ++v) {
      IcebergVectorValue value;
      value.num = v;
      c.values.push_back(value);
    }
  }
  if (in.bad()) return error("read failed");
  for (const IcebergVectorCase& c : *cases) {
    if (c.sketch_line == 0) {
      line_no = c.first_line;
      return error("case " + c.name + " has no S line");
    }
  }
  return "";
}

struct ContextCloser {
  void operator()(FunctionContext* ctx) const {
    UdfTestHarness::CloseContext(ctx);
    EXPECT_FALSE(ctx->has_error()) << ctx->error_msg();
    delete ctx;
  }
};
using ScopedContext = std::unique_ptr<FunctionContext, ContextCloser>;

// Query state and memory of the contexts the vectors test creates. The DECIMAL update
// reads the byte size of the argument type through the context, which needs both the
// argument types, missing from UdaTestHarness contexts, and a runtime state.
struct IcebergThetaEnv {
  RuntimeState* state;
  MemPool* pool;
};

ScopedContext NewIcebergThetaContext(
    const IcebergThetaEnv& env, const IcebergVectorCase& c) {
  FunctionContext::TypeDesc return_type{FunctionContext::TYPE_STRING, 0, 0, 0};
  return ScopedContext(
      UdfTestHarness::CreateTestContext(return_type, {c.type}, env.state, env.pool));
}

StringVal AsStringVal(const string& s) {
  return StringVal(reinterpret_cast<uint8_t*>(const_cast<char*>(s.data())), s.size());
}

string CopyResult(FunctionContext* ctx, const StringVal& v) {
  EXPECT_FALSE(ctx->has_error()) << ctx->error_msg();
  EXPECT_FALSE(v.is_null);
  return v.is_null ? string() : string(reinterpret_cast<const char*>(v.ptr), v.len);
}

// Calls the update function that ds_theta_sketch_iceberg registers for the case type.
void UpdateIceberg(FunctionContext* ctx, const IcebergVectorCase& c,
    const IcebergVectorValue& v, StringVal* dst) {
  switch (c.type.type) {
    case FunctionContext::TYPE_BOOLEAN:
      AggregateFunctions::DsThetaUpdateIceberg<BooleanVal>(
          ctx, v.is_null ? BooleanVal::null() : BooleanVal(v.num != 0), dst);
      break;
    case FunctionContext::TYPE_INT:
      AggregateFunctions::DsThetaUpdateIceberg<IntVal>(
          ctx, v.is_null ? IntVal::null() : IntVal(static_cast<int32_t>(v.num)), dst);
      break;
    case FunctionContext::TYPE_BIGINT:
      AggregateFunctions::DsThetaUpdate<BigIntVal>(ctx,
          v.is_null ? BigIntVal::null() : BigIntVal(static_cast<int64_t>(v.num)), dst);
      break;
    case FunctionContext::TYPE_FLOAT: {
      uint32_t bits = static_cast<uint32_t>(v.num);
      float f;
      memcpy(&f, &bits, sizeof(f));
      AggregateFunctions::DsThetaUpdateIceberg<FloatVal>(
          ctx, v.is_null ? FloatVal::null() : FloatVal(f), dst);
      break;
    }
    case FunctionContext::TYPE_DOUBLE: {
      uint64_t bits = static_cast<uint64_t>(v.num);
      double d;
      memcpy(&d, &bits, sizeof(d));
      AggregateFunctions::DsThetaUpdateIceberg<DoubleVal>(
          ctx, v.is_null ? DoubleVal::null() : DoubleVal(d), dst);
      break;
    }
    case FunctionContext::TYPE_DATE:
      AggregateFunctions::DsThetaUpdateIceberg<DateVal>(
          ctx, v.is_null ? DateVal::null() : DateVal(static_cast<int32_t>(v.num)), dst);
      break;
    case FunctionContext::TYPE_TIMESTAMP: {
      TimestampVal tv = TimestampVal::null();
      if (!v.is_null) {
        TimestampValue::UtcFromUnixTimeMicros(static_cast<int64_t>(v.num))
            .ToTimestampVal(&tv);
      }
      AggregateFunctions::DsThetaUpdateIceberg<TimestampVal>(ctx, tv, dst);
      break;
    }
    case FunctionContext::TYPE_DECIMAL: {
      DecimalVal dv = DecimalVal::null();
      if (!v.is_null && c.type.precision <= 9) {
        dv = DecimalVal(static_cast<int32_t>(v.num));
      } else if (!v.is_null && c.type.precision <= 18) {
        dv = DecimalVal(static_cast<int64_t>(v.num));
      } else if (!v.is_null) {
        dv = DecimalVal(v.num);
      }
      AggregateFunctions::DsThetaUpdateIceberg<DecimalVal>(ctx, dv, dst);
      break;
    }
    case FunctionContext::TYPE_STRING:
      AggregateFunctions::DsThetaUpdate<StringVal>(
          ctx, v.is_null ? StringVal::null() : AsStringVal(v.bytes), dst);
      break;
    default:
      ADD_FAILURE() << "no update function for " << c.type_name;
  }
}

// Single phase: Init, Update of all values, Finalize.
string RunIcebergThetaUpdate(const IcebergThetaEnv& env, const IcebergVectorCase& c) {
  ScopedContext ctx = NewIcebergThetaContext(env, c);
  StringVal state;
  AggregateFunctions::DsThetaInit(ctx.get(), &state);
  for (const IcebergVectorValue& v : c.values) UpdateIceberg(ctx.get(), c, v, &state);
  return CopyResult(
      ctx.get(), AggregateFunctions::DsThetaFinalizeSketch(ctx.get(), state));
}

// Splits the values into 'num_parts' consecutive parts of ceil(n / num_parts) values,
// the trailing parts possibly empty, and returns the serialized state of each part.
vector<string> SerializeIcebergThetaParts(
    const IcebergThetaEnv& env, const IcebergVectorCase& c, size_t num_parts) {
  size_t n = c.values.size();
  size_t part_size = (n + num_parts - 1) / num_parts;
  vector<string> parts;
  for (size_t p = 0; p < num_parts; ++p) {
    ScopedContext ctx = NewIcebergThetaContext(env, c);
    StringVal state;
    AggregateFunctions::DsThetaInit(ctx.get(), &state);
    size_t end = std::min(n, (p + 1) * part_size);
    for (size_t i = std::min(n, p * part_size); i < end; ++i) {
      UpdateIceberg(ctx.get(), c, c.values[i], &state);
    }
    parts.push_back(
        CopyResult(ctx.get(), AggregateFunctions::DsThetaSerialize(ctx.get(), state)));
  }
  return parts;
}

// Merges serialized states into a fresh one; returns it finalized as a sketch or, for
// the next merge level, serialized.
string MergeIcebergTheta(const IcebergThetaEnv& env, const IcebergVectorCase& c,
    vector<string>::const_iterator begin, vector<string>::const_iterator end,
    bool finalize) {
  ScopedContext ctx = NewIcebergThetaContext(env, c);
  StringVal state;
  AggregateFunctions::DsThetaInit(ctx.get(), &state);
  for (auto it = begin; it != end; ++it) {
    AggregateFunctions::DsThetaMerge(ctx.get(), AsStringVal(*it), &state);
  }
  return CopyResult(ctx.get(), finalize ?
      AggregateFunctions::DsThetaFinalizeSketch(ctx.get(), state) :
      AggregateFunctions::DsThetaSerialize(ctx.get(), state));
}

// Rewrites a compact sketch to the canonical form of its hash set: sorted entries,
// cut to the THETA_K smallest with theta set to the next one, laid out by content. The
// update sketch alone keeps up to 15/16 of 2 * THETA_K entries before it rebuilds.
string CanonicalThetaSketch(const string& bytes) {
  auto sketch =
      datasketches::compact_theta_sketch::deserialize(bytes.data(), bytes.size());
  vector<uint64_t> hashes;
  for (uint64_t hash : sketch) hashes.push_back(hash);
  std::sort(hashes.begin(), hashes.end());
  uint64_t theta = sketch.get_theta64();
  if (hashes.size() > THETA_K) {
    theta = hashes[THETA_K];
    hashes.resize(THETA_K);
  }
  const uint16_t seed_hash = compute_seed_hash(datasketches::DEFAULT_SEED);
  // Header bytes: preamble longs, serial version 3, theta family 3, two unused bytes,
  // flags (read-only, compact, ordered; empty if no entries) and the seed hash.
  uint8_t pre_longs = hashes.empty() || (hashes.size() == 1 && theta == THETA_MAX) ? 1 :
      theta == THETA_MAX ? 2 : 3;
  uint8_t flags = hashes.empty() ? 0x1e : 0x1a;
  string out = {static_cast<char>(pre_longs), 3, 3, 0, 0, static_cast<char>(flags)};
  // Values are little-endian, as on the hosts Impala runs on.
  auto append = [&out](const void* p, size_t len) {
    out.append(static_cast<const char*>(p), len);
  };
  append(&seed_hash, sizeof(seed_hash));
  if (pre_longs > 1) {
    uint32_t count = static_cast<uint32_t>(hashes.size());
    uint32_t unused = 0;
    append(&count, sizeof(count));
    append(&unused, sizeof(unused));
  }
  if (pre_longs > 2) append(&theta, sizeof(theta));
  append(hashes.data(), hashes.size() * sizeof(uint64_t));
  return out;
}

string ToHex(const string& bytes) {
  std::stringstream ss;
  ss << std::hex << std::setfill('0');
  for (char c : bytes) ss << std::setw(2) << static_cast<int>(static_cast<uint8_t>(c));
  return ss.str();
}

// Empty if the bytes are equal, otherwise where they first differ.
string DescribeSketchMismatch(const string& expected, const string& actual) {
  if (expected == actual) return "";
  size_t pos = std::mismatch(expected.begin(),
      expected.begin() + std::min(expected.size(), actual.size()), actual.begin()).first
      - expected.begin();
  std::stringstream ss;
  ss << "expected " << expected.size() << " bytes, got " << actual.size()
     << "; from byte " << pos << " expected " << ToHex(expected.substr(pos, 24))
     << ", got " << ToHex(actual.substr(pos, 24));
  return ss.str();
}

} // namespace

// Checks ds_theta_sketch_iceberg against the sketches of the shared vectors file, which
// the frontend test checks against DataSketches Java and the Iceberg encoding.
// The merge path, as used by plans with an exchange, must reproduce them byte for byte
// however the input is split; the single-phase result only after canonicalization.
TEST(DsThetaSketch, IcebergVectors) {
  const char* impala_home = getenv("IMPALA_HOME");
  ASSERT_TRUE(impala_home != nullptr) << "IMPALA_HOME is not set";
  const string path = string(impala_home) + "/testdata/iceberg_theta_vectors/vectors.txt";
  std::ifstream in(path);
  ASSERT_TRUE(in.is_open()) << "cannot read " << path;
  vector<IcebergVectorCase> cases;
  const string parse_error = ParseIcebergVectors(in, &cases);
  ASSERT_EQ("", parse_error) << path;

  TestEnv test_env;
  ASSERT_OK(test_env.Init());
  RuntimeState* runtime_state;
  ASSERT_OK(test_env.CreateQueryState(0, nullptr, &runtime_state));
  MemTracker tracker;
  MemPool pool(&tracker);
  const IcebergThetaEnv env{runtime_state, &pool};

  vector<string> required = {"empty", "all-null", "string-empty", "bigint-estimation",
      "int-estimation"};
  for (const char* t : {"boolean", "int", "bigint", "float", "double", "date",
           "timestamp", "decimal9", "decimal18", "decimal38", "string"}) {
    required.push_back(string(t) + "-basic");
    required.push_back(string(t) + "-single");
  }
  for (const string& name : required) {
    EXPECT_TRUE(std::any_of(cases.begin(), cases.end(),
        [&name](const IcebergVectorCase& c) { return c.name == name; }))
        << path << ": no case " << name;
  }

  for (const IcebergVectorCase& c : cases) {
    const string where = path + ":" + std::to_string(c.sketch_line) + " (" + c.name + ")";
    string mismatch =
        DescribeSketchMismatch(c.sketch, CanonicalThetaSketch(RunIcebergThetaUpdate(env, c)));
    EXPECT_EQ("", mismatch) << where << ": UPDATE, canonicalized";
    for (size_t num_parts : {1, 2, 10}) {
      const vector<string> parts = SerializeIcebergThetaParts(env, c, num_parts);
      mismatch = DescribeSketchMismatch(
          c.sketch, MergeIcebergTheta(env, c, parts.begin(), parts.end(), true));
      EXPECT_EQ("", mismatch) << where << ": MERGE of " << num_parts << " parts";
      auto middle = parts.begin() + (parts.size() + 1) / 2;
      const vector<string> level1 = {MergeIcebergTheta(env, c, parts.begin(), middle, false),
          MergeIcebergTheta(env, c, middle, parts.end(), false)};
      mismatch = DescribeSketchMismatch(
          c.sketch, MergeIcebergTheta(env, c, level1.begin(), level1.end(), true));
      EXPECT_EQ("", mismatch)
          << where << ": two-level MERGE of " << num_parts << " parts";
    }
  }
  pool.FreeAll();
}

TEST(DsThetaSketch, IcebergVectorsMalformedLines) {
  struct {
    string text;
    string error;
  } bad_files[] = {
      {"# comment\n\nV\tc\tINT\t1\n", "line 3: V needs 5"},
      {"X\tc\t00\n", "line 1: unknown record X"},
      {"V\tC\tINT\t1\t01000000\n", "line 1: bad case name"},
      {"V\tc\tINT(4)\t1\t01000000\n", "line 1: unknown type"},
      {"V\tc\tDECIMAL(39,2)\t1\t01\n", "line 1: unknown type"},
      {"V\tc\tINT\tone\t01000000\n", "line 1: bad INT value"},
      {"V\tc\tINT\t2147483648\t-\n", "line 1: bad INT value"},
      {"V\tc\tDECIMAL(9,2)\t1000000000\t-\n", "line 1: bad DECIMAL(9,2) value"},
      {"V\tc\tFLOAT\t0x3F800000\t-\n", "line 1: bad FLOAT value"},
      {"V\tc\tSTRING\t6\t-\n", "line 1: bad STRING value"},
      {"V\tc\tBOOLEAN\tTRUE\t-\n", "line 1: bad BOOLEAN value"},
      {"V\tc\tINT\t1\t0x01\n", "line 1: bad Iceberg bytes"},
      {"G\tc\tSTRING\t1\t2\n", "line 1: G needs INT or BIGINT"},
      {"G\tc\tINT\t2147483647\t2\n", "line 1: bad G range"},
      {"V\tc\tINT\t1\t01000000\nS\tc\t0G\n", "line 2: bad sketch hex"},
      {"V\tc\tINT\t1\t01000000\nV\tc\tBIGINT\t1\t-\n", "line 2: case c has type INT"},
      {"V\tc\tINT\t1\t01000000\n", "line 1: case c has no S line"},
      {"S\tc\t01030300001ecc93\nS\tc\t01030300001ecc93\n", "line 2: case c goes on"},
      {"S\tc\t01030300001ecc93\nV\tc\tINT\t1\t-\n", "line 2: case c goes on"}};
  for (const auto& bad : bad_files) {
    std::istringstream in(bad.text);
    vector<IcebergVectorCase> cases;
    const string error = ParseIcebergVectors(in, &cases);
    EXPECT_TRUE(boost::algorithm::starts_with(error, bad.error))
        << "'" << bad.text << "' gave '" << error << "'";
  }

  vector<IcebergVectorCase> cases;
  std::istringstream good("V\tc\tSTRING\t-\t-\nV\tc\tSTRING\tNULL\t-\n"
                          "S\tc\t01030300001ecc93\nG\td\tINT\t-1\t2\nS\td\t00\n");
  ASSERT_EQ("", ParseIcebergVectors(good, &cases));
  ASSERT_EQ(2u, cases.size());
  EXPECT_EQ(3, cases[0].sketch_line);
  EXPECT_EQ(2u, cases[0].values.size());
  EXPECT_TRUE(cases[0].values[0].bytes.empty() && !cases[0].values[0].is_null);
  EXPECT_TRUE(cases[0].values[1].is_null);
  EXPECT_EQ(2u, cases[1].values.size());
  EXPECT_TRUE(cases[1].values[0].num == -1 && cases[1].values[1].num == 0);
}

}
