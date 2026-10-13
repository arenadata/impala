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

#include <string>

#include "runtime/io/s3-native-client.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

namespace impala {

// Parses 'path' and expects 'expected_bucket' and 'expected_key'.
static void ExpectParsed(const string& path, const string& expected_bucket,
    const string& expected_key) {
  string bucket;
  string key;
  Status status = ParseS3Path(path, &bucket, &key);
  ASSERT_TRUE(status.ok()) << path << ": " << status.GetDetail();
  EXPECT_EQ(expected_bucket, bucket) << path;
  EXPECT_EQ(expected_key, key) << path;
}

static void ExpectNotParsed(const string& path) {
  string bucket;
  string key;
  EXPECT_FALSE(ParseS3Path(path, &bucket, &key).ok()) << path;
}

TEST(S3NativeTest, ParseS3Path) {
  ExpectParsed("s3a://bucket/key", "bucket", "key");
  ExpectParsed("s3a://bucket/db.db/t/000000_0.parq", "bucket", "db.db/t/000000_0.parq");
  ExpectParsed("s3://bucket/key", "bucket", "key");
  ExpectParsed("s3n://bucket/key", "bucket", "key");
  // A key that ends with a slash is passed on as is.
  ExpectParsed("s3a://bucket/dir/", "bucket", "dir/");

  ExpectNotParsed("");
  ExpectNotParsed("s3a://");
  ExpectNotParsed("s3a://bucket");
  ExpectNotParsed("s3a://bucket/");
  ExpectNotParsed("s3a:///key");
  ExpectNotParsed("hdfs://namenode:8020/key");
  ExpectNotParsed("/warehouse/t/key");
  ExpectNotParsed("S3A://bucket/key");
}

// Keys are literal object keys: nothing is percent-decoded, and spaces, '+' and UTF-8
// are kept. The SDK encodes the key once when it builds the request URI.
TEST(S3NativeTest, ParseS3PathKeepsKeyLiteral) {
  ExpectParsed("s3a://bucket/t/p=a%3Ab/f.parq", "bucket", "t/p=a%3Ab/f.parq");
  ExpectParsed("s3a://bucket/t/p=a%20b/f.parq", "bucket", "t/p=a%20b/f.parq");
  ExpectParsed("s3a://bucket/t/p=a b/f.parq", "bucket", "t/p=a b/f.parq");
  ExpectParsed("s3a://bucket/t/p=a+b/f.parq", "bucket", "t/p=a+b/f.parq");
  ExpectParsed("s3a://bucket/t/p=\xd0\xb4\xd0\xb0/f.parq", "bucket",
      "t/p=\xd0\xb4\xd0\xb0/f.parq");
}

TEST(S3NativeTest, ConnectionPoolSize) {
  EXPECT_EQ(64, S3NativeConnectionPoolSize(64, 16));
  EXPECT_EQ(64, S3NativeConnectionPoolSize(64, 64));
  EXPECT_EQ(100, S3NativeConnectionPoolSize(64, 100));
  EXPECT_EQ(16, S3NativeConnectionPoolSize(0, 16));
}

// Calls CheckS3RangeRead() and expects success with 'expected_bytes_read' and
// 'expected_eof'.
static void ExpectRead(int64_t requested, int64_t written, int64_t content_length,
    int64_t expected_bytes_read, bool expected_eof) {
  int64_t bytes_read = -1;
  bool eof = !expected_eof;
  ASSERT_OK(CheckS3RangeRead(requested, written, content_length, &bytes_read, &eof));
  EXPECT_EQ(expected_bytes_read, bytes_read);
  EXPECT_EQ(expected_eof, eof);
}

static void ExpectReadError(int64_t requested, int64_t written, int64_t content_length) {
  int64_t bytes_read = -1;
  bool eof = true;
  EXPECT_FALSE(
      CheckS3RangeRead(requested, written, content_length, &bytes_read, &eof).ok());
  EXPECT_EQ(0, bytes_read);
  EXPECT_FALSE(eof);
}

TEST(S3NativeTest, CheckS3RangeRead) {
  // Full range, with and without Content-Length (0 = header absent, e.g. chunked).
  ExpectRead(1024, 1024, 1024, 1024, false);
  ExpectRead(1024, 1024, 0, 1024, false);
  // The range went past the end of the object: short body, end of file.
  ExpectRead(1024, 100, 100, 100, true);
  ExpectRead(1024, 100, 0, 100, true);
  // Chunked response with an empty body: nothing read, end of file.
  ExpectRead(1024, 0, 0, 0, true);

  // The stream failed.
  ExpectReadError(1024, -1, 1024);
  // More than requested (the server ignored the Range header).
  ExpectReadError(1024, 2048, 2048);
  // Body and Content-Length disagree.
  ExpectReadError(1024, 100, 1024);
  ExpectReadError(1024, 1024, 100);
}

} // namespace impala
