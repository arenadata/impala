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

#include <istream>
#include <ostream>
#include <string>
#include <vector>

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

TEST(S3NativeTest, ReadPartSize) {
  const int64_t MiB = 1024 * 1024;
  // Large reads are split into parts.
  EXPECT_EQ(MiB, S3ReadPartSize(8 * MiB, 2 * MiB, MiB));
  EXPECT_EQ(MiB, S3ReadPartSize(2 * MiB, 2 * MiB, MiB));
  // Smaller than the threshold: one request.
  EXPECT_EQ(2 * MiB - 1, S3ReadPartSize(2 * MiB - 1, 2 * MiB, MiB));
  EXPECT_EQ(100, S3ReadPartSize(100, 2 * MiB, MiB));
  // Not larger than one part: one request.
  EXPECT_EQ(MiB, S3ReadPartSize(MiB, 512 * 1024, MiB));
  // Splitting off.
  EXPECT_EQ(8 * MiB, S3ReadPartSize(8 * MiB, 0, MiB));
  EXPECT_EQ(8 * MiB, S3ReadPartSize(8 * MiB, 2 * MiB, 0));
}

TEST(S3NativeTest, ReadsWholeFile) {
  const int64_t MiB = 1024 * 1024;
  EXPECT_TRUE(S3ReadsWholeFile(1, 2 * MiB));
  EXPECT_TRUE(S3ReadsWholeFile(2 * MiB, 2 * MiB));
  EXPECT_FALSE(S3ReadsWholeFile(2 * MiB + 1, 2 * MiB));
  // Unknown length, off.
  EXPECT_FALSE(S3ReadsWholeFile(0, 2 * MiB));
  EXPECT_FALSE(S3ReadsWholeFile(100, 0));
}

TEST(S3NativeTest, TailRead) {
  // The footer range of a Parquet file: the last 100 KB.
  EXPECT_TRUE(IsS3TailRead(900 * 1024, 100 * 1024, 1000 * 1024));
  // A file shorter than the footer range: read from the start.
  EXPECT_TRUE(IsS3TailRead(0, 500, 500));
  // Past the end (the read is cut at the end of the object).
  EXPECT_TRUE(IsS3TailRead(990, 100, 1000));
  // Column chunks end before the footer.
  EXPECT_FALSE(IsS3TailRead(0, 999, 1000));
  EXPECT_FALSE(IsS3TailRead(100, 100, 1000));
  // Empty read, unknown length.
  EXPECT_FALSE(IsS3TailRead(1000, 0, 1000));
  EXPECT_FALSE(IsS3TailRead(0, 100, 0));
}

TEST(S3NativeTest, PartsBytesRead) {
  // 2.5 parts of 100 bytes: all received.
  EXPECT_EQ(250, S3PartsBytesRead(250, 100, {100, 100, 50}));
  // The object ends in the second part: later parts do not count.
  EXPECT_EQ(130, S3PartsBytesRead(250, 100, {100, 30, 0}));
  EXPECT_EQ(130, S3PartsBytesRead(250, 100, {100, 30, 50}));
  // The object ends exactly at a part boundary.
  EXPECT_EQ(200, S3PartsBytesRead(300, 100, {100, 100, 0}));
  // Nothing at the offset (HTTP 416 on the first part).
  EXPECT_EQ(0, S3PartsBytesRead(300, 100, {0, 0, 0}));
  // One request.
  EXPECT_EQ(42, S3PartsBytesRead(100, 100, {42}));
}

// Writes go to the segments one after another (empty segments are skipped); writes past
// the last segment fail; tellp() is the number of bytes written.
TEST(S3NativeTest, SegmentStreamBufWritesAcrossSegments) {
  char a[3], b[1], c[5];
  S3SegmentStreamBuf::Segments segments = {{reinterpret_cast<uint8_t*>(a), 3},
      {reinterpret_cast<uint8_t*>(b), 0}, {reinterpret_cast<uint8_t*>(c), 5}};
  S3SegmentStreamBuf buf(segments);
  EXPECT_EQ(8, buf.size());
  std::ostream out(&buf);
  out.write("ab", 2);
  EXPECT_EQ(2, static_cast<int64_t>(out.tellp()));
  out.write("cde", 3);
  EXPECT_EQ(5, static_cast<int64_t>(out.tellp()));
  out.write("fgh", 3);
  EXPECT_TRUE(out.good());
  EXPECT_EQ(8, static_cast<int64_t>(out.tellp()));
  EXPECT_EQ("abc", string(a, 3));
  EXPECT_EQ("defgh", string(c, 5));
  out.write("x", 1);
  EXPECT_FALSE(out.good());
}

// A retry rewinds to the start; what was written can be read back.
TEST(S3NativeTest, SegmentStreamBufSeekAndRead) {
  char a[4], c[4];
  S3SegmentStreamBuf::Segments segments = {
      {reinterpret_cast<uint8_t*>(a), 4}, {reinterpret_cast<uint8_t*>(c), 4}};
  S3SegmentStreamBuf buf(segments);
  std::ostream out(&buf);
  out.write("abcdefgh", 8);
  out.seekp(0);
  EXPECT_EQ(0, static_cast<int64_t>(out.tellp()));
  out.write("XYZ", 3);
  EXPECT_EQ(3, static_cast<int64_t>(out.tellp()));
  out.seekp(6);
  out.write("Q", 1);
  EXPECT_EQ(7, static_cast<int64_t>(out.tellp()));
  std::istream in(&buf);
  in.seekg(0);
  char read[8];
  in.read(read, 8);
  EXPECT_EQ(8, in.gcount());
  EXPECT_EQ("XYZdefQh", string(read, 8));
  in.seekg(5);
  in.read(read, 3);
  EXPECT_EQ("fQh", string(read, 3));
  EXPECT_FALSE(out.seekp(9).good());
}

TEST(S3NativeTest, SegmentStreamBufEmpty) {
  S3SegmentStreamBuf::Segments none;
  S3SegmentStreamBuf buf(none);
  EXPECT_EQ(0, buf.size());
  std::ostream out(&buf);
  EXPECT_EQ(0, static_cast<int64_t>(out.tellp()));
  out.write("x", 1);
  EXPECT_FALSE(out.good());
}

} // namespace impala
