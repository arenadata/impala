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

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "common/status.h"

namespace Aws {
namespace S3 {
class S3Client;
}
}

namespace impala {

/// Experimental native S3 client based on aws-sdk-cpp (--s3_native_reader).
///
/// Initializes the AWS C++ SDK and creates the S3 client shared by all native S3 reads,
/// once at startup, when --s3_native_reader is set. Must be called before any native S3
/// read. Returns an error if the flag is set but impalad was built without aws-sdk-cpp
/// (AWS_SDK_CPP_HOME was not set at build time).
Status InitS3NativeClient();

/// Returns true if the native S3 client is enabled and initialized.
bool S3NativeClientEnabled();

/// Returns the S3 client shared by all native S3 reads. Only valid if
/// S3NativeClientEnabled() returns true.
Aws::S3::S3Client* GetS3NativeClient();

/// Splits an S3 path ('s3a://bucket/key', also 's3://' and 's3n://') into 'bucket' and
/// 'key'. Returns an error if 'path' is not such a path or has an empty bucket or key.
/// The key is not percent-decoded: Impala file paths hold the literal object key (the
/// frontend decodes listing URIs, and Hive-escaped partition values like 'p=a%3Ab' are
/// part of the key), and the SDK encodes the key once when it builds the request URI.
Status ParseS3Path(const std::string& path, std::string* bucket, std::string* key);

/// Returns the size of the native S3 client's HTTP connection pool: 'max_connections'
/// (--s3_native_max_connections), raised to 'num_request_threads' if lower: the S3 I/O
/// threads (--num_s3_io_threads) plus, with parallel reads, the client's executor threads
/// (--s3_native_executor_threads). Every such thread holds one connection during a
/// request, and aws-sdk-cpp waits without a time limit for a free connection when the
/// pool is exhausted.
int S3NativeConnectionPoolSize(int max_connections, int num_request_threads);

/// Returns the part size for a read of 'len' bytes: 'part_size' if the read is split into
/// parallel parts (--s3_native_parallel_read_min_size = 'min_size' > 0, 'len' at least
/// 'min_size' and larger than 'part_size'), otherwise 'len' (one request).
int64_t S3ReadPartSize(int64_t len, int64_t min_size, int64_t part_size);

/// Returns the number of bytes read by a split read of 'len' bytes in parts of
/// 'part_size' bytes, given the bytes received per part. The parts up to and including
/// the first short part count: bytes after a short part are past the end of the object.
int64_t S3PartsBytesRead(int64_t len, int64_t part_size,
    const std::vector<int64_t>& received);

/// Checks the result of one successful ranged GET request for 'bytes_requested' bytes
/// and sets 'bytes_read' and 'eof'. 'bytes_written' is the number of body bytes written
/// into the read buffer (the response stream position), -1 if the stream failed.
/// 'content_length' is the Content-Length header as reported by aws-sdk-cpp, which is 0
/// when the header is absent (chunked transfer encoding). A ranged GET never returns an
/// empty body for a valid range, so 0 is treated as "unknown" and the stream position is
/// what counts. Returns an error if the body does not fit the request or disagrees with
/// a present Content-Length header.
Status CheckS3RangeRead(int64_t bytes_requested, int64_t bytes_written,
    int64_t content_length, int64_t* bytes_read, bool* eof);

} // namespace impala
