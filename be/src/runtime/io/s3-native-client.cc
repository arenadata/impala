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

#include "runtime/io/s3-native-client.h"

#include <algorithm>
#include <cstdlib>
#include <memory>

#include <gflags/gflags.h>

#ifdef IMPALA_HAVE_AWS_SDK_CPP
#include <aws/core/Aws.h>
#include <aws/core/Version.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/threading/PooledThreadExecutor.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3ClientConfiguration.h>
#endif

#include "common/logging.h"
#include "gutil/strings/substitute.h"

#include "common/names.h"

DEFINE_bool(s3_native_reader, false, "(Experimental) Read s3a:// files with the native "
    "aws-sdk-cpp client instead of libhdfs and the Hadoop S3A connector.");
DEFINE_string(s3_native_endpoint, "", "(Experimental) S3 endpoint of the native S3 "
    "reader, e.g. https://s3gateway:9879. Empty means the AWS endpoint of "
    "--s3_native_region.");
DEFINE_string(s3_native_region, "us-east-1", "(Experimental) S3 region of the native S3 "
    "reader.");
DEFINE_bool(s3_native_path_style, true, "(Experimental) Use path-style S3 URLs "
    "(https://endpoint/bucket/key) in the native S3 reader. Needed for Ozone and MinIO.");
DEFINE_string(s3_native_credentials_file, "", "(Experimental) AWS credentials file (ini "
    "format with aws_access_key_id and aws_secret_access_key) for the native S3 reader. "
    "Empty means the default AWS credentials chain (environment variables, "
    "~/.aws/credentials). Keys are deliberately not taken from flags, which are visible "
    "on the debug web pages.");
DEFINE_int32(s3_native_max_connections, 128, "(Experimental) Maximum number of HTTP "
    "connections of the native S3 reader. Raised to --num_s3_io_threads (plus "
    "--s3_native_executor_threads when reads are split into parallel parts) if lower: "
    "every thread holds one connection during a request, and aws-sdk-cpp waits without "
    "a time limit for a free connection.");
DEFINE_int32(s3_native_connect_timeout_ms, 10000, "(Experimental) Connect timeout of the "
    "native S3 reader, in milliseconds.");
DEFINE_int32(s3_native_request_timeout_ms, 30000, "(Experimental) Request timeout of the "
    "native S3 reader, in milliseconds.");
DEFINE_string(s3_native_ca_file, "", "(Experimental) CA certificates file used to verify "
    "the S3 endpoint over HTTPS. Empty means the system CA store.");
DEFINE_int64(s3_native_parallel_read_min_size, 0, "(Experimental) Reads of at least "
    "this many bytes are split into parts of --s3_native_parallel_read_part_size bytes "
    "that are fetched in parallel, each with its own ranged GET. Helps only when one "
    "connection to the S3 gateway is slow. 0 (the default) turns splitting off.");
DEFINE_int64(s3_native_parallel_read_part_size, 1024 * 1024, "(Experimental) Size in "
    "bytes of the parts of a read split by --s3_native_parallel_read_min_size.");
DEFINE_int32(s3_native_executor_threads, 64, "(Experimental) Number of threads of the "
    "native S3 client that fetch the parts of split reads (the first part of a read is "
    "fetched by the I/O thread itself). Only used if --s3_native_parallel_read_min_size "
    "is set; otherwise the client has one such thread.");

DEFINE_int64(s3_native_whole_file_max_size, 8L * 1024 * 1024, "(Experimental) Files "
    "of at most this many bytes are read whole with one GET on their first read and "
    "kept in the native S3 file cache, so that later reads of the file (footer, column "
    "chunks, later queries) are served from memory. 0 turns this off.");
DEFINE_int64(s3_native_file_cache_capacity, 2L * 1024 * 1024 * 1024, "(Experimental) "
    "Capacity in bytes of the native S3 file cache (--s3_native_whole_file_max_size, "
    "--s3_native_block_size). "
    "Limited to --s3_native_file_cache_max_mem_percent of the memory admitted for "
    "queries; the capacity is subtracted from that memory. 0 turns the cache off.");
DEFINE_int32(s3_native_file_cache_max_mem_percent, 25, "(Experimental) Maximum share, "
    "in percent of the memory admitted for queries, of the native S3 file cache.");
DEFINE_int64(s3_native_block_size, 128 * 1024, "(Experimental) Reads of files larger "
    "than --s3_native_whole_file_max_size go through the native S3 file cache in blocks "
    "of this many bytes: cached blocks are copied from memory, each run of missing "
    "blocks is fetched with one GET and cached, so data read again soon (another scan "
    "of the same table, the next query, the footer) is not downloaded again. 0 turns "
    "this off.");
DEFINE_int32(s3_native_file_cache_protected_percent, 80, "(Experimental) Share, in "
    "percent of the capacity, of the protected segment of the native S3 file cache: "
    "files read by more than one scan. One large scan evicts only the rest.");

DECLARE_int32(num_s3_io_threads);

using strings::Substitute;

namespace impala {

namespace {

bool initialized = false;

#ifdef IMPALA_HAVE_AWS_SDK_CPP
const char* const kAllocationTag = "ImpalaS3NativeClient";

// Shared by all native S3 reads; S3Client is thread-safe. Created once in
// InitS3NativeClient() and kept for the lifetime of the process.
std::shared_ptr<Aws::S3::S3Client> s3_client;
#endif

} // anonymous namespace

Status InitS3NativeClient() {
  if (!FLAGS_s3_native_reader) return Status::OK();
#ifdef IMPALA_HAVE_AWS_SDK_CPP
  DCHECK(!initialized);
  // impalad does not run on EC2 instances that need the instance metadata service, and
  // probing it slows down client creation.
  setenv("AWS_EC2_METADATA_DISABLED", "true", /* overwrite */ 0);
  if (!FLAGS_s3_native_credentials_file.empty()) {
    setenv("AWS_SHARED_CREDENTIALS_FILE", FLAGS_s3_native_credentials_file.c_str(), 1);
  }

  // The SDK stays initialized for the lifetime of the process, so the options are never
  // freed and Aws::ShutdownAPI() is not called.
  Aws::SDKOptions* options = new Aws::SDKOptions();
  Aws::InitAPI(*options);

  Aws::Client::ClientConfigurationInitValues init_values;
  init_values.shouldDisableIMDS = true;
  Aws::S3::S3ClientConfiguration config(init_values);
  config.region = FLAGS_s3_native_region.c_str();
  if (!FLAGS_s3_native_endpoint.empty()) {
    config.endpointOverride = FLAGS_s3_native_endpoint.c_str();
  }
  config.useVirtualAddressing = !FLAGS_s3_native_path_style;
  // Reads only: there is no request payload to sign.
  config.payloadSigningPolicy =
      Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never;
  bool parallel_reads = FLAGS_s3_native_parallel_read_min_size > 0
      && FLAGS_s3_native_parallel_read_part_size > 0;
  int executor_threads = std::max(FLAGS_s3_native_executor_threads, 1);
  int request_threads =
      FLAGS_num_s3_io_threads + (parallel_reads ? executor_threads : 0);
  int pool_size =
      S3NativeConnectionPoolSize(FLAGS_s3_native_max_connections, request_threads);
  if (pool_size != FLAGS_s3_native_max_connections) {
    LOG(WARNING) << "--s3_native_max_connections=" << FLAGS_s3_native_max_connections
                 << " is lower than the number of request threads (--num_s3_io_threads="
                 << FLAGS_num_s3_io_threads << (parallel_reads ? Substitute(
                     " + --s3_native_executor_threads=$0", executor_threads) : "")
                 << "), using " << pool_size << " connections";
  }
  config.maxConnections = pool_size;
  // Fetches the parts of split reads (GetObjectCallable). The default executor would
  // start a thread per request. Only split reads use it: one thread if they are off.
  config.executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(
      kAllocationTag, parallel_reads ? executor_threads : 1);
  config.connectTimeoutMs = FLAGS_s3_native_connect_timeout_ms;
  config.requestTimeoutMs = FLAGS_s3_native_request_timeout_ms;
  if (!FLAGS_s3_native_ca_file.empty()) config.caFile = FLAGS_s3_native_ca_file.c_str();

  auto credentials =
      Aws::MakeShared<Aws::Auth::DefaultAWSCredentialsProviderChain>(kAllocationTag);
  if (credentials->GetAWSCredentials().IsEmpty()) {
    return Status("--s3_native_reader is set, but no AWS credentials were found. Set "
        "--s3_native_credentials_file or the AWS_ACCESS_KEY_ID and "
        "AWS_SECRET_ACCESS_KEY environment variables.");
  }
  s3_client = Aws::MakeShared<Aws::S3::S3Client>(
      kAllocationTag, credentials, /* endpointProvider */ nullptr, config);
  initialized = true;
  LOG(INFO) << "Native S3 client enabled (aws-sdk-cpp "
            << Aws::Version::GetVersionString() << "), endpoint '"
            << FLAGS_s3_native_endpoint << "', region " << FLAGS_s3_native_region
            << ", path-style " << (FLAGS_s3_native_path_style ? "on" : "off")
            << ", max connections " << pool_size << ", parallel reads "
            << (parallel_reads ? Substitute("from $0 bytes in parts of $1 bytes",
                   FLAGS_s3_native_parallel_read_min_size,
                   FLAGS_s3_native_parallel_read_part_size) : string("off"));
  return Status::OK();
#else
  return Status("--s3_native_reader is set, but impalad was built without aws-sdk-cpp");
#endif
}

bool S3NativeClientEnabled() {
  return initialized;
}

Aws::S3::S3Client* GetS3NativeClient() {
  DCHECK(initialized);
#ifdef IMPALA_HAVE_AWS_SDK_CPP
  return s3_client.get();
#else
  return nullptr;
#endif
}

Status ParseS3Path(const string& path, string* bucket, string* key) {
  static const char* const kSchemes[] = {"s3a://", "s3://", "s3n://"};
  for (const char* scheme : kSchemes) {
    const string prefix(scheme);
    if (path.compare(0, prefix.size(), prefix) != 0) continue;
    size_t slash = path.find('/', prefix.size());
    if (slash != string::npos && slash > prefix.size() && slash + 1 < path.size()) {
      *bucket = path.substr(prefix.size(), slash - prefix.size());
      *key = path.substr(slash + 1);
      return Status::OK();
    }
    break;
  }
  return Status(Substitute("Not a valid S3 path (expected s3a://bucket/key): $0", path));
}

int S3NativeConnectionPoolSize(int max_connections, int num_request_threads) {
  return std::max(max_connections, num_request_threads);
}

int64_t S3ReadPartSize(int64_t len, int64_t min_size, int64_t part_size) {
  if (min_size <= 0 || part_size <= 0 || len < min_size || len <= part_size) return len;
  return part_size;
}

int64_t S3PartsBytesRead(int64_t len, int64_t part_size,
    const std::vector<int64_t>& received) {
  int64_t total = 0;
  for (size_t i = 0; i < received.size(); ++i) {
    int64_t expected = std::min(part_size, len - static_cast<int64_t>(i) * part_size);
    total += received[i];
    // Bytes after a short part are past the end of the object.
    if (received[i] < expected) break;
  }
  return total;
}

Status CheckS3RangeRead(int64_t bytes_requested, int64_t bytes_written,
    int64_t content_length, int64_t* bytes_read, bool* eof) {
  DCHECK_GT(bytes_requested, 0);
  *bytes_read = 0;
  *eof = false;
  if (bytes_written < 0) {
    return Status("Failed to write the response body into the read buffer");
  }
  if (bytes_written > bytes_requested) {
    return Status(Substitute("Response body has $0 bytes, more than the $1 bytes "
        "requested", bytes_written, bytes_requested));
  }
  if (content_length > 0 && content_length != bytes_written) {
    return Status(Substitute("Response body has $0 bytes, but Content-Length is $1",
        bytes_written, content_length));
  }
  *bytes_read = bytes_written;
  // A shorter body means the range went past the end of the object.
  *eof = bytes_written < bytes_requested;
  return Status::OK();
}

} // namespace impala
