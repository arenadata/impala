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

#include <cstdlib>
#include <memory>

#include <gflags/gflags.h>

#ifdef IMPALA_HAVE_AWS_SDK_CPP
#include <aws/core/Aws.h>
#include <aws/core/Version.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/ClientConfiguration.h>
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
DEFINE_int32(s3_native_max_connections, 64, "(Experimental) Maximum number of HTTP "
    "connections of the native S3 reader.");
DEFINE_int32(s3_native_connect_timeout_ms, 10000, "(Experimental) Connect timeout of the "
    "native S3 reader, in milliseconds.");
DEFINE_int32(s3_native_request_timeout_ms, 30000, "(Experimental) Request timeout of the "
    "native S3 reader, in milliseconds.");
DEFINE_string(s3_native_ca_file, "", "(Experimental) CA certificates file used to verify "
    "the S3 endpoint over HTTPS. Empty means the system CA store.");

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
  config.maxConnections = FLAGS_s3_native_max_connections;
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
            << ", max connections " << FLAGS_s3_native_max_connections;
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

} // namespace impala
