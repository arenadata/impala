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

#include <string>

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
Status ParseS3Path(const std::string& path, std::string* bucket, std::string* key);

} // namespace impala
