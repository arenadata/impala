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

#include <gflags/gflags.h>

#ifdef IMPALA_HAVE_AWS_SDK_CPP
#include <aws/core/Aws.h>
#include <aws/core/Version.h>
#endif

#include "common/logging.h"

#include "common/names.h"

DEFINE_bool(s3_native_reader, false, "(Experimental) Read s3a:// files with the native "
    "aws-sdk-cpp client instead of libhdfs and the Hadoop S3A connector.");

namespace impala {

namespace {

bool initialized = false;

} // anonymous namespace

Status InitS3NativeClient() {
  if (!FLAGS_s3_native_reader) return Status::OK();
#ifdef IMPALA_HAVE_AWS_SDK_CPP
  DCHECK(!initialized);
  // The SDK stays initialized for the lifetime of the process, so the options are never
  // freed and Aws::ShutdownAPI() is not called.
  Aws::SDKOptions* options = new Aws::SDKOptions();
  Aws::InitAPI(*options);
  initialized = true;
  LOG(INFO) << "Native S3 client enabled (aws-sdk-cpp "
            << Aws::Version::GetVersionString() << ")";
  return Status::OK();
#else
  return Status("--s3_native_reader is set, but impalad was built without aws-sdk-cpp");
#endif
}

bool S3NativeClientEnabled() {
  return initialized;
}

} // namespace impala
