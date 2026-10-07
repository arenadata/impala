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

#include "runtime/io/file-reader.h"

namespace impala {
namespace io {

/// Experimental FileReader for s3a:// files that reads through the native aws-sdk-cpp
/// S3 client (s3-native-client.h) instead of libhdfs and the Hadoop S3A connector.
/// Used by ScanRange when --s3_native_reader is set.
///
/// Every ReadFromPos() is one ranged GetObject request whose body is written directly
/// into the caller's buffer. Open() makes no request: S3 has no file handles, and the
/// file length is already known from the catalog. Not supported (yet): the remote data
/// cache, HDFS caching and file handle caching.
class S3NativeFileReader : public FileReader {
 public:
  S3NativeFileReader(ScanRange* scan_range) : FileReader(scan_range) {}
  ~S3NativeFileReader() {}

  virtual Status Open() override;
  virtual Status ReadFromPos(DiskQueue* queue, int64_t file_offset, uint8_t* buffer,
      int64_t bytes_to_read, int64_t* bytes_read, bool* eof) override;
  /// HDFS caching does not apply to S3: always sets 'data' to nullptr.
  virtual void CachedFile(uint8_t** data, int64_t* length) override;
  virtual void Close() override {}
  virtual bool SupportsDelayedOpen() const override { return true; }
  virtual std::string DebugString() const override;

 private:
  /// Splits the scan range's file name into 'bucket_' and 'key_' on first use.
  Status ParsePath();

  std::string bucket_;
  std::string key_;
};

} // namespace io
} // namespace impala
