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
/// A ReadFromPos() is one ranged GetObject request whose body is written directly into
/// the caller's buffer. Reads of at least --s3_native_parallel_read_min_size bytes are
/// split into parts of --s3_native_parallel_read_part_size bytes that are fetched in
/// parallel (the first part by the I/O thread, the others on the client's executor), each
/// into its own section of the buffer. Files of at most --s3_native_whole_file_max_size
/// bytes are instead read whole on their first read and served from S3NativeFileCache
/// (s3-native-file-cache.h); reads of larger files go through the same cache in blocks of
/// --s3_native_block_size bytes. Open() makes no request: S3 has no file handles,
/// and the file length is already known from the catalog. Not supported (yet): the
/// remote data cache, HDFS caching and file handle caching.
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

  /// Reads 'len' bytes at 'offset' into 'buffer': one ranged GET, or parallel GETs of
  /// --s3_native_parallel_read_part_size bytes for large reads. Waits for every part
  /// before returning, since all of them write into 'buffer'. Sets 'bytes_read', less
  /// than 'len' only at the end of the object. Updates the native S3 metrics.
  Status GetRange(int64_t offset, uint8_t* buffer, int64_t len, int64_t* bytes_read);

  /// Copies the requested bytes from the whole file in S3NativeFileCache, reading the
  /// file (of 'file_length' bytes, as known from the catalog) with GetRange() on a miss.
  /// Sets 'bytes_read', less than 'bytes_to_read' only at the end of the file.
  Status ReadFromCachedFile(int64_t file_offset, uint8_t* buffer, int64_t bytes_to_read,
      int64_t file_length, int64_t* bytes_read);

  /// Reads through blocks of --s3_native_block_size bytes in S3NativeFileCache,
  /// fetching each run of missing blocks with one GetRange(). Arguments as in
  /// ReadFromCachedFile().
  Status ReadFromCachedBlocks(int64_t file_offset, uint8_t* buffer, int64_t bytes_to_read,
      int64_t file_length, int64_t* bytes_read);

  std::string bucket_;
  std::string key_;
};

} // namespace io
} // namespace impala
