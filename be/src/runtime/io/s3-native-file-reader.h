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
#include "runtime/io/s3-native-file-cache.h"

namespace impala {
namespace io {

class DataCache;

/// Connects the native S3 reader to Impala's remote data cache on local disk
/// (--data_cache) if 'data_cache' is not null, --s3_native_data_cache is set and the
/// native S3 file cache reads blocks: a block missing from the file cache is looked up
/// in the data cache before it is fetched from S3. Blocks are stored in the data cache
/// right after they are fetched (--s3_native_data_cache_write_on_fetch, the default), or
/// else when they are evicted from the file cache to make room if they were read again
/// after they were loaded (blocks read only once never reach the disk; blocks given back
/// under memory pressure are not stored). Reads that do not go through the file cache
/// (no file cache, --s3_native_cache_capacity=0) use the data cache like the S3A
/// reader (HdfsFileReader). Called by ExecEnv::Init() after the DiskIoMgr is set up.
void EnableS3NativeDataCache(DataCache* data_cache);

/// Experimental FileReader for s3a:// files that reads through the native aws-sdk-cpp
/// S3 client (s3-native-client.h) instead of libhdfs and the Hadoop S3A connector.
/// Used by ScanRange when --s3_native_reader is set.
///
/// A ReadFromPos() is one ranged GetObject request whose body is written directly into
/// the caller's buffer. Reads of at least --s3_native_parallel_read_min_size bytes are
/// split into parts of --s3_native_parallel_read_part_size bytes that are fetched in
/// parallel (the first part by the I/O thread, the others on the client's executor), each
/// into its own section of the buffer. With S3NativeFileCache (s3-native-file-cache.h),
/// reads go through the cache in blocks of --s3_native_block_size bytes; files of at
/// most --s3_native_whole_file_max_size bytes (off by default) are instead read whole on
/// their first read. Open() makes no request: S3 has no file handles,
/// and the file length is already known from the catalog. Blocks missing from the file
/// cache are looked up in the remote data cache on local disk first, if it is enabled
/// (EnableS3NativeDataCache()); without the file cache, whole reads are looked up there.
/// Not supported: HDFS caching and file handle caching.
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

  /// Reads like the S3A reader with the remote data cache: looks the read up in the data
  /// cache (key: file name, mtime, offset, as for S3A, so both readers share entries),
  /// reads the rest with GetRange() and stores the whole read in the data cache. Used
  /// for reads that do not go through S3NativeFileCache. Arguments as in GetRange().
  Status ReadThroughDataCache(int64_t offset, uint8_t* buffer, int64_t len,
      int64_t* bytes_read);

  /// Copies the requested bytes from the whole file in S3NativeFileCache, reading the
  /// file (of 'file_length' bytes, as known from the catalog) with GetRange() on a miss.
  /// Sets 'bytes_read', less than 'bytes_to_read' only at the end of the file.
  Status ReadFromCachedFile(int64_t file_offset, uint8_t* buffer, int64_t bytes_to_read,
      int64_t file_length, int64_t* bytes_read);

  /// One ranged GET at 'offset' whose body is written into 'segments' one after another
  /// (the blocks of a run in S3NativeFileCache). Never split into parallel parts. Sets
  /// 'bytes_read', less than the total length only at the end of the object.
  Status GetSegments(int64_t offset, const S3NativeFileCache::Segments& segments,
      int64_t* bytes_read);

  /// Fetches the blocks 'segments' (consecutive, starting at 'offset') of the file cache
  /// entry 'file_key': blocks found in the remote data cache are copied from it, each
  /// run of the other blocks is read with one GetSegments(). Without the data cache (or
  /// if the scan range does not use it) one GetSegments() for all of them. Sets
  /// 'bytes_read' like GetSegments().
  Status FetchSegments(const std::string& file_key, int64_t offset,
      const S3NativeFileCache::Segments& segments, int64_t* bytes_read);

  /// Reads through blocks of --s3_native_block_size bytes in S3NativeFileCache,
  /// fetching each run of missing blocks with FetchSegments(). Arguments as in
  /// ReadFromCachedFile().
  Status ReadFromCachedBlocks(int64_t file_offset, uint8_t* buffer, int64_t bytes_to_read,
      int64_t file_length, int64_t* bytes_read);

  std::string bucket_;
  std::string key_;
};

} // namespace io
} // namespace impala
