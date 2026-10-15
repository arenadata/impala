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
#include <functional>
#include <future>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include "common/status.h"

namespace impala {
namespace io {

/// Experimental in-memory cache of S3 data for the native S3 reader (--s3_native_reader):
/// - whole small files (--s3_native_whole_file_max_size): read whole with one GET
///   request, so all later reads of the file (Parquet footer, every column chunk,
///   repeated scans by later queries) are served from memory;
/// - fixed-size blocks of larger files (--s3_native_block_size, ReadBlocks()): a read is
///   split into blocks, cached blocks are copied from memory and each run of consecutive
///   missing blocks is fetched with one GET. Catches data read again soon: a second scan
///   of the same table in one query, the next query reading the same columns, the footer
///   and page index read by every scan range of the file.
/// Callers key the entries by path and mtime, so a rewritten file is never served from a
/// stale entry; stale entries age out.
///
/// If several threads miss on the same entry at the same time, one of them loads it and
/// the others wait for that load instead of sending their own requests.
///
/// Eviction is a segmented LRU over all entries, so that one large scan does not flush
/// the cache: a loaded entry enters the probation segment and moves to the protected
/// segment (at most --s3_native_file_cache_protected_percent of the capacity) only when
/// a different reader (another scan of the same or of another query) reads it. Reads by
/// the reader that loaded it only refresh it within probation. Entries are evicted from
/// the end of probation first; when protected outgrows its share, its least recently
/// used entries move back to probation.
///
/// The memory is process memory within --mem_limit. ExecEnv preserves the cache capacity
/// (it is subtracted from the memory admitted for queries, like for the codegen cache),
/// shows the cache as "Native S3 File Cache" under the process MemTracker, and calls
/// ReleaseMemory() when the process is over its memory limit.
///
/// Thread safe.
class S3NativeFileCache {
 public:
  /// One cached file. 'len' is the number of valid bytes in 'data'.
  struct File {
    explicit File(int64_t capacity) : data(new uint8_t[capacity]), len(capacity) {}
    std::unique_ptr<uint8_t[]> data;
    int64_t len;
  };

  /// Fills 'file' with the whole file (at most file->len bytes) and sets file->len to
  /// the number of bytes actually read.
  typedef std::function<Status(File* file)> Loader;

  /// Reads 'len' bytes of the object at 'offset' into 'buffer' and sets 'bytes_read',
  /// less than 'len' only at the end of the object.
  typedef std::function<Status(int64_t offset, uint8_t* buffer, int64_t len,
      int64_t* bytes_read)> Fetcher;

  typedef std::pair<Status, std::shared_ptr<const File>> LoadResult;

  /// Result of Acquire(): exactly one of the members is set.
  struct Ref {
    /// The cached entry.
    std::shared_ptr<const File> file;
    /// Another thread is loading the entry; get() returns its result.
    std::shared_future<LoadResult> pending;
    /// The caller must load the entry and call Fill() for it, also on error.
    bool reserved = false;
  };

  /// Block reads of ReadBlocks(), for metrics.
  struct BlockStats {
    int64_t hit_blocks = 0;
    int64_t hit_bytes = 0;
    int64_t missed_blocks = 0;
  };

  S3NativeFileCache(int64_t capacity, int protected_percent)
    : capacity_(capacity), protected_capacity_(capacity * protected_percent / 100) {}

  /// Returns in '*file' the whole file stored under 'key'. On a miss allocates a buffer
  /// of 'length' bytes and calls 'loader' to fill it; concurrent calls for the same key
  /// wait for that load. '*hit' is false only for the caller that ran 'loader'. An
  /// error of 'loader' is returned to every waiting caller and nothing is cached.
  /// 'owner' identifies the reader (the scan's RequestContext); a hit by an owner other
  /// than the one that loaded the file promotes the file to the protected segment.
  Status Lookup(const std::string& key, int64_t length, const void* owner,
      const Loader& loader, std::shared_ptr<const File>* file, bool* hit);

  /// Reads 'len' bytes at 'offset' of a file of 'file_length' bytes (as known from the
  /// catalog) into 'buffer' through cached blocks of 'block_size' bytes stored under
  /// 'key_prefix' + block index. Each run of consecutive blocks that are neither cached
  /// nor being loaded is fetched with one call of 'fetch' and cached. Sets 'bytes_read',
  /// less than 'len' only at the end of the file. 'owner' as in Lookup().
  Status ReadBlocks(const std::string& key_prefix, int64_t file_length,
      int64_t block_size, int64_t offset, uint8_t* buffer, int64_t len,
      const void* owner, const Fetcher& fetch, int64_t* bytes_read, BlockStats* stats);

  /// Looks up 'key' without blocking: sets 'ref->file' on a hit, 'ref->pending' if
  /// another thread is loading it, or reserves the entry for the caller
  /// ('ref->reserved'), who must then call Fill(). Hits by an owner other than the one
  /// that loaded the entry promote it to the protected segment.
  void Acquire(const std::string& key, const void* owner, Ref* ref);

  /// Completes a reservation made by Acquire(): caches 'file' if 'status' is OK and the
  /// file fits into the cache, and passes the result to the threads waiting for it.
  void Fill(const std::string& key, const void* owner, const Status& status,
      std::shared_ptr<const File> file);

  int64_t capacity() const { return capacity_; }

  /// Evicts files, least recently used first, until at least 'bytes_to_free' bytes are
  /// released or the cache is empty. Called when the process is over its memory limit,
  /// so that queries take priority over cached files. Files still being read are freed
  /// when their last reader is done.
  void ReleaseMemory(int64_t bytes_to_free);

  /// Number of bytes and files currently cached (not counting files being loaded).
  int64_t TotalBytes();
  int64_t NumEntries();

 private:
  struct Entry {
    /// The cached file, or nullptr while it is being loaded.
    std::shared_ptr<const File> file;
    /// Set while the file is being loaded; Fill() sets its value.
    std::shared_ptr<std::promise<LoadResult>> load_promise;
    /// Set while the file is being loaded; waiters block on it.
    std::shared_future<LoadResult> loading;
    /// The reader that loaded the file. A hit by another reader promotes the file.
    const void* owner = nullptr;
    /// Whether the file is in 'protected_' (otherwise in 'probation_').
    bool is_protected = false;
    /// Position in 'probation_' or 'protected_'. Valid only if 'file' is set.
    std::list<std::string>::iterator lru_pos;
  };

  /// Moves a loaded file to the front of its segment, or to the protected segment if
  /// 'owner' is not the reader that loaded it.
  void TouchLocked(Entry* entry, const void* owner);

  /// Moves least recently used protected files back to probation until the protected
  /// segment fits into 'protected_capacity_'.
  void DemoteLocked();

  /// Removes files until the cache fits into 'capacity_': from the end of probation
  /// first, from the end of protected if probation holds only 'keep'. Files being
  /// loaded are not in the lists and are never evicted.
  void EvictLocked(const std::string& keep);

  /// Removes the least recently used file of 'probation_' (or of 'protected_' if
  /// 'from_probation' is false) and returns its size.
  int64_t RemoveLastLocked(bool from_probation);

  /// Updates the size and entry count gauges.
  void UpdateGaugesLocked();

  const int64_t capacity_;
  const int64_t protected_capacity_;

  /// Protects all members below.
  std::mutex lock_;
  std::unordered_map<std::string, Entry> entries_;
  /// Keys of loaded files in each segment, most recently used first.
  std::list<std::string> probation_;
  std::list<std::string> protected_;
  /// Sum of File::len of the files in each segment.
  int64_t probation_bytes_ = 0;
  int64_t protected_bytes_ = 0;
};

/// Creates the process-wide cache if --s3_native_file_cache_capacity and
/// --s3_native_whole_file_max_size or --s3_native_block_size are positive, with the
/// capacity limited to
/// 'max_capacity' bytes. Returns the capacity of the cache, or 0 if it is disabled.
/// Called by ExecEnv::Init(), which preserves that much memory for the cache like for
/// the codegen cache.
int64_t InitS3NativeFileCache(int64_t max_capacity);

/// Returns the process-wide cache, or nullptr if it is disabled.
S3NativeFileCache* GetS3NativeFileCache();

} // namespace io
} // namespace impala
