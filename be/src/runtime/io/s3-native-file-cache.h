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

#include <atomic>
#include <cstdint>
#include <functional>
#include <future>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/status.h"

namespace impala {
namespace io {

/// Experimental in-memory cache of S3 data for the native S3 reader (--s3_native_reader):
/// - whole small files (--s3_native_whole_file_max_size, off by default): read whole
///   with one GET request, so all later reads of the file (Parquet footer, every column
///   chunk, repeated scans by later queries) are served from memory;
/// - fixed-size blocks of larger files (--s3_native_block_size, ReadBlocks()): a read is
///   split into blocks, cached blocks are copied from memory and each run of consecutive
///   missing blocks is fetched with one GET whose body is written straight into the new
///   blocks. Catches data read again soon: a second scan of the same table in one query,
///   the next query reading the same columns, the footer and page index read by every
///   scan range of the file.
/// Entries are keyed by a file key (callers use path and mtime, so a rewritten file is
/// never served from a stale entry; stale entries age out) and a block index
/// (kWholeFile for whole files).
///
/// If several threads miss on the same entry at the same time, one of them loads it and
/// the others wait for that load instead of sending their own requests.
///
/// The cache is split into shards by file key, each with its own lock, eviction lists
/// and an equal share of the capacity, so that concurrent reads of different files do
/// not contend for one lock. Within a shard, eviction is a segmented LRU, so that one
/// large scan does not flush the cache: a loaded entry enters the probation segment and
/// moves to the protected segment (at most --s3_native_file_cache_protected_percent of
/// the shard's capacity) only when a different reader (another scan of the same or of
/// another query) reads it. Reads by the reader that loaded it only refresh it within
/// probation. Entries are evicted from the end of probation first; when protected
/// outgrows its share, its least recently used entries move back to probation.
///
/// The memory is process memory within --mem_limit. ExecEnv preserves the cache capacity
/// (it is subtracted from the memory admitted for queries, like for the codegen cache),
/// shows the cache as "Native S3 File Cache" under the process MemTracker, and calls
/// ReleaseMemory() when the process is over its memory limit.
///
/// Thread safe.
class S3NativeFileCache {
 public:
  /// Block index of a whole file.
  static constexpr int64_t kWholeFile = -1;

  /// One cached file or block. 'len' is the number of valid bytes in 'data'.
  struct File {
    explicit File(int64_t capacity) : data(new uint8_t[capacity]), len(capacity) {}
    std::unique_ptr<uint8_t[]> data;
    int64_t len;
  };

  /// Fills 'file' with the whole file (at most file->len bytes) and sets file->len to
  /// the number of bytes actually read.
  typedef std::function<Status(File* file)> Loader;

  /// Buffers (pointer, length) that a fetch writes one after another.
  typedef std::vector<std::pair<uint8_t*, int64_t>> Segments;

  /// Reads the object at 'offset' into 'segments' (as many bytes as they hold together)
  /// and sets 'bytes_read', less than their total length only at the end of the object.
  typedef std::function<Status(int64_t offset, const Segments& segments,
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

  /// 'capacity' bytes in total, split evenly into 'num_shards' shards.
  S3NativeFileCache(int64_t capacity, int protected_percent, int num_shards = 1);

  /// Returns in '*file' the whole file stored under 'file_key'. On a miss allocates a
  /// buffer of 'length' bytes and calls 'loader' to fill it; concurrent calls for the
  /// same key wait for that load. '*hit' is false only for the caller that ran 'loader'.
  /// An error of 'loader' is returned to every waiting caller and nothing is cached.
  /// 'owner' identifies the reader (the scan's RequestContext); a hit by an owner other
  /// than the one that loaded the file promotes the file to the protected segment.
  Status Lookup(const std::string& file_key, int64_t length, const void* owner,
      const Loader& loader, std::shared_ptr<const File>* file, bool* hit);

  /// Reads 'len' bytes at 'offset' of a file of 'file_length' bytes (as known from the
  /// catalog) into 'buffer' through cached blocks of 'block_size' bytes stored under
  /// 'file_key'. Each run of consecutive blocks that are neither cached nor being loaded
  /// is fetched with one call of 'fetch', straight into the new blocks. Sets
  /// 'bytes_read', less than 'len' only at the end of the file. 'owner' as in Lookup().
  Status ReadBlocks(const std::string& file_key, int64_t file_length,
      int64_t block_size, int64_t offset, uint8_t* buffer, int64_t len,
      const void* owner, const Fetcher& fetch, int64_t* bytes_read, BlockStats* stats);

  /// Looks up block 'block' of 'file_key' without blocking: sets 'ref->file' on a hit,
  /// 'ref->pending' if another thread is loading it, or reserves the entry for the caller
  /// ('ref->reserved'), who must then call Fill(). Hits by an owner other than the one
  /// that loaded the entry promote it to the protected segment.
  void Acquire(const std::string& file_key, int64_t block, const void* owner, Ref* ref);

  /// Completes a reservation made by Acquire(): caches 'file' if 'status' is OK and the
  /// file fits into the shard, and passes the result to the threads waiting for it.
  void Fill(const std::string& file_key, int64_t block, const void* owner,
      const Status& status, std::shared_ptr<const File> file);

  int64_t capacity() const { return capacity_; }
  int num_shards() const { return static_cast<int>(shards_.size()); }

  /// Evicts entries, least recently used first, until at least 'bytes_to_free' bytes are
  /// released or the cache is empty. Called when the process is over its memory limit,
  /// so that queries take priority over cached files. Entries still being read are freed
  /// when their last reader is done.
  void ReleaseMemory(int64_t bytes_to_free);

  /// Number of bytes and entries currently cached (not counting entries being loaded).
  int64_t TotalBytes() const { return total_bytes_.load(); }
  int64_t NumEntries() const { return num_entries_.load(); }

 private:
  struct FileEntries;

  /// An entry in an eviction list.
  struct LruItem {
    FileEntries* file;
    int64_t block;
  };

  struct Entry {
    /// The cached file or block, or nullptr while it is being loaded.
    std::shared_ptr<const File> file;
    /// Set while the entry is being loaded; Fill() sets its value.
    std::shared_ptr<std::promise<LoadResult>> load_promise;
    /// Set while the entry is being loaded; waiters block on it.
    std::shared_future<LoadResult> loading;
    /// The reader that loaded the entry. A hit by another reader promotes the entry.
    const void* owner = nullptr;
    /// Whether the entry is in 'protected_lru' (otherwise in 'probation').
    bool is_protected = false;
    /// Position in 'probation' or 'protected_lru'. Valid only if 'file' is set.
    std::list<LruItem>::iterator lru_pos;
  };

  /// The entries of one file key, by block index.
  struct FileEntries {
    /// The file key (the key of this element in Shard::files, which never moves).
    const std::string* key = nullptr;
    std::unordered_map<int64_t, Entry> blocks;
  };

  struct Shard {
    int64_t capacity = 0;
    int64_t protected_capacity = 0;
    /// Protects all members below.
    std::mutex lock;
    std::unordered_map<std::string, FileEntries> files;
    /// Loaded entries in each segment, most recently used first.
    std::list<LruItem> probation;
    std::list<LruItem> protected_lru;
    /// Sum of File::len of the entries in each segment.
    int64_t probation_bytes = 0;
    int64_t protected_bytes = 0;
  };

  Shard& ShardFor(const std::string& file_key);

  /// Returns the entries of 'file_key' in 'shard', inserting them if 'create'.
  FileEntries* FindFileLocked(Shard* shard, const std::string& file_key, bool create);

  /// Acquire() of block 'block' of 'file' in 'shard' with its lock held.
  void AcquireLocked(Shard* shard, FileEntries* file, int64_t block, const void* owner,
      Ref* ref);

  /// Fill() with the shard lock held. Returns the promise to set after the lock is
  /// released.
  std::shared_ptr<std::promise<LoadResult>> FillLocked(Shard* shard,
      const std::string& file_key, int64_t block, const void* owner,
      const Status& status, const std::shared_ptr<const File>& file);

  /// Removes block 'block' of 'file' (and 'file' if it has no blocks left).
  void EraseLocked(Shard* shard, FileEntries* file, int64_t block);

  /// Moves a loaded entry to the front of its segment, or to the protected segment if
  /// 'owner' is not the reader that loaded it.
  void TouchLocked(Shard* shard, Entry* entry, const void* owner);

  /// Moves least recently used protected entries back to probation until the protected
  /// segment fits into the shard's protected capacity.
  void DemoteLocked(Shard* shard);

  /// Removes entries until the shard fits into its capacity: from the end of probation
  /// first, from the end of protected if probation holds only 'keep'. Entries being
  /// loaded are not in the lists and are never evicted.
  void EvictLocked(Shard* shard, const LruItem& keep);

  /// Removes the least recently used entry of the shard's probation (or protected if
  /// 'from_probation' is false) segment and returns its size.
  int64_t RemoveLastLocked(Shard* shard, bool from_probation);

  /// Updates the size and entry count gauges.
  void UpdateGauges();

  const int64_t capacity_;
  std::vector<std::unique_ptr<Shard>> shards_;
  /// Totals over all shards, for the gauges and TotalBytes() / NumEntries().
  std::atomic<int64_t> total_bytes_{0};
  std::atomic<int64_t> num_entries_{0};
};

/// Creates the process-wide cache if --s3_native_file_cache_capacity and
/// --s3_native_whole_file_max_size or --s3_native_block_size are positive, with the
/// capacity limited to 'max_capacity' bytes. Returns the capacity of the cache, or 0 if
/// it is disabled. Uses up to 16 shards, each at least 8 times the largest entry
/// (max(--s3_native_whole_file_max_size, --s3_native_block_size)).
/// Called by ExecEnv::Init(), which preserves that much memory for the cache like for
/// the codegen cache.
int64_t InitS3NativeFileCache(int64_t max_capacity);

/// Returns the process-wide cache, or nullptr if it is disabled.
S3NativeFileCache* GetS3NativeFileCache();

} // namespace io
} // namespace impala
