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
#include <condition_variable>
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

class Thread;

namespace io {

/// Experimental in-memory cache of S3 data for the native S3 reader (--s3_native_reader):
/// - whole small files (--s3_native_whole_file_max_size, 2 MiB by default): read whole
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
/// moves to the protected segment (at most --s3_native_cache_protected_percent of
/// the shard's capacity) only when a different reader (another scan of the same or of
/// another query) reads it. Reads by the reader that loaded it only refresh it within
/// probation. Entries are evicted from the end of probation first; when protected
/// outgrows its share, its least recently used entries move back to probation.
///
/// With the admission filter (EnableAdmissionFilter(),
/// --s3_native_cache_admission_filter, on by default) the shards work like W-TinyLFU:
/// each shard counts how often its entries are read (a frequency sketch: every lookup
/// counts, the counts are halved periodically so that they follow changes), and new
/// entries enter a window segment (LRU, --s3_native_cache_admission_window_percent of
/// the capacity) instead of probation, so that data read again soon (the next read of a
/// scan range, the footer and page index) is served from memory. An entry leaving the
/// window enters the main part (probation and protected, as above) only if there is room
/// or it was read more often than the entry it would evict there; otherwise it is
/// dropped. Repeated scans of data larger than the cache then keep a stable part of it
/// cached instead of evicting each block before the next scan reads it again.
///
/// The memory is process memory within --mem_limit. ExecEnv preserves only the reserved
/// part of the capacity (--s3_native_cache_reserved_capacity is subtracted from the
/// memory admitted for queries, like for the codegen cache); the rest is elastic: it uses
/// memory that queries leave free. ExecEnv shows the cache as "Native S3 File Cache"
/// under the process MemTracker, calls ReleaseMemory() when the process is over its
/// memory limit, and sets a headroom function: when the process may allocate less than
/// --s3_native_cache_min_headroom more bytes, the cache gives the elastic part back
/// (down to the reserved part) before queries run into the limit. The headroom is
/// checked after new entries are filled and, every
/// --s3_native_cache_headroom_check_ms, by a background thread, so that the cache
/// also makes room while queries grow and the cache does not fill (joins, aggregations
/// after their scans). While the last check found the headroom below the margin, new
/// entries above the reserved part are not cached (the data still goes to the reader),
/// so that the cache does not fill only to give the memory back right after.
///
/// Thread safe.
class S3NativeFileCache {
 public:
  /// Block index of a whole file.
  static constexpr int64_t kWholeFile = -1;

  /// Default headroom margin: the elastic part of the cache is given back when the
  /// process may allocate less than this many more bytes (the same margin as
  /// MemTracker::GcMemory() frees in excess).
  static constexpr int64_t kDefaultMinHeadroom = 512L * 1024 * 1024;

  /// Returns how many more bytes the process may allocate before its memory limit.
  typedef std::function<int64_t()> HeadroomFn;

  /// Called after the headroom check gave memory back (e.g. to refresh the process
  /// MemTracker from its metric, like MemTracker::GcMemory() does after GC).
  typedef std::function<void()> AfterReleaseFn;

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

  /// Called for an entry evicted to make room in its shard (not for entries given back
  /// under memory pressure) that was read again after it was loaded, outside the shard
  /// locks: e.g. to keep the entry in a slower cache tier (the remote data cache on
  /// local disk). Entries read only once never reach it.
  typedef std::function<void(const std::string& file_key, int64_t block,
      const File& file)> EvictionHandler;

  /// Block reads of ReadBlocks(), for metrics.
  struct BlockStats {
    int64_t hit_blocks = 0;
    int64_t hit_bytes = 0;
    int64_t missed_blocks = 0;
  };

  /// 'capacity' bytes in total, split evenly into 'num_shards' shards. Of them
  /// 'reserved' bytes (all if negative or larger than 'capacity') are preserved memory
  /// that the headroom check never gives back.
  S3NativeFileCache(int64_t capacity, int protected_percent, int num_shards = 1,
      int64_t reserved = -1);

  /// Stops the headroom check thread, if any.
  ~S3NativeFileCache();

  /// Sets the function the cache uses to check the process headroom and the margin
  /// 'min_headroom': below it the elastic part is given back. The headroom is checked
  /// after filling new entries (at most every headroom check interval of filled bytes)
  /// and, if 'check_interval_ms' is positive, every 'check_interval_ms' by a background
  /// thread (logs a warning and keeps only the check on fill if the thread cannot be
  /// started). 'after_release', if set, is called after the check gave memory back.
  /// Has no effect if the whole capacity is reserved. Must be called once, before the
  /// cache is used.
  void SetHeadroomFunction(HeadroomFn fn, int64_t min_headroom = kDefaultMinHeadroom,
      int64_t check_interval_ms = 0, AfterReleaseFn after_release = nullptr);

  /// Turns on the admission filter (see the class comment) with a window of
  /// 'window_percent' of the capacity (1-50, at least 16 entries of 'entry_size' bytes),
  /// with the frequency sketch sized for entries of about 'entry_size' bytes. Must be
  /// called once, before the cache is used.
  void EnableAdmissionFilter(int64_t entry_size, int window_percent);

  /// Sets the handler of evicted entries that were read again (see EvictionHandler).
  /// Must be called before the cache is used.
  void SetEvictionHandler(EvictionHandler handler) {
    eviction_handler_ = std::move(handler);
  }

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
  int64_t reserved() const { return reserved_; }
  int num_shards() const { return static_cast<int>(shards_.size()); }

  /// Evicts entries, least recently used first, until at least 'bytes_to_free' bytes are
  /// released or the cache is empty, also the reserved part. Called when the process is
  /// over its memory limit, so that queries take priority over cached files. Entries
  /// still being read are freed when their last reader is done.
  void ReleaseMemory(int64_t bytes_to_free);

  /// Number of bytes and entries currently cached (not counting entries being loaded).
  int64_t TotalBytes() const { return total_bytes_.load(); }
  int64_t NumEntries() const { return num_entries_.load(); }

 private:
  struct FileEntries;

  /// Count-min sketch of how often entries were read (4 rows of saturating 8-bit counters
  /// up to 15, conservative update). All counters are halved after 'sample_size_'
  /// increments, so that old reads count less. Not thread safe (used under a shard lock).
  class FrequencySketch {
   public:
    /// Sizes the sketch for at most 'max_entries' entries; clears it.
    void Resize(int64_t max_entries);
    bool enabled() const { return !table_.empty(); }
    void Increment(uint64_t hash);
    int Frequency(uint64_t hash) const;

   private:
    static constexpr int kRows = 4;
    static constexpr uint8_t kMaxCount = 15;
    /// Index of 'hash' in row 'row'.
    size_t IndexOf(uint64_t hash, int row) const;
    /// Halves every counter.
    void Age();

    /// kRows rows of 'mask_' + 1 counters.
    std::vector<uint8_t> table_;
    uint64_t mask_ = 0;
    int64_t additions_ = 0;
    int64_t sample_size_ = 0;
  };

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
    /// Whether the entry is in 'protected_lru' (otherwise in 'probation' or 'window').
    bool is_protected = false;
    /// Whether the entry is in 'window' (admission filter only).
    bool in_window = false;
    /// Whether the entry was read again after it was loaded (by any reader).
    bool reused = false;
    /// Position in its segment's list. Valid only if 'file' is set.
    std::list<LruItem>::iterator lru_pos;
    /// Key of the entry in the shard's frequency sketch.
    uint64_t sketch_hash = 0;
  };

  /// The entries of one file key, by block index.
  struct FileEntries {
    /// The file key (the key of this element in Shard::files, which never moves).
    const std::string* key = nullptr;
    /// Hash of the file key, for the frequency sketch.
    uint64_t key_hash = 0;
    std::unordered_map<int64_t, Entry> blocks;
  };

  struct Shard {
    int64_t capacity = 0;
    /// Capacity of probation and protected together: 'capacity' without the window.
    int64_t main_capacity = 0;
    int64_t protected_capacity = 0;
    /// Capacity of the window; 0 without the admission filter.
    int64_t window_capacity = 0;
    /// Protects all members below.
    std::mutex lock;
    std::unordered_map<std::string, FileEntries> files;
    /// Loaded entries in each segment, most recently used first.
    std::list<LruItem> window;
    std::list<LruItem> probation;
    std::list<LruItem> protected_lru;
    /// Sum of File::len of the entries in each segment.
    int64_t window_bytes = 0;
    int64_t probation_bytes = 0;
    int64_t protected_bytes = 0;
    /// Read frequencies for the admission filter; empty if the filter is off.
    FrequencySketch sketch;
  };

  Shard& ShardFor(const std::string& file_key);

  /// Returns the entries of 'file_key' in 'shard', inserting them if 'create'.
  FileEntries* FindFileLocked(Shard* shard, const std::string& file_key, bool create);

  /// Acquire() of block 'block' of 'file' in 'shard' with its lock held.
  void AcquireLocked(Shard* shard, FileEntries* file, int64_t block, const void* owner,
      Ref* ref);

  /// An entry evicted to make room, kept until the eviction handler ran.
  struct Evicted {
    std::string file_key;
    int64_t block;
    std::shared_ptr<const File> file;
  };

  /// Fill() with the shard lock held. Returns the promise to set after the lock is
  /// released. Evicted entries for the eviction handler are added to 'evicted'.
  std::shared_ptr<std::promise<LoadResult>> FillLocked(Shard* shard,
      const std::string& file_key, int64_t block, const void* owner,
      const Status& status, const std::shared_ptr<const File>& file,
      std::vector<Evicted>* evicted);

  /// Calls the eviction handler for 'evicted' (without locks held) and clears it.
  void HandleEvicted(std::vector<Evicted>* evicted);

  /// Admission filter: moves the least recently used entries out of the window until it
  /// fits into its capacity. Each enters probation if the main part has room or after
  /// evicting entries of the main part read less often than it (the next victims: the
  /// end of probation, then of protected); it is dropped as soon as a victim was read at
  /// least as often. Evicted and dropped entries for the eviction handler are added to
  /// 'evicted'.
  void DrainWindowLocked(Shard* shard, std::vector<Evicted>* evicted);

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
  /// loaded are not in the lists and are never evicted. Reused entries are added to
  /// 'evicted' if there is an eviction handler.
  void EvictLocked(Shard* shard, const LruItem& keep, std::vector<Evicted>* evicted);

  enum class Segment { kWindow, kProbation, kProtected };

  /// Removes the least recently used entry of 'segment' of the shard and returns its
  /// size. Adds it to 'evicted' if that is not null, it was reused and there is an
  /// eviction handler.
  int64_t RemoveLastLocked(Shard* shard, Segment segment,
      std::vector<Evicted>* evicted = nullptr);

  /// Updates the size and entry count gauges.
  void UpdateGauges();

  /// Why Release() gives memory back: the process headroom is below the margin, or the
  /// process is over its memory limit (MemTracker GC). Counted and logged separately.
  enum class ReleaseReason { kHeadroom = 0, kProcessLimit = 1 };

  /// Evicts entries, least recently used first, until 'bytes_to_free' bytes are released
  /// or at most 'keep_bytes' are left; returns the number of bytes released. Returns the
  /// freed memory to the system, updates the metrics of 'reason' and logs it (at most
  /// every few seconds per reason).
  int64_t Release(int64_t bytes_to_free, int64_t keep_bytes, ReleaseReason reason);

  /// Whether an entry of 'len' bytes is not cached because the headroom is low (see the
  /// class comment). Updates the skipped bytes metric.
  bool SkipFill(int64_t len);

  /// Called after 'filled' bytes of new entries were cached: every headroom check
  /// interval of filled bytes calls ReleaseForHeadroom().
  void CheckHeadroom(int64_t filled);

  /// Asks 'headroom_fn_' and gives the elastic part back when the process headroom is
  /// below 'min_headroom_'.
  void ReleaseForHeadroom();

  /// Body of the background thread: ReleaseForHeadroom() every 'interval_ms' until the
  /// cache is destroyed.
  void HeadroomCheckLoop(int64_t interval_ms);

  const int64_t capacity_;
  const int protected_percent_;
  /// Part of the capacity that the headroom check keeps.
  const int64_t reserved_;
  /// Filled bytes between two headroom checks.
  const int64_t headroom_check_interval_;
  HeadroomFn headroom_fn_;
  AfterReleaseFn after_release_fn_;
  EvictionHandler eviction_handler_;
  int64_t min_headroom_ = kDefaultMinHeadroom;
  /// Bytes filled since the last headroom check.
  std::atomic<int64_t> filled_since_check_{0};
  /// Set by every headroom check: whether the headroom was below 'min_headroom_' (also
  /// set when the process was over its limit). While set, SkipFill() is true above the
  /// reserved part.
  std::atomic<bool> low_headroom_{false};
  /// Background headroom check: the thread waits on 'stop_cv_' between checks.
  std::unique_ptr<Thread> headroom_thread_;
  std::mutex stop_lock_;
  std::condition_variable stop_cv_;
  bool stop_ = false;
  std::vector<std::unique_ptr<Shard>> shards_;
  /// Totals over all shards, for the gauges and TotalBytes() / NumEntries().
  std::atomic<int64_t> total_bytes_{0};
  std::atomic<int64_t> num_entries_{0};
};

/// Creates the process-wide cache if --s3_native_cache_capacity and
/// --s3_native_whole_file_max_size or --s3_native_block_size are positive, with the
/// capacity limited to 'max_capacity' bytes. Returns the reserved part of the capacity
/// (--s3_native_cache_reserved_capacity, at most the capacity), or 0 if the cache is
/// disabled. Uses up to 16 shards, each at least 8 times the largest entry
/// (max(--s3_native_whole_file_max_size, --s3_native_block_size)).
/// Called by ExecEnv::Init(), which preserves the reserved part for the cache like for
/// the codegen cache.
int64_t InitS3NativeFileCache(int64_t max_capacity);

/// Returns the process-wide cache, or nullptr if it is disabled.
S3NativeFileCache* GetS3NativeFileCache();

} // namespace io
} // namespace impala
