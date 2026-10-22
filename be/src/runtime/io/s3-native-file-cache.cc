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

#include "runtime/io/s3-native-file-cache.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <vector>

#include <gflags/gflags.h>
#if !defined(ADDRESS_SANITIZER) && !defined(THREAD_SANITIZER)
#include <gperftools/malloc_extension.h>
#endif

#include "common/logging.h"
#include "util/impalad-metrics.h"
#include "util/metrics.h"
#include "util/pretty-printer.h"
#include "util/thread.h"
#include "util/time.h"

DECLARE_int64(s3_native_whole_file_max_size);
DECLARE_int64(s3_native_block_size);
DECLARE_int64(s3_native_cache_capacity);
DECLARE_int64(s3_native_cache_reserved_capacity);
DECLARE_int32(s3_native_cache_protected_percent);

namespace impala {
namespace io {

namespace {

// Created once by InitS3NativeFileCache() and kept for the lifetime of the process.
std::unique_ptr<S3NativeFileCache> file_cache;

// Release() logs at most once in this many milliseconds, with the bytes since then.
constexpr int64_t RELEASE_LOG_INTERVAL_MS = 10000;

} // anonymous namespace

S3NativeFileCache::S3NativeFileCache(
    int64_t capacity, int protected_percent, int num_shards, int64_t reserved)
  : capacity_(capacity),
    reserved_(reserved < 0 ? capacity : std::min(reserved, capacity)),
    // Often enough that the process does not grow far past the headroom margin.
    headroom_check_interval_(
        std::max<int64_t>(std::min<int64_t>(64L * 1024 * 1024, capacity / 16), 1)) {
  num_shards = std::max(num_shards, 1);
  for (int i = 0; i < num_shards; ++i) {
    std::unique_ptr<Shard> shard = std::make_unique<Shard>();
    shard->capacity = capacity / num_shards;
    shard->protected_capacity = shard->capacity * protected_percent / 100;
    shards_.push_back(std::move(shard));
  }
}

S3NativeFileCache::Shard& S3NativeFileCache::ShardFor(const std::string& file_key) {
  return *shards_[std::hash<std::string>()(file_key) % shards_.size()];
}

Status S3NativeFileCache::Lookup(const std::string& file_key, int64_t length,
    const void* owner, const Loader& loader, std::shared_ptr<const File>* file,
    bool* hit) {
  DCHECK_GT(length, 0);
  Ref ref;
  Acquire(file_key, kWholeFile, owner, &ref);
  if (!ref.reserved) {
    // Cached, or another thread is loading it: wait for that load instead of sending
    // another request.
    if (ref.file == nullptr) {
      const LoadResult& result = ref.pending.get();
      RETURN_IF_ERROR(result.first);
      ref.file = result.second;
    }
    *file = std::move(ref.file);
    *hit = true;
    return Status::OK();
  }
  *hit = false;
  std::shared_ptr<File> loaded = std::make_shared<File>(length);
  Status status = loader(loaded.get());
  Fill(file_key, kWholeFile, owner, status, loaded);
  RETURN_IF_ERROR(status);
  *file = std::move(loaded);
  return Status::OK();
}

void S3NativeFileCache::Acquire(
    const std::string& file_key, int64_t block, const void* owner, Ref* ref) {
  Shard& shard = ShardFor(file_key);
  std::lock_guard<std::mutex> l(shard.lock);
  AcquireLocked(&shard, FindFileLocked(&shard, file_key, true), block, owner, ref);
}

void S3NativeFileCache::Fill(const std::string& file_key, int64_t block,
    const void* owner, const Status& status, std::shared_ptr<const File> file) {
  Shard& shard = ShardFor(file_key);
  std::shared_ptr<std::promise<LoadResult>> load_promise;
  std::vector<Evicted> evicted;
  {
    std::lock_guard<std::mutex> l(shard.lock);
    load_promise = FillLocked(&shard, file_key, block, owner, status, file, &evicted);
  }
  UpdateGauges();
  int64_t filled = status.ok() && file != nullptr ? file->len : 0;
  load_promise->set_value(LoadResult(status, std::move(file)));
  HandleEvicted(&evicted);
  CheckHeadroom(filled);
}

void S3NativeFileCache::HandleEvicted(std::vector<Evicted>* evicted) {
  for (const Evicted& e : *evicted) eviction_handler_(e.file_key, e.block, *e.file);
  evicted->clear();
}

S3NativeFileCache::FileEntries* S3NativeFileCache::FindFileLocked(
    Shard* shard, const std::string& file_key, bool create) {
  auto it = shard->files.find(file_key);
  if (it != shard->files.end()) return &it->second;
  if (!create) return nullptr;
  auto inserted = shard->files.emplace(file_key, FileEntries());
  inserted.first->second.key = &inserted.first->first;
  return &inserted.first->second;
}

void S3NativeFileCache::AcquireLocked(Shard* shard, FileEntries* file, int64_t block,
    const void* owner, Ref* ref) {
  auto it = file->blocks.find(block);
  if (it == file->blocks.end()) {
    // Miss: the caller loads the entry. Later callers wait for 'load_promise'.
    Entry& entry = file->blocks[block];
    entry.load_promise = std::make_shared<std::promise<LoadResult>>();
    entry.loading = entry.load_promise->get_future().share();
    ref->reserved = true;
  } else if (it->second.file != nullptr) {
    TouchLocked(shard, &it->second, owner);
    ref->file = it->second.file;
  } else {
    ref->pending = it->second.loading;
  }
}

std::shared_ptr<std::promise<S3NativeFileCache::LoadResult>>
S3NativeFileCache::FillLocked(Shard* shard, const std::string& file_key, int64_t block,
    const void* owner, const Status& status, const std::shared_ptr<const File>& file,
    std::vector<Evicted>* evicted) {
  FileEntries* entries = FindFileLocked(shard, file_key, false);
  DCHECK(entries != nullptr);
  auto it = entries->blocks.find(block);
  DCHECK(it != entries->blocks.end());
  Entry& entry = it->second;
  DCHECK(entry.file == nullptr);
  std::shared_ptr<std::promise<LoadResult>> load_promise = std::move(entry.load_promise);
  DCHECK(load_promise != nullptr);
  if (status.ok() && file != nullptr && file->len <= shard->capacity
      && !SkipFill(file->len)) {
    shard->probation.push_front(LruItem{entries, block});
    entry.file = file;
    entry.loading = std::shared_future<LoadResult>();
    entry.owner = owner;
    entry.is_protected = false;
    entry.lru_pos = shard->probation.begin();
    shard->probation_bytes += file->len;
    total_bytes_ += file->len;
    ++num_entries_;
    EvictLocked(shard, LruItem{entries, block}, evicted);
  } else {
    // Nothing to cache: the load failed (the next caller retries it), the entry is
    // larger than the shard or the process headroom is low (SkipFill()).
    EraseLocked(shard, entries, block);
  }
  return load_promise;
}

void S3NativeFileCache::EraseLocked(Shard* shard, FileEntries* file, int64_t block) {
  file->blocks.erase(block);
  if (file->blocks.empty()) {
    // Copy the key: it is owned by the element being erased.
    std::string key = *file->key;
    shard->files.erase(key);
  }
}

Status S3NativeFileCache::ReadBlocks(const std::string& file_key, int64_t file_length,
    int64_t block_size, int64_t offset, uint8_t* buffer, int64_t len,
    const void* owner, const Fetcher& fetch, int64_t* bytes_read, BlockStats* stats) {
  DCHECK_GT(block_size, 0);
  DCHECK_GE(offset, 0);
  *bytes_read = 0;
  int64_t end = std::min(offset + len, file_length);
  if (offset >= end) return Status::OK();
  const int64_t first_block = offset / block_size;
  const int num_blocks = static_cast<int>((end - 1) / block_size - first_block + 1);
  Shard& shard = ShardFor(file_key);
  std::vector<Ref> refs(num_blocks);
  {
    std::lock_guard<std::mutex> l(shard.lock);
    FileEntries* entries = FindFileLocked(&shard, file_key, true);
    for (int i = 0; i < num_blocks; ++i) {
      AcquireLocked(&shard, entries, first_block + i, owner, &refs[i]);
    }
  }

  // Load the blocks reserved by this thread first, a run of consecutive blocks with one
  // fetch straight into the new blocks, and only then wait for blocks loaded by other
  // threads: two readers never wait for each other's reservations. Every reservation is
  // filled, also after an error.
  Status status;
  std::vector<std::shared_ptr<File>> run_blocks;
  Segments segments;
  std::vector<std::pair<std::shared_ptr<std::promise<LoadResult>>, LoadResult>> results;
  for (int i = 0; i < num_blocks;) {
    if (!refs[i].reserved) {
      ++i;
      continue;
    }
    int j = i;
    while (j < num_blocks && refs[j].reserved) ++j;
    const int64_t run_offset = (first_block + i) * block_size;
    run_blocks.clear();
    segments.clear();
    for (int k = i; k < j; ++k) {
      int64_t block_start = (first_block + k) * block_size;
      int64_t block_len = std::min(block_size, file_length - block_start);
      run_blocks.push_back(std::make_shared<File>(block_len));
      segments.emplace_back(run_blocks.back()->data.get(), block_len);
    }
    int64_t received = 0;
    if (status.ok()) status = fetch(run_offset, segments, &received);
    results.clear();
    int64_t filled = 0;
    std::vector<Evicted> evicted;
    {
      std::lock_guard<std::mutex> l(shard.lock);
      for (int k = i; k < j; ++k) {
        int64_t block = first_block + k;
        if (!status.ok()) {
          results.emplace_back(
              FillLocked(&shard, file_key, block, owner, status, nullptr, &evicted),
              LoadResult(status, nullptr));
          continue;
        }
        // The object may be shorter than the catalog says: keep what was received.
        std::shared_ptr<File>& run_block = run_blocks[k - i];
        int64_t block_offset = block * block_size - run_offset;
        run_block->len =
            std::max<int64_t>(0, std::min(run_block->len, received - block_offset));
        refs[k].file = run_block;
        filled += run_block->len;
        results.emplace_back(
            FillLocked(&shard, file_key, block, owner, Status::OK(), run_block,
                &evicted),
            LoadResult(Status::OK(), run_block));
      }
    }
    UpdateGauges();
    for (auto& result : results) result.first->set_value(std::move(result.second));
    HandleEvicted(&evicted);
    CheckHeadroom(filled);
    stats->missed_blocks += j - i;
    i = j;
  }
  RETURN_IF_ERROR(status);

  int64_t pos = offset;
  for (int i = 0; i < num_blocks && pos < end; ++i) {
    bool hit = !refs[i].reserved;
    if (refs[i].file == nullptr) {
      const LoadResult& result = refs[i].pending.get();
      RETURN_IF_ERROR(result.first);
      refs[i].file = result.second;
    }
    const File& block = *refs[i].file;
    int64_t block_start = (first_block + i) * block_size;
    int64_t in_block = pos - block_start;
    // A short block is the end of the object.
    if (in_block >= block.len) break;
    int64_t n = std::min(block.len - in_block, end - pos);
    memcpy(buffer + (pos - offset), block.data.get() + in_block, n);
    pos += n;
    if (hit) {
      ++stats->hit_blocks;
      stats->hit_bytes += n;
    }
    if (block.len < std::min(block_size, file_length - block_start)) break;
  }
  *bytes_read = pos - offset;
  return Status::OK();
}

void S3NativeFileCache::TouchLocked(Shard* shard, Entry* entry, const void* owner) {
  entry->reused = true;
  if (entry->is_protected) {
    shard->protected_lru.splice(
        shard->protected_lru.begin(), shard->protected_lru, entry->lru_pos);
    return;
  }
  if (owner == entry->owner) {
    // Another read by the scan that loaded the entry: keep it in probation.
    shard->probation.splice(shard->probation.begin(), shard->probation, entry->lru_pos);
    return;
  }
  // Reused by another reader: promote to the protected segment.
  shard->protected_lru.splice(
      shard->protected_lru.begin(), shard->probation, entry->lru_pos);
  entry->is_protected = true;
  shard->probation_bytes -= entry->file->len;
  shard->protected_bytes += entry->file->len;
  if (ImpaladMetrics::IO_MGR_S3_NATIVE_CACHE_PROMOTIONS != nullptr) {
    ImpaladMetrics::IO_MGR_S3_NATIVE_CACHE_PROMOTIONS->Increment(1);
  }
  DemoteLocked(shard);
}

void S3NativeFileCache::DemoteLocked(Shard* shard) {
  while (shard->protected_bytes > shard->protected_capacity
      && shard->protected_lru.size() > 1) {
    const LruItem& item = shard->protected_lru.back();
    auto it = item.file->blocks.find(item.block);
    DCHECK(it != item.file->blocks.end());
    Entry& entry = it->second;
    shard->probation.splice(
        shard->probation.begin(), shard->protected_lru, entry.lru_pos);
    entry.is_protected = false;
    // Any later read promotes the entry again: it has been reused before.
    entry.owner = nullptr;
    shard->protected_bytes -= entry.file->len;
    shard->probation_bytes += entry.file->len;
  }
}

int64_t S3NativeFileCache::RemoveLastLocked(
    Shard* shard, bool from_probation, std::vector<Evicted>* evicted) {
  std::list<LruItem>& segment = from_probation ? shard->probation : shard->protected_lru;
  DCHECK(!segment.empty());
  LruItem item = segment.back();
  auto it = item.file->blocks.find(item.block);
  DCHECK(it != item.file->blocks.end());
  int64_t len = it->second.file->len;
  if (evicted != nullptr && it->second.reused && eviction_handler_) {
    evicted->push_back(Evicted{*item.file->key, item.block, it->second.file});
  }
  (from_probation ? shard->probation_bytes : shard->protected_bytes) -= len;
  segment.pop_back();
  EraseLocked(shard, item.file, item.block);
  total_bytes_ -= len;
  --num_entries_;
  if (ImpaladMetrics::IO_MGR_S3_NATIVE_CACHE_EVICTIONS != nullptr) {
    ImpaladMetrics::IO_MGR_S3_NATIVE_CACHE_EVICTIONS->Increment(1);
  }
  return len;
}

void S3NativeFileCache::EvictLocked(
    Shard* shard, const LruItem& keep, std::vector<Evicted>* evicted) {
  while (shard->probation_bytes + shard->protected_bytes > shard->capacity) {
    bool from_probation = shard->probation.size() > 1
        || (!shard->probation.empty()
            && (shard->probation.back().file != keep.file
                || shard->probation.back().block != keep.block));
    if ((from_probation ? shard->probation : shard->protected_lru).empty()) break;
    RemoveLastLocked(shard, from_probation, evicted);
  }
}

void S3NativeFileCache::ReleaseMemory(int64_t bytes_to_free) {
  // The next headroom check clears it; without a headroom check it stays unset.
  if (headroom_fn_ && reserved_ < capacity_) low_headroom_ = true;
  Release(bytes_to_free, 0, ReleaseReason::kProcessLimit);
}

bool S3NativeFileCache::SkipFill(int64_t len) {
  if (!low_headroom_.load() || total_bytes_.load() + len <= reserved_) return false;
  if (ImpaladMetrics::IO_MGR_S3_NATIVE_CACHE_SKIPPED_BYTES != nullptr) {
    ImpaladMetrics::IO_MGR_S3_NATIVE_CACHE_SKIPPED_BYTES->Increment(len);
  }
  return true;
}

S3NativeFileCache::~S3NativeFileCache() {
  if (headroom_thread_ == nullptr) return;
  {
    std::lock_guard<std::mutex> l(stop_lock_);
    stop_ = true;
  }
  stop_cv_.notify_all();
  headroom_thread_->Join();
}

void S3NativeFileCache::SetHeadroomFunction(HeadroomFn fn, int64_t min_headroom,
    int64_t check_interval_ms, AfterReleaseFn after_release) {
  DCHECK(!headroom_fn_);
  headroom_fn_ = std::move(fn);
  after_release_fn_ = std::move(after_release);
  min_headroom_ = std::max<int64_t>(min_headroom, 0);
  if (check_interval_ms > 0 && reserved_ < capacity_) {
    Status status = Thread::Create("s3-native-file-cache", "headroom-check",
        [this, check_interval_ms] () { HeadroomCheckLoop(check_interval_ms); },
        &headroom_thread_);
    if (!status.ok()) {
      LOG(WARNING) << "Native S3 reader: background headroom check of the file cache "
                   << "not started, only the check on fill runs: " << status.GetDetail();
      headroom_thread_.reset();
    }
  }
}

void S3NativeFileCache::CheckHeadroom(int64_t filled) {
  if (filled <= 0 || !headroom_fn_ || reserved_ >= capacity_) return;
  if (filled_since_check_.fetch_add(filled) + filled < headroom_check_interval_) return;
  filled_since_check_ = 0;
  ReleaseForHeadroom();
}

void S3NativeFileCache::ReleaseForHeadroom() {
  int64_t headroom = headroom_fn_();
  bool low = headroom < min_headroom_;
  low_headroom_ = low;
  if (low && Release(min_headroom_ - headroom, reserved_, ReleaseReason::kHeadroom) > 0
      && after_release_fn_) {
    after_release_fn_();
  }
}

void S3NativeFileCache::HeadroomCheckLoop(int64_t interval_ms) {
  std::unique_lock<std::mutex> l(stop_lock_);
  while (!stop_cv_.wait_for(
      l, std::chrono::milliseconds(interval_ms), [this] () { return stop_; })) {
    l.unlock();
    // Nothing to give back below the reserved part: skip the metric read.
    if (total_bytes_.load() > reserved_) ReleaseForHeadroom();
    l.lock();
  }
}

int64_t S3NativeFileCache::Release(
    int64_t bytes_to_free, int64_t keep_bytes, ReleaseReason reason) {
  bytes_to_free = std::min(bytes_to_free, total_bytes_.load() - keep_bytes);
  if (bytes_to_free <= 0) return 0;
  int64_t freed = 0;
  const int64_t num_shards = shards_.size();
  // First an equal share from every shard, then whatever is still missing.
  for (int pass = 0; pass < 2 && freed < bytes_to_free; ++pass) {
    for (std::unique_ptr<Shard>& shard : shards_) {
      if (freed >= bytes_to_free) break;
      int64_t target = pass == 0 ? (bytes_to_free + num_shards - 1) / num_shards
                                 : bytes_to_free - freed;
      int64_t shard_freed = 0;
      std::lock_guard<std::mutex> l(shard->lock);
      while (shard_freed < target
          && !(shard->probation.empty() && shard->protected_lru.empty())) {
        shard_freed += RemoveLastLocked(shard.get(), !shard->probation.empty());
      }
      freed += shard_freed;
    }
  }
  UpdateGauges();
  if (freed <= 0) return 0;
#if !defined(ADDRESS_SANITIZER) && !defined(THREAD_SANITIZER)
  // The freed blocks go to tcmalloc's free lists, which still count as process memory
  // (memory.total-used): return them to the system so that the process headroom grows.
  MallocExtension::instance()->ReleaseToSystem(freed);
#endif
  const bool headroom = reason == ReleaseReason::kHeadroom;
  if (ImpaladMetrics::IO_MGR_S3_NATIVE_CACHE_RELEASED_BYTES != nullptr) {
    ImpaladMetrics::IO_MGR_S3_NATIVE_CACHE_RELEASED_BYTES->Increment(freed);
    (headroom ? ImpaladMetrics::IO_MGR_S3_NATIVE_CACHE_RELEASED_HEADROOM_BYTES
              : ImpaladMetrics::IO_MGR_S3_NATIVE_CACHE_RELEASED_LIMIT_BYTES)
        ->Increment(freed);
  }
  // Logged per reason, at most once per interval, with the bytes since the last line.
  static std::atomic<int64_t> last_log_ms[2] = {{0}, {0}};
  static std::atomic<int64_t> unlogged_bytes[2] = {{0}, {0}};
  const int r = static_cast<int>(reason);
  unlogged_bytes[r] += freed;
  int64_t now = MonotonicMillis();
  int64_t last = last_log_ms[r].load();
  if (now - last >= RELEASE_LOG_INTERVAL_MS
      && last_log_ms[r].compare_exchange_strong(last, now)) {
    LOG(INFO) << "Native S3 reader: released "
              << PrettyPrinter::PrintBytes(unlogged_bytes[r].exchange(0))
              << " of the file cache because "
              << (headroom ? "the process is close to its memory limit"
                           : "the process is over its memory limit")
              << " (cache now " << PrettyPrinter::PrintBytes(total_bytes_.load()) << ")";
  }
  return freed;
}

void S3NativeFileCache::UpdateGauges() {
  if (ImpaladMetrics::IO_MGR_S3_NATIVE_CACHE_TOTAL_BYTES != nullptr) {
    ImpaladMetrics::IO_MGR_S3_NATIVE_CACHE_TOTAL_BYTES->SetValue(
        total_bytes_.load());
  }
  if (ImpaladMetrics::IO_MGR_S3_NATIVE_CACHE_NUM_ENTRIES != nullptr) {
    ImpaladMetrics::IO_MGR_S3_NATIVE_CACHE_NUM_ENTRIES->SetValue(
        num_entries_.load());
  }
}

int64_t InitS3NativeFileCache(int64_t max_capacity) {
  DCHECK(file_cache == nullptr);
  if ((FLAGS_s3_native_whole_file_max_size <= 0 && FLAGS_s3_native_block_size <= 0)
      || FLAGS_s3_native_cache_capacity <= 0) {
    LOG(INFO) << "Native S3 reader: file cache is disabled";
    return 0;
  }
  int64_t capacity = FLAGS_s3_native_cache_capacity;
  if (max_capacity >= 0 && capacity > max_capacity) {
    LOG(INFO) << "Native S3 reader: file cache capacity reduced from "
              << PrettyPrinter::PrintBytes(capacity) << " to "
              << PrettyPrinter::PrintBytes(max_capacity) << " (at most "
              << "--s3_native_cache_max_mem_percent of the admit memory limit)";
    capacity = max_capacity;
  }
  if (capacity < std::max(FLAGS_s3_native_whole_file_max_size,
          FLAGS_s3_native_block_size)) {
    LOG(WARNING) << "Native S3 reader: file cache capacity ("
                 << PrettyPrinter::PrintBytes(capacity) << ") is smaller than "
                 << "--s3_native_whole_file_max_size or --s3_native_block_size; "
                 << "file cache is disabled";
    return 0;
  }
  int64_t reserved = std::min(
      std::max<int64_t>(FLAGS_s3_native_cache_reserved_capacity, 0), capacity);
  int protected_percent =
      std::min(std::max(FLAGS_s3_native_cache_protected_percent, 0), 100);
  // Each shard holds at least 8 of the largest entries.
  int64_t largest_entry =
      std::max(FLAGS_s3_native_whole_file_max_size, FLAGS_s3_native_block_size);
  int num_shards = static_cast<int>(
      std::min<int64_t>(std::max<int64_t>(capacity / (8 * largest_entry), 1), 16));
  file_cache = std::make_unique<S3NativeFileCache>(
      capacity, protected_percent, num_shards, reserved);
  LOG(INFO) << "Native S3 reader: file cache capacity "
            << PrettyPrinter::PrintBytes(capacity) << " ("
            << PrettyPrinter::PrintBytes(reserved) << " reserved, the rest uses memory "
            << "queries leave free) in " << num_shards
            << " shards, protected segment " << protected_percent << "%; "
            << (FLAGS_s3_native_whole_file_max_size > 0
                   ? "files up to "
                       + PrettyPrinter::PrintBytes(FLAGS_s3_native_whole_file_max_size)
                       + " are read whole, "
                   : std::string("whole-file reads off, "))
            << "files are cached in blocks of "
            << PrettyPrinter::PrintBytes(FLAGS_s3_native_block_size) << " (0 = off)";
  return reserved;
}

S3NativeFileCache* GetS3NativeFileCache() {
  return file_cache.get();
}

} // namespace io
} // namespace impala
