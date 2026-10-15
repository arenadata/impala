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
#include <cstring>
#include <vector>

#include <gflags/gflags.h>

#include "common/logging.h"
#include "util/impalad-metrics.h"
#include "util/metrics.h"
#include "util/pretty-printer.h"

DECLARE_int64(s3_native_whole_file_max_size);
DECLARE_int64(s3_native_block_size);
DECLARE_int64(s3_native_file_cache_capacity);
DECLARE_int32(s3_native_file_cache_protected_percent);

namespace impala {
namespace io {

namespace {

// Created once by InitS3NativeFileCache() and kept for the lifetime of the process.
std::unique_ptr<S3NativeFileCache> file_cache;

} // anonymous namespace

Status S3NativeFileCache::Lookup(const std::string& key, int64_t length,
    const void* owner, const Loader& loader, std::shared_ptr<const File>* file,
    bool* hit) {
  DCHECK_GT(length, 0);
  Ref ref;
  Acquire(key, owner, &ref);
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
  Fill(key, owner, status, loaded);
  RETURN_IF_ERROR(status);
  *file = std::move(loaded);
  return Status::OK();
}

void S3NativeFileCache::Acquire(const std::string& key, const void* owner, Ref* ref) {
  std::lock_guard<std::mutex> l(lock_);
  auto it = entries_.find(key);
  if (it == entries_.end()) {
    // Miss: the caller loads the entry. Later callers wait for 'load_promise'.
    Entry& entry = entries_[key];
    entry.load_promise = std::make_shared<std::promise<LoadResult>>();
    entry.loading = entry.load_promise->get_future().share();
    ref->reserved = true;
  } else if (it->second.file != nullptr) {
    TouchLocked(&it->second, owner);
    ref->file = it->second.file;
  } else {
    ref->pending = it->second.loading;
  }
}

void S3NativeFileCache::Fill(const std::string& key, const void* owner,
    const Status& status, std::shared_ptr<const File> file) {
  std::shared_ptr<std::promise<LoadResult>> load_promise;
  {
    std::lock_guard<std::mutex> l(lock_);
    auto it = entries_.find(key);
    DCHECK(it != entries_.end());
    DCHECK(it->second.file == nullptr);
    load_promise = std::move(it->second.load_promise);
    if (status.ok() && file != nullptr && file->len <= capacity_) {
      probation_.push_front(key);
      it->second.file = file;
      it->second.loading = std::shared_future<LoadResult>();
      it->second.owner = owner;
      it->second.is_protected = false;
      it->second.lru_pos = probation_.begin();
      probation_bytes_ += file->len;
      EvictLocked(key);
    } else {
      // Nothing to cache: either the load failed (the next caller retries it) or the
      // entry is larger than the whole cache.
      entries_.erase(it);
    }
    UpdateGaugesLocked();
  }
  DCHECK(load_promise != nullptr);
  load_promise->set_value(LoadResult(status, std::move(file)));
}

Status S3NativeFileCache::ReadBlocks(const std::string& key_prefix, int64_t file_length,
    int64_t block_size, int64_t offset, uint8_t* buffer, int64_t len,
    const void* owner, const Fetcher& fetch, int64_t* bytes_read, BlockStats* stats) {
  DCHECK_GT(block_size, 0);
  DCHECK_GE(offset, 0);
  *bytes_read = 0;
  int64_t end = std::min(offset + len, file_length);
  if (offset >= end) return Status::OK();
  const int64_t first_block = offset / block_size;
  const int num_blocks = static_cast<int>((end - 1) / block_size - first_block + 1);
  std::vector<std::string> keys(num_blocks);
  std::vector<Ref> refs(num_blocks);
  for (int i = 0; i < num_blocks; ++i) {
    keys[i] = key_prefix + std::to_string(first_block + i);
    Acquire(keys[i], owner, &refs[i]);
  }

  // Load the blocks reserved by this thread first, a run of consecutive blocks with one
  // fetch, and only then wait for blocks loaded by other threads: two readers never wait
  // for each other's reservations. Every reservation is filled, also after an error.
  Status status;
  for (int i = 0; i < num_blocks;) {
    if (!refs[i].reserved) {
      ++i;
      continue;
    }
    int j = i;
    while (j < num_blocks && refs[j].reserved) ++j;
    int64_t run_offset = (first_block + i) * block_size;
    int64_t run_len = std::min((first_block + j) * block_size, file_length) - run_offset;
    std::unique_ptr<uint8_t[]> run(new uint8_t[run_len]);
    int64_t received = 0;
    if (status.ok()) status = fetch(run_offset, run.get(), run_len, &received);
    for (int k = i; k < j; ++k) {
      if (!status.ok()) {
        Fill(keys[k], owner, status, nullptr);
        continue;
      }
      // The object may be shorter than the catalog says: keep what was received.
      int64_t block_offset = (first_block + k) * block_size - run_offset;
      int64_t block_len = std::min(block_size, run_len - block_offset);
      block_len = std::max<int64_t>(0, std::min(block_len, received - block_offset));
      std::shared_ptr<File> block = std::make_shared<File>(block_len);
      memcpy(block->data.get(), run.get() + block_offset, block_len);
      refs[k].file = block;
      Fill(keys[k], owner, Status::OK(), std::move(block));
    }
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

void S3NativeFileCache::TouchLocked(Entry* entry, const void* owner) {
  if (entry->is_protected) {
    protected_.splice(protected_.begin(), protected_, entry->lru_pos);
    return;
  }
  if (owner == entry->owner) {
    // Another read by the scan that loaded the file: keep it in probation.
    probation_.splice(probation_.begin(), probation_, entry->lru_pos);
    return;
  }
  // Reused by another reader: promote to the protected segment.
  protected_.splice(protected_.begin(), probation_, entry->lru_pos);
  entry->is_protected = true;
  probation_bytes_ -= entry->file->len;
  protected_bytes_ += entry->file->len;
  if (ImpaladMetrics::IO_MGR_S3_NATIVE_FILE_CACHE_PROMOTIONS != nullptr) {
    ImpaladMetrics::IO_MGR_S3_NATIVE_FILE_CACHE_PROMOTIONS->Increment(1);
  }
  DemoteLocked();
}

void S3NativeFileCache::DemoteLocked() {
  while (protected_bytes_ > protected_capacity_ && protected_.size() > 1) {
    auto it = entries_.find(protected_.back());
    DCHECK(it != entries_.end());
    Entry& entry = it->second;
    probation_.splice(probation_.begin(), protected_, entry.lru_pos);
    entry.is_protected = false;
    // Any later read promotes the file again: it has been reused before.
    entry.owner = nullptr;
    protected_bytes_ -= entry.file->len;
    probation_bytes_ += entry.file->len;
  }
}

int64_t S3NativeFileCache::RemoveLastLocked(bool from_probation) {
  std::list<std::string>& segment = from_probation ? probation_ : protected_;
  DCHECK(!segment.empty());
  auto it = entries_.find(segment.back());
  DCHECK(it != entries_.end());
  int64_t len = it->second.file->len;
  (from_probation ? probation_bytes_ : protected_bytes_) -= len;
  entries_.erase(it);
  segment.pop_back();
  if (ImpaladMetrics::IO_MGR_S3_NATIVE_FILE_CACHE_EVICTIONS != nullptr) {
    ImpaladMetrics::IO_MGR_S3_NATIVE_FILE_CACHE_EVICTIONS->Increment(1);
  }
  return len;
}

void S3NativeFileCache::EvictLocked(const std::string& keep) {
  while (probation_bytes_ + protected_bytes_ > capacity_) {
    bool from_probation =
        probation_.size() > 1 || (!probation_.empty() && probation_.back() != keep);
    if ((from_probation ? probation_ : protected_).empty()) break;
    RemoveLastLocked(from_probation);
  }
}

void S3NativeFileCache::ReleaseMemory(int64_t bytes_to_free) {
  int64_t freed = 0;
  {
    std::lock_guard<std::mutex> l(lock_);
    while (freed < bytes_to_free && !(probation_.empty() && protected_.empty())) {
      freed += RemoveLastLocked(/* from_probation */ !probation_.empty());
    }
    UpdateGaugesLocked();
  }
  if (freed > 0) {
    LOG(INFO) << "Native S3 reader: released " << PrettyPrinter::PrintBytes(freed)
              << " of the file cache because the process is over its memory limit";
  }
}

int64_t S3NativeFileCache::TotalBytes() {
  std::lock_guard<std::mutex> l(lock_);
  return probation_bytes_ + protected_bytes_;
}

int64_t S3NativeFileCache::NumEntries() {
  std::lock_guard<std::mutex> l(lock_);
  return probation_.size() + protected_.size();
}

void S3NativeFileCache::UpdateGaugesLocked() {
  if (ImpaladMetrics::IO_MGR_S3_NATIVE_FILE_CACHE_TOTAL_BYTES != nullptr) {
    ImpaladMetrics::IO_MGR_S3_NATIVE_FILE_CACHE_TOTAL_BYTES->SetValue(
        probation_bytes_ + protected_bytes_);
  }
  if (ImpaladMetrics::IO_MGR_S3_NATIVE_FILE_CACHE_NUM_ENTRIES != nullptr) {
    ImpaladMetrics::IO_MGR_S3_NATIVE_FILE_CACHE_NUM_ENTRIES->SetValue(
        probation_.size() + protected_.size());
  }
}

int64_t InitS3NativeFileCache(int64_t max_capacity) {
  DCHECK(file_cache == nullptr);
  if ((FLAGS_s3_native_whole_file_max_size <= 0 && FLAGS_s3_native_block_size <= 0)
      || FLAGS_s3_native_file_cache_capacity <= 0) {
    LOG(INFO) << "Native S3 reader: file cache is disabled";
    return 0;
  }
  int64_t capacity = FLAGS_s3_native_file_cache_capacity;
  if (max_capacity >= 0 && capacity > max_capacity) {
    LOG(INFO) << "Native S3 reader: file cache capacity reduced from "
              << PrettyPrinter::PrintBytes(capacity) << " to "
              << PrettyPrinter::PrintBytes(max_capacity) << " (at most "
              << "--s3_native_file_cache_max_mem_percent of the admit memory limit)";
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
  int protected_percent =
      std::min(std::max(FLAGS_s3_native_file_cache_protected_percent, 0), 100);
  file_cache = std::make_unique<S3NativeFileCache>(capacity, protected_percent);
  LOG(INFO) << "Native S3 reader: file cache capacity "
            << PrettyPrinter::PrintBytes(capacity) << ", protected segment "
            << protected_percent << "%; files up to "
            << PrettyPrinter::PrintBytes(FLAGS_s3_native_whole_file_max_size)
            << " are read whole, larger files are cached in blocks of "
            << PrettyPrinter::PrintBytes(FLAGS_s3_native_block_size)
            << " (0 = off)";
  return capacity;
}

S3NativeFileCache* GetS3NativeFileCache() {
  return file_cache.get();
}

} // namespace io
} // namespace impala
