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

#include <atomic>
#include <chrono>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "runtime/io/s3-native-file-cache.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

namespace impala {
namespace io {

static const void* const OWNER_1 = reinterpret_cast<const void*>(0x1);
static const void* const OWNER_2 = reinterpret_cast<const void*>(0x2);
static const int64_t WHOLE = S3NativeFileCache::kWholeFile;

// Loader that fills the file with 'fill' and counts its calls in 'calls'.
static S3NativeFileCache::Loader FillLoader(char fill, std::atomic<int>* calls) {
  return [fill, calls](S3NativeFileCache::File* f) {
    if (calls != nullptr) ++*calls;
    memset(f->data.get(), fill, f->len);
    return Status::OK();
  };
}

// Looks up 'key' of 'length' bytes by 'owner' and returns whether it was a hit. On a
// miss the file is filled with the first character of 'key'.
static bool Read(S3NativeFileCache* cache, const string& key, int64_t length,
    const void* owner) {
  shared_ptr<const S3NativeFileCache::File> file;
  bool hit = false;
  Status status = cache->Lookup(key, length, owner, FillLoader(key[0], nullptr), &file,
      &hit);
  EXPECT_TRUE(status.ok()) << status.GetDetail();
  EXPECT_TRUE(file != nullptr);
  if (file != nullptr) EXPECT_EQ(key[0], file->data[0]);
  return hit;
}

TEST(S3NativeFileCacheTest, HitAfterLoad) {
  S3NativeFileCache cache(1000, 80);
  std::atomic<int> calls(0);
  shared_ptr<const S3NativeFileCache::File> file;
  bool hit = true;
  ASSERT_OK(cache.Lookup("a", 100, OWNER_1, FillLoader('x', &calls), &file, &hit));
  EXPECT_FALSE(hit);
  EXPECT_EQ(100, file->len);
  EXPECT_EQ('x', file->data[99]);
  ASSERT_OK(cache.Lookup("a", 100, OWNER_1, FillLoader('y', &calls), &file, &hit));
  EXPECT_TRUE(hit);
  EXPECT_EQ('x', file->data[0]);
  EXPECT_EQ(1, calls);
  EXPECT_EQ(100, cache.TotalBytes());
  EXPECT_EQ(1, cache.NumEntries());
}

// The object is shorter than the catalog says: only the loaded bytes are kept.
TEST(S3NativeFileCacheTest, ShortLoad) {
  S3NativeFileCache cache(1000, 80);
  shared_ptr<const S3NativeFileCache::File> file;
  bool hit;
  ASSERT_OK(cache.Lookup("a", 100, OWNER_1, [](S3NativeFileCache::File* f) {
    memset(f->data.get(), 'x', 40);
    f->len = 40;
    return Status::OK();
  }, &file, &hit));
  EXPECT_EQ(40, file->len);
  EXPECT_EQ(40, cache.TotalBytes());
}

// Concurrent misses on one file send one request; the other callers wait for it.
TEST(S3NativeFileCacheTest, ConcurrentMissesLoadOnce) {
  S3NativeFileCache cache(1000, 80);
  std::atomic<int> calls(0);
  std::atomic<int> misses(0);
  S3NativeFileCache::Loader slow_loader = [&calls](S3NativeFileCache::File* f) {
    ++calls;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    memset(f->data.get(), 'x', f->len);
    return Status::OK();
  };
  vector<std::thread> threads;
  for (int i = 0; i < 8; ++i) {
    threads.emplace_back([&]() {
      shared_ptr<const S3NativeFileCache::File> file;
      bool hit;
      Status status = cache.Lookup("a", 100, OWNER_1, slow_loader, &file, &hit);
      EXPECT_TRUE(status.ok()) << status.GetDetail();
      if (!hit) ++misses;
      EXPECT_EQ('x', file->data[50]);
    });
  }
  for (std::thread& t : threads) t.join();
  EXPECT_EQ(1, calls);
  EXPECT_EQ(1, misses);
}

// A failed load is returned to the caller and not cached: the next caller retries.
TEST(S3NativeFileCacheTest, FailedLoadIsNotCached) {
  S3NativeFileCache cache(1000, 80);
  shared_ptr<const S3NativeFileCache::File> file;
  bool hit;
  Status status = cache.Lookup("a", 100, OWNER_1, [](S3NativeFileCache::File* f) {
    return Status("injected error");
  }, &file, &hit);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(0, cache.NumEntries());
  EXPECT_FALSE(Read(&cache, "a", 100, OWNER_1));
  EXPECT_TRUE(Read(&cache, "a", 100, OWNER_1));
}

TEST(S3NativeFileCacheTest, EvictsLeastRecentlyUsedInProbation) {
  S3NativeFileCache cache(300, 80);
  EXPECT_FALSE(Read(&cache, "a", 100, OWNER_1));
  EXPECT_FALSE(Read(&cache, "b", 100, OWNER_1));
  EXPECT_FALSE(Read(&cache, "c", 100, OWNER_1));
  // A read by the same scan refreshes 'a' within probation.
  EXPECT_TRUE(Read(&cache, "a", 100, OWNER_1));
  EXPECT_FALSE(Read(&cache, "d", 100, OWNER_1));
  EXPECT_EQ(3, cache.NumEntries());
  EXPECT_EQ(300, cache.TotalBytes());
  EXPECT_TRUE(Read(&cache, "a", 100, OWNER_1));
  EXPECT_TRUE(Read(&cache, "c", 100, OWNER_1));
  EXPECT_TRUE(Read(&cache, "d", 100, OWNER_1));
  EXPECT_FALSE(Read(&cache, "b", 100, OWNER_1));
}

// A file read by another scan is protected: a large scan of new files evicts only
// probation.
TEST(S3NativeFileCacheTest, ReuseByAnotherScanIsProtected) {
  S3NativeFileCache cache(400, 50);
  EXPECT_FALSE(Read(&cache, "a", 100, OWNER_1));
  EXPECT_TRUE(Read(&cache, "a", 100, OWNER_2));
  // A scan of many new files by OWNER_1.
  for (char c = 'b'; c <= 'k'; ++c) {
    EXPECT_FALSE(Read(&cache, string(1, c), 100, OWNER_1));
  }
  EXPECT_TRUE(Read(&cache, "a", 100, OWNER_1));
  EXPECT_LE(cache.TotalBytes(), 400);
}

TEST(S3NativeFileCacheTest, ReleaseMemoryEvictsOldestFirst) {
  S3NativeFileCache cache(1000, 80);
  EXPECT_FALSE(Read(&cache, "a", 100, OWNER_1));
  EXPECT_FALSE(Read(&cache, "b", 100, OWNER_1));
  EXPECT_FALSE(Read(&cache, "c", 100, OWNER_1));
  cache.ReleaseMemory(150);
  EXPECT_EQ(1, cache.NumEntries());
  EXPECT_EQ(100, cache.TotalBytes());
  EXPECT_TRUE(Read(&cache, "c", 100, OWNER_1));
}

// When the process headroom drops below the margin, a fill gives the elastic part of
// the capacity back, least recently used first, but keeps the reserved part.
TEST(S3NativeFileCacheTest, ElasticPartReleasedWhenHeadroomLow) {
  S3NativeFileCache cache(1000, 80, 1, 300);
  EXPECT_EQ(300, cache.reserved());
  std::atomic<int64_t> headroom(S3NativeFileCache::kDefaultMinHeadroom);
  std::atomic<int> checks(0);
  cache.SetHeadroomFunction([&headroom, &checks] () {
    ++checks;
    return headroom.load();
  });
  for (char c = 'a'; c <= 'h'; ++c) Read(&cache, string(1, c), 100, OWNER_1);
  EXPECT_EQ(800, cache.TotalBytes());
  EXPECT_GT(checks, 0);

  headroom = 0;
  EXPECT_FALSE(Read(&cache, "i", 100, OWNER_1));
  EXPECT_EQ(300, cache.TotalBytes());
  EXPECT_EQ(3, cache.NumEntries());
  EXPECT_TRUE(Read(&cache, "i", 100, OWNER_1));
  EXPECT_FALSE(Read(&cache, "a", 100, OWNER_1));
  // The reserved part stays while the headroom is low.
  EXPECT_EQ(300, cache.TotalBytes());

  // Block reads check the headroom too.
  std::atomic<int> fetches(0);
  int64_t bytes_read = 0;
  S3NativeFileCache::BlockStats stats;
  vector<uint8_t> buffer(400);
  ASSERT_OK(cache.ReadBlocks("j", 400, 100, 0, buffer.data(), 400, OWNER_1,
      [&fetches] (int64_t offset, const S3NativeFileCache::Segments& segments,
          int64_t* received) {
        ++fetches;
        *received = 0;
        for (const auto& segment : segments) {
          memset(segment.first, 'j', segment.second);
          *received += segment.second;
        }
        return Status::OK();
      }, &bytes_read, &stats));
  EXPECT_EQ(400, bytes_read);
  EXPECT_EQ(1, fetches);
  EXPECT_EQ(300, cache.TotalBytes());

  // With enough headroom the cache grows to its capacity again.
  headroom = S3NativeFileCache::kDefaultMinHeadroom;
  for (char c = 'k'; c <= 'r'; ++c) Read(&cache, string(1, c), 100, OWNER_1);
  EXPECT_EQ(1000, cache.TotalBytes());
}

// While the last headroom check found the headroom low, new entries above the reserved
// part are not cached (the reader still gets the data); the next check with enough
// headroom lets the cache fill again.
TEST(S3NativeFileCacheTest, SkipsFillsWhileHeadroomLow) {
  S3NativeFileCache cache(1000, 80, 1, 300);
  std::atomic<int64_t> headroom(1000);
  cache.SetHeadroomFunction([&headroom] () { return headroom.load(); }, 500);
  for (char c = 'a'; c <= 'e'; ++c) Read(&cache, string(1, c), 100, OWNER_1);
  EXPECT_EQ(500, cache.TotalBytes());

  headroom = 0;
  // Cached, then the check gives back down to the reserved part.
  EXPECT_FALSE(Read(&cache, "f", 100, OWNER_1));
  EXPECT_EQ(300, cache.TotalBytes());
  // Not cached while the headroom is low.
  EXPECT_FALSE(Read(&cache, "g", 100, OWNER_1));
  EXPECT_EQ(300, cache.TotalBytes());
  EXPECT_FALSE(Read(&cache, "g", 100, OWNER_1));
  EXPECT_EQ(300, cache.TotalBytes());

  headroom = 1000;
  // Skipped: the flag is from the last check; this fill's check clears it.
  EXPECT_FALSE(Read(&cache, "h", 100, OWNER_1));
  EXPECT_EQ(300, cache.TotalBytes());
  EXPECT_FALSE(Read(&cache, "i", 100, OWNER_1));
  EXPECT_EQ(400, cache.TotalBytes());
  EXPECT_TRUE(Read(&cache, "i", 100, OWNER_1));
}

// The function given to SetHeadroomFunction() runs after the headroom check gave memory
// back, and only then.
TEST(S3NativeFileCacheTest, AfterReleaseCalledOnlyAfterRelease) {
  S3NativeFileCache cache(1000, 80, 1, 300);
  std::atomic<int64_t> headroom(1000);
  std::atomic<int> after(0);
  cache.SetHeadroomFunction([&headroom] () { return headroom.load(); }, 500, 0,
      [&after] () { ++after; });
  for (char c = 'a'; c <= 'e'; ++c) Read(&cache, string(1, c), 100, OWNER_1);
  EXPECT_EQ(0, after);
  headroom = 0;
  Read(&cache, "f", 100, OWNER_1);
  EXPECT_EQ(300, cache.TotalBytes());
  EXPECT_EQ(1, after);
  // Low headroom, but nothing above the reserved part to give back: not called.
  Read(&cache, "g", 100, OWNER_1);
  EXPECT_EQ(1, after);
}

// The background check gives the elastic part back while nothing is filled (queries
// grow after their scans), down to the reserved part; the destructor stops the thread.
TEST(S3NativeFileCacheTest, BackgroundHeadroomCheck) {
  std::atomic<int64_t> headroom(1000);
  {
    S3NativeFileCache cache(1000, 80, 1, 300);
    cache.SetHeadroomFunction([&headroom] () { return headroom.load(); }, 500, 5);
    for (char c = 'a'; c <= 'h'; ++c) Read(&cache, string(1, c), 100, OWNER_1);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(800, cache.TotalBytes());

    headroom = 0;
    for (int i = 0; i < 200 && cache.TotalBytes() > 300; ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_EQ(300, cache.TotalBytes());
    // The most recently used entries stay.
    EXPECT_TRUE(Read(&cache, "h", 100, OWNER_1));
  }
  // A fully reserved cache starts no thread and keeps its entries.
  S3NativeFileCache reserved(1000, 80);
  reserved.SetHeadroomFunction([] () { return 0; }, 500, 5);
  for (char c = 'a'; c <= 'e'; ++c) Read(&reserved, string(1, c), 100, OWNER_1);
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_EQ(500, reserved.TotalBytes());
}

// The eviction handler gets entries evicted to make room that were read again after
// they were loaded; entries read only once and entries given back under memory
// pressure do not reach it.
TEST(S3NativeFileCacheTest, EvictionHandlerGetsReusedEntries) {
  S3NativeFileCache cache(300, 80);
  vector<string> handled;
  cache.SetEvictionHandler([&handled] (const string& key, int64_t block,
      const S3NativeFileCache::File& file) {
    EXPECT_EQ(WHOLE, block);
    EXPECT_EQ(100, file.len);
    EXPECT_EQ(key[0], file.data[0]);
    handled.push_back(key);
  });
  EXPECT_FALSE(Read(&cache, "a", 100, OWNER_1));
  EXPECT_TRUE(Read(&cache, "a", 100, OWNER_1));
  EXPECT_FALSE(Read(&cache, "b", 100, OWNER_1));
  EXPECT_FALSE(Read(&cache, "c", 100, OWNER_1));
  // a is the least recently used entry and was read again: handled.
  EXPECT_FALSE(Read(&cache, "d", 100, OWNER_1));
  ASSERT_EQ(1, handled.size());
  EXPECT_EQ("a", handled[0]);
  // b was read only once: not handled.
  EXPECT_FALSE(Read(&cache, "e", 100, OWNER_1));
  EXPECT_EQ(1, handled.size());
  // Given back under memory pressure: not handled, also when reused.
  EXPECT_TRUE(Read(&cache, "e", 100, OWNER_1));
  cache.ReleaseMemory(cache.TotalBytes());
  EXPECT_EQ(0, cache.TotalBytes());
  EXPECT_EQ(1, handled.size());
}

// Blocks evicted from ReadBlocks() reach the handler with their block index.
TEST(S3NativeFileCacheTest, EvictionHandlerGetsBlocks) {
  S3NativeFileCache cache(300, 80);
  vector<int64_t> handled;
  cache.SetEvictionHandler([&handled] (const string& key, int64_t block,
      const S3NativeFileCache::File& file) {
    EXPECT_EQ("x", key);
    handled.push_back(block);
  });
  auto fill = [] (int64_t offset, const S3NativeFileCache::Segments& segments,
      int64_t* received) {
    *received = 0;
    for (const auto& segment : segments) {
      memset(segment.first, 'x', segment.second);
      *received += segment.second;
    }
    return Status::OK();
  };
  vector<uint8_t> buffer(1000);
  int64_t bytes_read = 0;
  S3NativeFileCache::BlockStats stats;
  // Blocks 0-2, then block 0 again, then blocks 3-4 evict blocks 1 and 2 (read once)
  // after block 0 is refreshed: none handled; then blocks 5-7 evict 0 (reused).
  ASSERT_OK(cache.ReadBlocks("x", 1000, 100, 0, buffer.data(), 300, OWNER_1, fill,
      &bytes_read, &stats));
  ASSERT_OK(cache.ReadBlocks("x", 1000, 100, 0, buffer.data(), 100, OWNER_1, fill,
      &bytes_read, &stats));
  ASSERT_OK(cache.ReadBlocks("x", 1000, 100, 300, buffer.data(), 200, OWNER_1, fill,
      &bytes_read, &stats));
  EXPECT_TRUE(handled.empty());
  ASSERT_OK(cache.ReadBlocks("x", 1000, 100, 500, buffer.data(), 300, OWNER_1, fill,
      &bytes_read, &stats));
  ASSERT_EQ(1, handled.size());
  EXPECT_EQ(0, handled[0]);
}

// A fully reserved cache (the default of the constructor) ignores the headroom; the
// process memory limit still frees it, also its reserved part.
TEST(S3NativeFileCacheTest, ReservedCacheIgnoresHeadroom) {
  S3NativeFileCache cache(1000, 80);
  EXPECT_EQ(1000, cache.reserved());
  cache.SetHeadroomFunction([] () { return 0; });
  for (char c = 'a'; c <= 'h'; ++c) Read(&cache, string(1, c), 100, OWNER_1);
  EXPECT_EQ(800, cache.TotalBytes());

  S3NativeFileCache elastic(1000, 80, 1, 300);
  for (char c = 'a'; c <= 'e'; ++c) Read(&elastic, string(1, c), 100, OWNER_1);
  elastic.ReleaseMemory(1000);
  EXPECT_EQ(0, elastic.TotalBytes());
}

// Waiters on a reserved entry get the result of Fill(), also an error.
TEST(S3NativeFileCacheTest, AcquireAndFill) {
  S3NativeFileCache cache(1000, 80);
  S3NativeFileCache::Ref first;
  cache.Acquire("a", WHOLE, OWNER_1, &first);
  ASSERT_TRUE(first.reserved);
  S3NativeFileCache::Ref second;
  cache.Acquire("a", WHOLE, OWNER_2, &second);
  ASSERT_FALSE(second.reserved);
  ASSERT_TRUE(second.pending.valid());
  cache.Fill("a", WHOLE, OWNER_1, Status("injected error"), nullptr);
  EXPECT_FALSE(second.pending.get().first.ok());
  EXPECT_EQ(0, cache.NumEntries());

  S3NativeFileCache::Ref third;
  cache.Acquire("a", WHOLE, OWNER_1, &third);
  ASSERT_TRUE(third.reserved);
  S3NativeFileCache::Ref fourth;
  cache.Acquire("a", WHOLE, OWNER_2, &fourth);
  std::shared_ptr<S3NativeFileCache::File> file =
      std::make_shared<S3NativeFileCache::File>(10);
  cache.Fill("a", WHOLE, OWNER_1, Status::OK(), file);
  EXPECT_EQ(10, fourth.pending.get().second->len);
  S3NativeFileCache::Ref fifth;
  cache.Acquire("a", WHOLE, OWNER_2, &fifth);
  EXPECT_EQ(file, fifth.file);
}

// A fake object of 'length' bytes whose byte at offset i is i % 251. Records the fetches
// (offset, total length) and the lengths of the segments of the last fetch.
class FakeObject {
 public:
  explicit FakeObject(int64_t length) : length_(length) {}

  S3NativeFileCache::Fetcher Fetcher() {
    return [this](int64_t offset, const S3NativeFileCache::Segments& segments,
               int64_t* bytes_read) {
      std::lock_guard<std::mutex> l(lock_);
      int64_t len = 0;
      last_segments_.clear();
      for (const auto& segment : segments) {
        len += segment.second;
        last_segments_.push_back(segment.second);
      }
      fetches_.emplace_back(offset, len);
      if (fail_) return Status("injected error");
      if (delay_ms_ > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms_));
      }
      *bytes_read = std::max<int64_t>(0, std::min(len, length_ - offset));
      int64_t pos = 0;
      for (const auto& segment : segments) {
        for (int64_t i = 0; i < segment.second && pos < *bytes_read; ++i, ++pos) {
          segment.first[i] = (offset + pos) % 251;
        }
      }
      return Status::OK();
    };
  }

  vector<std::pair<int64_t, int64_t>> TakeFetches() {
    std::lock_guard<std::mutex> l(lock_);
    vector<std::pair<int64_t, int64_t>> result;
    result.swap(fetches_);
    return result;
  }

  vector<int64_t> LastSegments() {
    std::lock_guard<std::mutex> l(lock_);
    return last_segments_;
  }

  int64_t length_;
  bool fail_ = false;
  int delay_ms_ = 0;

 private:
  std::mutex lock_;
  vector<std::pair<int64_t, int64_t>> fetches_;
  vector<int64_t> last_segments_;
};

// Reads [offset, offset + len) of a file of 'file_length' bytes with blocks of 100 bytes
// and checks the data. Returns the number of bytes read.
static int64_t ReadAndCheck(S3NativeFileCache* cache, FakeObject* object,
    int64_t file_length, int64_t offset, int64_t len, const void* owner = OWNER_1,
    S3NativeFileCache::BlockStats* stats = nullptr) {
  vector<uint8_t> buffer(len, 0xff);
  int64_t bytes_read = -1;
  S3NativeFileCache::BlockStats local_stats;
  Status status = cache->ReadBlocks("f@1", file_length, 100, offset, buffer.data(),
      len, owner, object->Fetcher(), &bytes_read, stats ? stats : &local_stats);
  EXPECT_TRUE(status.ok()) << status.GetDetail();
  for (int64_t i = 0; i < bytes_read; ++i) {
    if (buffer[i] != (offset + i) % 251) {
      ADD_FAILURE() << "wrong byte at offset " << offset + i;
      break;
    }
  }
  return bytes_read;
}

typedef vector<std::pair<int64_t, int64_t>> Fetches;

// Consecutive missing blocks are fetched with one request; later reads within them are
// served from the cache.
TEST(S3NativeFileCacheTest, ReadBlocksCoalescesMisses) {
  S3NativeFileCache cache(10000, 80);
  FakeObject object(1000);
  S3NativeFileCache::BlockStats stats;
  EXPECT_EQ(300, ReadAndCheck(&cache, &object, 1000, 50, 300, OWNER_1, &stats));
  EXPECT_EQ(Fetches({{0, 400}}), object.TakeFetches());
  EXPECT_EQ(4, stats.missed_blocks);
  EXPECT_EQ(0, stats.hit_blocks);
  S3NativeFileCache::BlockStats stats2;
  EXPECT_EQ(260, ReadAndCheck(&cache, &object, 1000, 120, 260, OWNER_2, &stats2));
  EXPECT_TRUE(object.TakeFetches().empty());
  EXPECT_EQ(3, stats2.hit_blocks);
  EXPECT_EQ(260, stats2.hit_bytes);
  EXPECT_EQ(4, cache.NumEntries());
  EXPECT_EQ(400, cache.TotalBytes());
}

// Only the missing blocks are fetched, one request per run of missing blocks.
TEST(S3NativeFileCacheTest, ReadBlocksFetchesOnlyMissingRuns) {
  S3NativeFileCache cache(10000, 80);
  FakeObject object(1000);
  ReadAndCheck(&cache, &object, 1000, 0, 100);
  ReadAndCheck(&cache, &object, 1000, 250, 10);
  object.TakeFetches();
  EXPECT_EQ(500, ReadAndCheck(&cache, &object, 1000, 0, 500));
  EXPECT_EQ(Fetches({{100, 100}, {300, 200}}), object.TakeFetches());
}

// Reads past the end of the file return the bytes up to the end.
TEST(S3NativeFileCacheTest, ReadBlocksEndOfFile) {
  S3NativeFileCache cache(10000, 80);
  FakeObject object(250);
  EXPECT_EQ(50, ReadAndCheck(&cache, &object, 250, 200, 200));
  EXPECT_EQ(Fetches({{200, 50}}), object.TakeFetches());
  EXPECT_EQ(0, ReadAndCheck(&cache, &object, 250, 250, 100));
  EXPECT_EQ(0, ReadAndCheck(&cache, &object, 250, 300, 100));
  EXPECT_TRUE(object.TakeFetches().empty());
}

// The object is shorter than the catalog says: the read ends at the end of the object,
// also when served from the cache.
TEST(S3NativeFileCacheTest, ReadBlocksShortObject) {
  S3NativeFileCache cache(10000, 80);
  FakeObject object(250);
  EXPECT_EQ(50, ReadAndCheck(&cache, &object, 400, 200, 200));
  EXPECT_EQ(Fetches({{200, 200}}), object.TakeFetches());
  EXPECT_EQ(50, ReadAndCheck(&cache, &object, 400, 200, 200));
  EXPECT_EQ(0, ReadAndCheck(&cache, &object, 400, 300, 100));
  EXPECT_TRUE(object.TakeFetches().empty());
}

// A failed fetch is returned and not cached: the next read fetches again.
TEST(S3NativeFileCacheTest, ReadBlocksFetchError) {
  S3NativeFileCache cache(10000, 80);
  FakeObject object(1000);
  object.fail_ = true;
  vector<uint8_t> buffer(300);
  int64_t bytes_read;
  S3NativeFileCache::BlockStats stats;
  EXPECT_FALSE(cache.ReadBlocks("f@1", 1000, 100, 0, buffer.data(), 300, OWNER_1,
      object.Fetcher(), &bytes_read, &stats).ok());
  EXPECT_EQ(0, cache.NumEntries());
  object.fail_ = false;
  object.TakeFetches();
  EXPECT_EQ(300, ReadAndCheck(&cache, &object, 1000, 0, 300));
  EXPECT_EQ(Fetches({{0, 300}}), object.TakeFetches());
}

// Concurrent reads of the same range fetch each block once.
TEST(S3NativeFileCacheTest, ReadBlocksConcurrentReadsFetchOnce) {
  S3NativeFileCache cache(10000, 80);
  FakeObject object(1000);
  object.delay_ms_ = 100;
  vector<std::thread> threads;
  for (int i = 0; i < 8; ++i) {
    threads.emplace_back([&]() {
      EXPECT_EQ(500, ReadAndCheck(&cache, &object, 1000, 0, 500));
    });
  }
  for (std::thread& t : threads) t.join();
  int64_t fetched = 0;
  for (const auto& f : object.TakeFetches()) fetched += f.second;
  EXPECT_EQ(500, fetched);
}

// A run of missing blocks is fetched straight into the blocks: one segment per block,
// the last block of the file shorter.
TEST(S3NativeFileCacheTest, ReadBlocksFetchesIntoBlocks) {
  S3NativeFileCache cache(10000, 80);
  FakeObject object(250);
  EXPECT_EQ(250, ReadAndCheck(&cache, &object, 250, 0, 250));
  EXPECT_EQ(Fetches({{0, 250}}), object.TakeFetches());
  EXPECT_EQ(vector<int64_t>({100, 100, 50}), object.LastSegments());
  EXPECT_EQ(3, cache.NumEntries());
  EXPECT_EQ(250, cache.TotalBytes());
}

// Whole files and blocks of the same file key are separate entries.
TEST(S3NativeFileCacheTest, WholeFileAndBlocksAreSeparate) {
  S3NativeFileCache cache(10000, 80);
  EXPECT_FALSE(Read(&cache, "f@1", 100, OWNER_1));
  FakeObject object(1000);
  EXPECT_EQ(100, ReadAndCheck(&cache, &object, 1000, 0, 100));
  EXPECT_EQ(Fetches({{0, 100}}), object.TakeFetches());
  EXPECT_TRUE(Read(&cache, "f@1", 100, OWNER_1));
  EXPECT_EQ(2, cache.NumEntries());
}

// With several shards the totals cover all shards, each shard keeps to its share of the
// capacity, and ReleaseMemory() frees entries in all shards.
TEST(S3NativeFileCacheTest, ShardsKeepTotalsAndCapacity) {
  S3NativeFileCache cache(1000, 80, 4);
  EXPECT_EQ(4, cache.num_shards());
  for (int i = 0; i < 40; ++i) Read(&cache, "f" + std::to_string(i), 100, OWNER_1);
  EXPECT_LE(cache.TotalBytes(), 1000);
  EXPECT_GT(cache.TotalBytes(), 0);
  EXPECT_EQ(cache.TotalBytes(), cache.NumEntries() * 100);
  cache.ReleaseMemory(cache.TotalBytes());
  EXPECT_EQ(0, cache.TotalBytes());
  EXPECT_EQ(0, cache.NumEntries());
}

// Concurrent block reads of many files in a sharded cache return the right data.
TEST(S3NativeFileCacheTest, ShardedConcurrentReads) {
  S3NativeFileCache cache(100000, 80, 8);
  FakeObject object(1000);
  vector<std::thread> threads;
  for (int t = 0; t < 8; ++t) {
    threads.emplace_back([&, t]() {
      for (int i = 0; i < 50; ++i) {
        string key = "f" + std::to_string((t * 7 + i) % 20);
        vector<uint8_t> buffer(300);
        int64_t bytes_read;
        S3NativeFileCache::BlockStats stats;
        int64_t offset = (i * 37) % 700;
        Status status = cache.ReadBlocks(key, 1000, 100, offset, buffer.data(), 300,
            OWNER_1, object.Fetcher(), &bytes_read, &stats);
        EXPECT_TRUE(status.ok()) << status.GetDetail();
        EXPECT_EQ(300, bytes_read);
        for (int64_t j = 0; j < bytes_read; ++j) {
          if (buffer[j] != (offset + j) % 251) {
            ADD_FAILURE() << key << ": wrong byte at offset " << offset + j;
            break;
          }
        }
      }
    });
  }
  for (std::thread& t : threads) t.join();
  EXPECT_EQ(cache.TotalBytes(), cache.NumEntries() * 100);
}

} // namespace io
} // namespace impala
