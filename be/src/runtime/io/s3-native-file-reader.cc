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

#include "runtime/io/s3-native-file-reader.h"

#include <algorithm>
#include <cstring>
#include <functional>
#include <memory>
#include <vector>

#include <gflags/gflags.h>

#ifdef IMPALA_HAVE_AWS_SDK_CPP
#include <aws/core/http/HttpResponse.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectResult.h>
#endif

#include "gutil/strings/substitute.h"
#include "runtime/io/data-cache.h"
#include "runtime/io/disk-io-mgr-internal.h"
#include "runtime/io/request-context.h"
#include "runtime/io/request-ranges.h"
#include "runtime/io/s3-native-client.h"
#include "runtime/io/s3-native-file-cache.h"
#include "util/debug-util.h"
#include "util/histogram-metric.h"
#include "util/impalad-metrics.h"
#include "util/metrics.h"

#include "common/names.h"

DECLARE_int64(s3_native_parallel_read_min_size);
DECLARE_int64(s3_native_parallel_read_part_size);
DECLARE_int64(s3_native_whole_file_max_size);
DECLARE_int64(s3_native_block_size);
DECLARE_bool(s3_native_data_cache);
DECLARE_bool(s3_native_data_cache_write_on_fetch);

using strings::Substitute;

namespace impala {
namespace io {

namespace {

// The remote data cache used for blocks and whole files of the native S3 file cache, or
// null. Set once by EnableS3NativeDataCache() before any read.
DataCache* native_data_cache = nullptr;

} // anonymous namespace

void EnableS3NativeDataCache(DataCache* data_cache) {
  if (data_cache == nullptr || !FLAGS_s3_native_data_cache) return;
  native_data_cache = data_cache;
  S3NativeFileCache* file_cache = GetS3NativeFileCache();
  if (file_cache == nullptr || FLAGS_s3_native_block_size <= 0) {
    LOG(INFO) << "Native S3 reader: reads that do not go through the file cache use the "
              << "remote data cache on local disk like the S3A reader";
    return;
  }
  if (FLAGS_s3_native_data_cache_write_on_fetch) {
    // FetchSegments() stores each block fetched from S3.
    LOG(INFO) << "Native S3 reader: blocks fetched from S3 are also stored in the remote "
              << "data cache on local disk";
    return;
  }
  const int64_t block_size = FLAGS_s3_native_block_size;
  // Keys: the file cache key (bucket/key@mtime, so a rewritten file is never served) and
  // the block offset; mtime 0. Insertion is opportunistic: the data cache drops it when
  // its writers are busy (--data_cache_write_concurrency, asynchronous writes with
  // --data_cache_num_async_write_threads).
  // Whole files are stored when they are read (ReadFromCachedFile()), so only blocks.
  file_cache->SetEvictionHandler([data_cache, block_size] (const std::string& file_key,
      int64_t block, const S3NativeFileCache::File& file) {
    if (block == S3NativeFileCache::kWholeFile || file.len <= 0) return;
    if (data_cache->Store(file_key, 0, block * block_size, file.data.get(), file.len)
        && ImpaladMetrics::IO_MGR_S3_NATIVE_DATA_CACHE_OFFERED_BYTES != nullptr) {
      ImpaladMetrics::IO_MGR_S3_NATIVE_DATA_CACHE_OFFERED_BYTES->Increment(file.len);
    }
  });
  LOG(INFO) << "Native S3 reader: blocks of the file cache that were read again are "
            << "kept in the remote data cache on local disk after their eviction";
}

#ifdef IMPALA_HAVE_AWS_SDK_CPP
namespace {

const char* const kAllocationTag = "ImpalaS3NativeFileReader";

/// Response stream that writes the GetObject body straight into a caller-owned buffer.
/// The SDK creates a new stream for every attempt, so a retried request writes from the
/// start of the buffer again. The stream buffer is set up before the iostream uses it:
/// the base class only stores the pointer.
class BufferIOStream : public Aws::IOStream {
 public:
  BufferIOStream(uint8_t* buffer, int64_t length)
    : Aws::IOStream(&stream_buf_), stream_buf_(buffer, length) {}

 private:
  Aws::Utils::Stream::PreallocatedStreamBuf stream_buf_;
};

/// Like BufferIOStream, but writes the body into several buffers one after another
/// (S3SegmentStreamBuf), e.g. straight into the blocks of the native S3 file cache.
class SegmentIOStream : public Aws::IOStream {
 public:
  explicit SegmentIOStream(const S3SegmentStreamBuf::Segments& segments)
    : Aws::IOStream(&stream_buf_), stream_buf_(segments) {}

 private:
  S3SegmentStreamBuf stream_buf_;
};

/// Ranged GET of 'len' bytes at 'offset' whose body goes to the streams of 'factory'.
Aws::S3::Model::GetObjectRequest MakeGetRequest(const string& bucket, const string& key,
    int64_t offset, int64_t len, std::function<Aws::IOStream*()> factory) {
  Aws::S3::Model::GetObjectRequest request;
  request.SetBucket(bucket.c_str());
  request.SetKey(key.c_str());
  request.SetRange(Substitute("bytes=$0-$1", offset, offset + len - 1).c_str());
  request.SetResponseStreamFactory(std::move(factory));
  return request;
}

/// Ranged GET of 'len' bytes at 'offset' whose body is written into 'buffer'.
Aws::S3::Model::GetObjectRequest MakeGetRequest(const string& bucket, const string& key,
    int64_t offset, uint8_t* buffer, int64_t len) {
  return MakeGetRequest(bucket, key, offset, len, [buffer, len]() {
    return Aws::New<BufferIOStream>(kAllocationTag, buffer, len);
  });
}

/// Checks the outcome of a ranged GET of 'len' bytes and sets 'bytes_read'. HTTP 416
/// means the range starts at or past the end of the object: 0 bytes, not an error.
Status CheckGetOutcome(const Aws::S3::Model::GetObjectOutcome& outcome, int64_t len,
    int64_t* bytes_read) {
  *bytes_read = 0;
  if (!outcome.IsSuccess()) {
    const auto& error = outcome.GetError();
    if (error.GetResponseCode()
        == Aws::Http::HttpResponseCode::REQUESTED_RANGE_NOT_SATISFIABLE) {
      return Status::OK();
    }
    return Status(Substitute("HTTP $0 $1: $2", static_cast<int>(error.GetResponseCode()),
        string(error.GetExceptionName().c_str()), string(error.GetMessage().c_str())));
  }
  // The body size is taken from the stream position, not only from Content-Length:
  // aws-sdk-cpp reports 0 when the header is absent (chunked transfer encoding).
  const Aws::S3::Model::GetObjectResult& result = outcome.GetResult();
  int64_t bytes_written = static_cast<int64_t>(result.GetBody().tellp());
  bool eof;
  return CheckS3RangeRead(
      len, bytes_written, result.GetContentLength(), bytes_read, &eof);
}

} // anonymous namespace
#endif

Status S3NativeFileReader::ParsePath() {
  if (!bucket_.empty()) return Status::OK();
  return ParseS3Path(*scan_range_->file_string(), &bucket_, &key_);
}

Status S3NativeFileReader::Open() {
  unique_lock<SpinLock> fs_lock(lock_);
  RETURN_IF_ERROR(scan_range_->cancel_status_);
  return ParsePath();
}

Status S3NativeFileReader::ReadFromPos(DiskQueue* queue, int64_t file_offset,
    uint8_t* buffer, int64_t bytes_to_read, int64_t* bytes_read, bool* eof) {
  DCHECK(scan_range_->read_in_flight());
  DCHECK_GE(bytes_to_read, 0);
  unique_lock<SpinLock> fs_lock(lock_);
  RETURN_IF_ERROR(scan_range_->cancel_status_);
  *eof = false;
  *bytes_read = 0;
  if (bytes_to_read == 0) return Status::OK();
  RETURN_IF_ERROR(ParsePath());

#ifdef IMPALA_HAVE_AWS_SDK_CPP
  ScopedTimer<MonotonicStopWatch> req_context_read_timer(
      scan_range_->reader_->read_timer_);
  ScopedHistogramTimer read_timer(queue->read_latency());
  int64_t file_length = scan_range_->file_length();
  // Footer reads are counted separately: whether they were served from the cache and how
  // long they took (a footer GET is one more round trip before the column chunks).
  bool footer = IsS3TailRead(file_offset, bytes_to_read, file_length);
  int64_t gets_before = gets_sent_;
  MonotonicStopWatch footer_timer;
  if (footer) footer_timer.Start();
  bool cache = GetS3NativeFileCache() != nullptr && file_length > 0;
  if (cache && S3ReadsWholeFile(file_length, FLAGS_s3_native_whole_file_max_size)) {
    RETURN_IF_ERROR(
        ReadFromCachedFile(file_offset, buffer, bytes_to_read, file_length, bytes_read));
  } else if (cache && FLAGS_s3_native_block_size > 0) {
    RETURN_IF_ERROR(ReadFromCachedBlocks(
        file_offset, buffer, bytes_to_read, file_length, bytes_read));
  } else if (native_data_cache != nullptr && scan_range_->UseDataCache()) {
    RETURN_IF_ERROR(ReadThroughDataCache(file_offset, buffer, bytes_to_read, bytes_read));
  } else {
    RETURN_IF_ERROR(GetRange(file_offset, buffer, bytes_to_read, bytes_read));
  }
  if (footer && ImpaladMetrics::IO_MGR_S3_NATIVE_FOOTER_READ_COUNT != nullptr) {
    ImpaladMetrics::IO_MGR_S3_NATIVE_FOOTER_READ_COUNT->Increment(1);
    ImpaladMetrics::IO_MGR_S3_NATIVE_FOOTER_READ_TIME_US->Increment(
        footer_timer.ElapsedTime() / 1000);
    if (gets_sent_ > gets_before) {
      ImpaladMetrics::IO_MGR_S3_NATIVE_FOOTER_READ_MISS_COUNT->Increment(1);
    }
  }
  // A shorter read means the range went past the end of the object.
  *eof = *bytes_read < bytes_to_read;
  queue->read_size()->Update(*bytes_read);
  return Status::OK();
#else
  return Status("Native S3 reader is not available: impalad was built without "
      "aws-sdk-cpp");
#endif
}

Status S3NativeFileReader::GetRange(int64_t offset, uint8_t* buffer, int64_t len,
    int64_t* bytes_read) {
  DCHECK_GT(len, 0);
  *bytes_read = 0;
#ifdef IMPALA_HAVE_AWS_SDK_CPP
  int64_t part_size = S3ReadPartSize(len, FLAGS_s3_native_parallel_read_min_size,
      FLAGS_s3_native_parallel_read_part_size);
  int num_parts = static_cast<int>((len + part_size - 1) / part_size);
  Aws::S3::S3Client* client = GetS3NativeClient();
  MonotonicStopWatch get_timer;
  get_timer.Start();

  // Parts 1.. run on the client's executor, part 0 in this thread.
  vector<Aws::S3::Model::GetObjectOutcomeCallable> other_parts;
  other_parts.reserve(num_parts - 1);
  for (int i = 1; i < num_parts; ++i) {
    int64_t part_offset = i * part_size;
    other_parts.push_back(client->GetObjectCallable(MakeGetRequest(bucket_, key_,
        offset + part_offset, buffer + part_offset, min(part_size, len - part_offset))));
  }
  // Outcomes are not copyable (they own the response stream), so each one is checked as
  // soon as it is available; only the byte counts and the first error are kept.
  Status status;
  vector<int64_t> received(num_parts, 0);
  auto check_part = [&](int i, const Aws::S3::Model::GetObjectOutcome& outcome) {
    int64_t part_offset = i * part_size;
    int64_t part_len = min(part_size, len - part_offset);
    Status part_status = CheckGetOutcome(outcome, part_len, &received[i]);
    if (!part_status.ok() && status.ok()) {
      status = Status(TErrorCode::DISK_IO_ERROR, GetBackendString(),
          Substitute("Error reading S3 object s3a://$0/$1 at offset $2 ($3 bytes): $4",
              bucket_, key_, offset + part_offset, part_len, part_status.msg().msg()));
    }
  };
  check_part(0, client->GetObject(
      MakeGetRequest(bucket_, key_, offset, buffer, min(part_size, len))));
  // Wait for every part before returning, even after an error: they write into 'buffer'.
  for (int i = 1; i < num_parts; ++i) check_part(i, other_parts[i - 1].get());
  gets_sent_ += num_parts;
  if (ImpaladMetrics::IO_MGR_S3_NATIVE_GET_COUNT != nullptr) {
    ImpaladMetrics::IO_MGR_S3_NATIVE_GET_COUNT->Increment(num_parts);
    ImpaladMetrics::IO_MGR_S3_NATIVE_GET_TIME_US->Increment(
        get_timer.ElapsedTime() / 1000);
    if (num_parts > 1) ImpaladMetrics::IO_MGR_S3_NATIVE_PARALLEL_READ_COUNT->Increment(1);
  }
  RETURN_IF_ERROR(status);
  *bytes_read = S3PartsBytesRead(len, part_size, received);
  if (ImpaladMetrics::IO_MGR_S3_NATIVE_GET_BYTES != nullptr) {
    ImpaladMetrics::IO_MGR_S3_NATIVE_GET_BYTES->Increment(*bytes_read);
  }
  return Status::OK();
#else
  return Status("Native S3 reader is not available: impalad was built without "
      "aws-sdk-cpp");
#endif
}

Status S3NativeFileReader::GetSegments(int64_t offset,
    const S3NativeFileCache::Segments& segments, int64_t* bytes_read) {
  *bytes_read = 0;
#ifdef IMPALA_HAVE_AWS_SDK_CPP
  int64_t len = 0;
  for (const auto& segment : segments) len += segment.second;
  DCHECK_GT(len, 0);
  // The SDK creates a new stream for every attempt, so a retry writes from the start of
  // the first segment again.
  MonotonicStopWatch get_timer;
  get_timer.Start();
  Aws::S3::Model::GetObjectOutcome outcome = GetS3NativeClient()->GetObject(
      MakeGetRequest(bucket_, key_, offset, len, [segments]() {
        return Aws::New<SegmentIOStream>(kAllocationTag, segments);
      }));
  Status status = CheckGetOutcome(outcome, len, bytes_read);
  ++gets_sent_;
  if (ImpaladMetrics::IO_MGR_S3_NATIVE_GET_COUNT != nullptr) {
    ImpaladMetrics::IO_MGR_S3_NATIVE_GET_COUNT->Increment(1);
    ImpaladMetrics::IO_MGR_S3_NATIVE_GET_TIME_US->Increment(
        get_timer.ElapsedTime() / 1000);
  }
  if (!status.ok()) {
    return Status(TErrorCode::DISK_IO_ERROR, GetBackendString(),
        Substitute("Error reading S3 object s3a://$0/$1 at offset $2 ($3 bytes): $4",
            bucket_, key_, offset, len, status.msg().msg()));
  }
  if (ImpaladMetrics::IO_MGR_S3_NATIVE_GET_BYTES != nullptr) {
    ImpaladMetrics::IO_MGR_S3_NATIVE_GET_BYTES->Increment(*bytes_read);
  }
  return Status::OK();
#else
  return Status("Native S3 reader is not available: impalad was built without "
      "aws-sdk-cpp");
#endif
}

Status S3NativeFileReader::FetchSegments(const string& file_key, int64_t offset,
    const S3NativeFileCache::Segments& segments, int64_t* bytes_read) {
  DataCache* data_cache = native_data_cache;
  if (data_cache == nullptr || !scan_range_->UseDataCache()) {
    return GetSegments(offset, segments, bytes_read);
  }
  *bytes_read = 0;
  const size_t n = segments.size();
  vector<bool> hit(n, false);
  int64_t hit_blocks = 0, hit_bytes = 0, missed_blocks = 0, missed_bytes = 0;
  int64_t segment_offset = offset;
  for (size_t i = 0; i < n; ++i) {
    // A shorter entry (end of the object) counts as a miss: the block is fetched again.
    int64_t len = segments[i].second;
    hit[i] = data_cache->Lookup(file_key, 0, segment_offset, len, segments[i].first)
        == len;
    if (hit[i]) {
      ++hit_blocks;
      hit_bytes += len;
    }
    segment_offset += len;
  }
  // Each run of the other blocks with one GET; a short GET is the end of the object.
  int64_t pos = offset;
  for (size_t i = 0; i < n;) {
    if (hit[i]) {
      *bytes_read += segments[i].second;
      pos += segments[i].second;
      ++i;
      continue;
    }
    S3NativeFileCache::Segments run;
    int64_t run_len = 0;
    size_t j = i;
    for (; j < n && !hit[j]; ++j) {
      run.push_back(segments[j]);
      run_len += segments[j].second;
    }
    int64_t received = 0;
    RETURN_IF_ERROR(GetSegments(pos, run, &received));
    *bytes_read += received;
    missed_blocks += j - i;
    missed_bytes += run_len;
    if (FLAGS_s3_native_data_cache_write_on_fetch) {
      // Opportunistic, like the S3A reader: the data cache copies the data and drops the
      // write when its writers are busy. A short last block (end of the object) is stored
      // with the bytes received; FetchSegments() treats a shorter entry as a miss.
      int64_t block_pos = pos;
      int64_t left = received;
      for (const auto& segment : run) {
        int64_t len = std::min(segment.second, left);
        if (len <= 0) break;
        if (data_cache->Store(file_key, 0, block_pos, segment.first, len)
            && ImpaladMetrics::IO_MGR_S3_NATIVE_DATA_CACHE_OFFERED_BYTES != nullptr) {
          ImpaladMetrics::IO_MGR_S3_NATIVE_DATA_CACHE_OFFERED_BYTES->Increment(len);
        }
        block_pos += segment.second;
        left -= len;
      }
    }
    if (received < run_len) break;
    pos += run_len;
    i = j;
  }
  RequestContext* reader = scan_range_->reader_;
  if (hit_bytes > 0) {
    if (reader->data_cache_hit_bytes_counter_ != nullptr) {
      reader->data_cache_hit_bytes_counter_->Add(hit_bytes);
      reader->data_cache_hit_counter_->Add(hit_blocks);
    }
    if (ImpaladMetrics::IO_MGR_REMOTE_DATA_CACHE_HIT_BYTES != nullptr) {
      ImpaladMetrics::IO_MGR_REMOTE_DATA_CACHE_HIT_BYTES->Increment(hit_bytes);
      ImpaladMetrics::IO_MGR_REMOTE_DATA_CACHE_HIT_COUNT->Increment(hit_blocks);
    }
  }
  if (missed_bytes > 0) {
    if (reader->data_cache_miss_bytes_counter_ != nullptr) {
      reader->data_cache_miss_bytes_counter_->Add(missed_bytes);
      reader->data_cache_miss_counter_->Add(missed_blocks);
    }
    if (ImpaladMetrics::IO_MGR_REMOTE_DATA_CACHE_MISS_BYTES != nullptr) {
      ImpaladMetrics::IO_MGR_REMOTE_DATA_CACHE_MISS_BYTES->Increment(missed_bytes);
      ImpaladMetrics::IO_MGR_REMOTE_DATA_CACHE_MISS_COUNT->Increment(missed_blocks);
    }
  }
  return Status::OK();
}

Status S3NativeFileReader::ReadThroughDataCache(int64_t offset, uint8_t* buffer,
    int64_t len, int64_t* bytes_read) {
  DataCache* data_cache = native_data_cache;
  const string& filename = *scan_range_->file_string();
  int64_t mtime = scan_range_->mtime();
  RequestContext* reader = scan_range_->reader_;
  int64_t cached = data_cache->Lookup(filename, mtime, offset, len, buffer);
  DCHECK_GE(cached, 0);
  if (cached > 0) {
    if (reader->data_cache_hit_bytes_counter_ != nullptr) {
      reader->data_cache_hit_bytes_counter_->Add(cached);
      if (cached == len) {
        reader->data_cache_hit_counter_->Add(1);
      } else {
        reader->data_cache_partial_hit_counter_->Add(1);
      }
    }
    if (ImpaladMetrics::IO_MGR_REMOTE_DATA_CACHE_HIT_BYTES != nullptr) {
      ImpaladMetrics::IO_MGR_REMOTE_DATA_CACHE_HIT_BYTES->Increment(cached);
      ImpaladMetrics::IO_MGR_REMOTE_DATA_CACHE_HIT_COUNT->Increment(1);
    }
  }
  *bytes_read = cached;
  if (cached == len) return Status::OK();
  int64_t received = 0;
  RETURN_IF_ERROR(GetRange(offset + cached, buffer + cached, len - cached, &received));
  *bytes_read += received;
  if (received <= 0) return Status::OK();
  // Opportunistic, like the S3A reader: the whole read, also the part found on disk (a
  // larger entry replaces a shorter one); dropped when the data cache's writers are busy.
  if (data_cache->Store(filename, mtime, offset, buffer, *bytes_read)
      && ImpaladMetrics::IO_MGR_S3_NATIVE_DATA_CACHE_OFFERED_BYTES != nullptr) {
    ImpaladMetrics::IO_MGR_S3_NATIVE_DATA_CACHE_OFFERED_BYTES->Increment(*bytes_read);
  }
  if (reader->data_cache_miss_bytes_counter_ != nullptr) {
    reader->data_cache_miss_bytes_counter_->Add(received);
    reader->data_cache_miss_counter_->Add(1);
  }
  if (ImpaladMetrics::IO_MGR_REMOTE_DATA_CACHE_MISS_BYTES != nullptr) {
    ImpaladMetrics::IO_MGR_REMOTE_DATA_CACHE_MISS_BYTES->Increment(received);
    ImpaladMetrics::IO_MGR_REMOTE_DATA_CACHE_MISS_COUNT->Increment(1);
  }
  return Status::OK();
}

Status S3NativeFileReader::ReadFromCachedFile(int64_t file_offset, uint8_t* buffer,
    int64_t bytes_to_read, int64_t file_length, int64_t* bytes_read) {
  *bytes_read = 0;
  // The mtime is part of the key, so a rewritten file is never served from the cache.
  string cache_key = Substitute("$0/$1@$2", bucket_, key_, scan_range_->mtime());
  shared_ptr<const S3NativeFileCache::File> file;
  bool hit;
  RETURN_IF_ERROR(GetS3NativeFileCache()->Lookup(cache_key, file_length,
      scan_range_->reader_, [this](S3NativeFileCache::File* f) {
        // With the remote data cache the whole file is looked up on local disk first and
        // stored there after it is fetched, like any read outside the file cache.
        int64_t loaded;
        if (native_data_cache != nullptr && scan_range_->UseDataCache()) {
          RETURN_IF_ERROR(ReadThroughDataCache(0, f->data.get(), f->len, &loaded));
        } else {
          RETURN_IF_ERROR(GetRange(0, f->data.get(), f->len, &loaded));
        }
        f->len = loaded;
        return Status::OK();
      }, &file, &hit));
  if (file_offset < file->len) {
    *bytes_read = min(bytes_to_read, file->len - file_offset);
    memcpy(buffer, file->data.get() + file_offset, *bytes_read);
  }
  if (hit) {
    if (ImpaladMetrics::IO_MGR_S3_NATIVE_WHOLE_FILE_CACHE_HIT_COUNT != nullptr) {
      ImpaladMetrics::IO_MGR_S3_NATIVE_WHOLE_FILE_CACHE_HIT_COUNT->Increment(1);
      ImpaladMetrics::IO_MGR_S3_NATIVE_WHOLE_FILE_CACHE_HIT_BYTES->Increment(*bytes_read);
    }
  } else if (ImpaladMetrics::IO_MGR_S3_NATIVE_WHOLE_FILE_CACHE_MISS_COUNT != nullptr) {
    ImpaladMetrics::IO_MGR_S3_NATIVE_WHOLE_FILE_CACHE_MISS_COUNT->Increment(1);
  }
  return Status::OK();
}

Status S3NativeFileReader::ReadFromCachedBlocks(int64_t file_offset, uint8_t* buffer,
    int64_t bytes_to_read, int64_t file_length, int64_t* bytes_read) {
  // The mtime is part of the key, so a rewritten file is never served from the cache.
  string file_key = Substitute("$0/$1@$2", bucket_, key_, scan_range_->mtime());
  S3NativeFileCache::BlockStats stats;
  RETURN_IF_ERROR(GetS3NativeFileCache()->ReadBlocks(file_key, file_length,
      FLAGS_s3_native_block_size, file_offset, buffer, bytes_to_read,
      scan_range_->reader_,
      [this, &file_key](int64_t offset, const S3NativeFileCache::Segments& segments,
          int64_t* received) {
        return FetchSegments(file_key, offset, segments, received);
      }, bytes_read, &stats));
  if (ImpaladMetrics::IO_MGR_S3_NATIVE_BLOCK_CACHE_HIT_COUNT != nullptr) {
    ImpaladMetrics::IO_MGR_S3_NATIVE_BLOCK_CACHE_HIT_COUNT->Increment(stats.hit_blocks);
    ImpaladMetrics::IO_MGR_S3_NATIVE_BLOCK_CACHE_HIT_BYTES->Increment(stats.hit_bytes);
    ImpaladMetrics::IO_MGR_S3_NATIVE_BLOCK_CACHE_MISS_COUNT->Increment(
        stats.missed_blocks);
  }
  return Status::OK();
}

void S3NativeFileReader::CachedFile(uint8_t** data, int64_t* length) {
  *data = nullptr;
  *length = 0;
}

string S3NativeFileReader::DebugString() const {
  return Substitute("S3NativeFileReader bucket=$0 key=$1", bucket_, key_);
}

} // namespace io
} // namespace impala
