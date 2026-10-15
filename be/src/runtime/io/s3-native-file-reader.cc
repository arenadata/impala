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

using strings::Substitute;

namespace impala {
namespace io {

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

/// Ranged GET of 'len' bytes at 'offset' whose body is written into 'buffer'.
Aws::S3::Model::GetObjectRequest MakeGetRequest(const string& bucket, const string& key,
    int64_t offset, uint8_t* buffer, int64_t len) {
  Aws::S3::Model::GetObjectRequest request;
  request.SetBucket(bucket.c_str());
  request.SetKey(key.c_str());
  request.SetRange(Substitute("bytes=$0-$1", offset, offset + len - 1).c_str());
  request.SetResponseStreamFactory([buffer, len]() {
    return Aws::New<BufferIOStream>(kAllocationTag, buffer, len);
  });
  return request;
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
  bool cache = GetS3NativeFileCache() != nullptr && file_length > 0;
  if (cache && file_length <= FLAGS_s3_native_whole_file_max_size) {
    RETURN_IF_ERROR(
        ReadFromCachedFile(file_offset, buffer, bytes_to_read, file_length, bytes_read));
  } else if (cache && FLAGS_s3_native_block_size > 0) {
    RETURN_IF_ERROR(ReadFromCachedBlocks(
        file_offset, buffer, bytes_to_read, file_length, bytes_read));
  } else {
    RETURN_IF_ERROR(GetRange(file_offset, buffer, bytes_to_read, bytes_read));
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
  if (ImpaladMetrics::IO_MGR_S3_NATIVE_GET_COUNT != nullptr) {
    ImpaladMetrics::IO_MGR_S3_NATIVE_GET_COUNT->Increment(num_parts);
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

Status S3NativeFileReader::ReadFromCachedFile(int64_t file_offset, uint8_t* buffer,
    int64_t bytes_to_read, int64_t file_length, int64_t* bytes_read) {
  *bytes_read = 0;
  // The mtime is part of the key, so a rewritten file is never served from the cache.
  string cache_key = Substitute("$0/$1@$2", bucket_, key_, scan_range_->mtime());
  shared_ptr<const S3NativeFileCache::File> file;
  bool hit;
  RETURN_IF_ERROR(GetS3NativeFileCache()->Lookup(cache_key, file_length,
      scan_range_->reader_, [this](S3NativeFileCache::File* f) {
        int64_t loaded;
        RETURN_IF_ERROR(GetRange(0, f->data.get(), f->len, &loaded));
        f->len = loaded;
        return Status::OK();
      }, &file, &hit));
  if (file_offset < file->len) {
    *bytes_read = min(bytes_to_read, file->len - file_offset);
    memcpy(buffer, file->data.get() + file_offset, *bytes_read);
  }
  if (hit) {
    if (ImpaladMetrics::IO_MGR_S3_NATIVE_FILE_CACHE_HIT_COUNT != nullptr) {
      ImpaladMetrics::IO_MGR_S3_NATIVE_FILE_CACHE_HIT_COUNT->Increment(1);
      ImpaladMetrics::IO_MGR_S3_NATIVE_FILE_CACHE_HIT_BYTES->Increment(*bytes_read);
    }
  } else if (ImpaladMetrics::IO_MGR_S3_NATIVE_FILE_CACHE_MISS_COUNT != nullptr) {
    ImpaladMetrics::IO_MGR_S3_NATIVE_FILE_CACHE_MISS_COUNT->Increment(1);
  }
  return Status::OK();
}

Status S3NativeFileReader::ReadFromCachedBlocks(int64_t file_offset, uint8_t* buffer,
    int64_t bytes_to_read, int64_t file_length, int64_t* bytes_read) {
  // The mtime is part of the key, so a rewritten file is never served from the cache.
  string key_prefix = Substitute("$0/$1@$2#", bucket_, key_, scan_range_->mtime());
  S3NativeFileCache::BlockStats stats;
  RETURN_IF_ERROR(GetS3NativeFileCache()->ReadBlocks(key_prefix, file_length,
      FLAGS_s3_native_block_size, file_offset, buffer, bytes_to_read,
      scan_range_->reader_,
      [this](int64_t offset, uint8_t* run, int64_t len, int64_t* received) {
        return GetRange(offset, run, len, received);
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
