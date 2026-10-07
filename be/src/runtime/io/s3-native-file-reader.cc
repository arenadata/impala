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

#ifdef IMPALA_HAVE_AWS_SDK_CPP
#include <aws/core/http/HttpResponse.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#endif

#include "gutil/strings/substitute.h"
#include "runtime/io/disk-io-mgr-internal.h"
#include "runtime/io/request-context.h"
#include "runtime/io/request-ranges.h"
#include "runtime/io/s3-native-client.h"
#include "util/debug-util.h"
#include "util/histogram-metric.h"

#include "common/names.h"

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

  Aws::S3::Model::GetObjectRequest request;
  request.SetBucket(bucket_.c_str());
  request.SetKey(key_.c_str());
  request.SetRange(
      Substitute("bytes=$0-$1", file_offset, file_offset + bytes_to_read - 1).c_str());
  request.SetResponseStreamFactory([buffer, bytes_to_read]() {
    return Aws::New<BufferIOStream>(kAllocationTag, buffer, bytes_to_read);
  });

  Aws::S3::Model::GetObjectOutcome outcome = GetS3NativeClient()->GetObject(request);
  if (!outcome.IsSuccess()) {
    const auto& error = outcome.GetError();
    if (error.GetResponseCode()
        == Aws::Http::HttpResponseCode::REQUESTED_RANGE_NOT_SATISFIABLE) {
      // 'file_offset' is at or past the end of the object.
      *eof = true;
      return Status::OK();
    }
    return Status(TErrorCode::DISK_IO_ERROR, GetBackendString(),
        Substitute("Error reading S3 object s3a://$0/$1 at offset $2 ($3 bytes): "
            "HTTP $4 $5: $6", bucket_, key_, file_offset, bytes_to_read,
            static_cast<int>(error.GetResponseCode()),
            string(error.GetExceptionName().c_str()),
            string(error.GetMessage().c_str())));
  }

  int64_t content_length = outcome.GetResult().GetContentLength();
  DCHECK_GE(content_length, 0);
  DCHECK_LE(content_length, bytes_to_read);
  *bytes_read = content_length;
  // A shorter response means the range went past the end of the object.
  if (*bytes_read < bytes_to_read) *eof = true;
  queue->read_size()->Update(*bytes_read);
  return Status::OK();
#else
  return Status("Native S3 reader is not available: impalad was built without "
      "aws-sdk-cpp");
#endif
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
