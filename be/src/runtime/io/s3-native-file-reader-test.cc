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

// Tests of S3NativeFileReader against a mock aws-sdk-cpp S3 client that serves objects
// from memory: ranged GETs, short reads and errors, parallel parts, and reads through the
// native S3 file cache (whole files and blocks) and the remote data cache.

#ifdef IMPALA_HAVE_AWS_SDK_CPP

#include <algorithm>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectResult.h>
#include <boost/filesystem.hpp>
#include <gflags/gflags.h>

#include "common/object-pool.h"
#include "gutil/strings/substitute.h"
#include "runtime/io/data-cache.h"
#include "runtime/io/disk-io-mgr-internal.h"
#include "runtime/io/disk-io-mgr.h"
#include "runtime/io/request-context.h"
#include "runtime/io/request-ranges.h"
#include "runtime/io/s3-native-client.h"
#include "runtime/io/s3-native-file-cache.h"
#include "runtime/io/s3-native-file-reader.h"
#include "testutil/gtest-util.h"
#include "util/filesystem-util.h"
#include "util/histogram-metric.h"
#include "util/impalad-metrics.h"
#include "util/metrics.h"

#include "common/names.h"

DECLARE_int64(s3_native_parallel_read_min_size);
DECLARE_int64(s3_native_parallel_read_part_size);
DECLARE_int64(s3_native_whole_file_max_size);
DECLARE_int64(s3_native_block_size);
DECLARE_bool(s3_native_data_cache_write_on_fetch);

using strings::Substitute;

namespace impala {
namespace io {

namespace {

const char* const kTag = "S3NativeFileReaderTest";
const string kBucket = "bucket";
const int64_t kMtime = 1000;

// Byte at 'pos' of the test objects.
uint8_t ObjectByte(int64_t pos) { return static_cast<uint8_t>(pos % 251); }

string MakeObject(int64_t len) {
  string data(len, '\0');
  for (int64_t i = 0; i < len; ++i) data[i] = static_cast<char>(ObjectByte(i));
  return data;
}

// Parses "bytes=<first>-<last>".
bool ParseRange(const string& range, int64_t* first, int64_t* last) {
  return sscanf(range.c_str(), "bytes=%ld-%ld", first, last) == 2;
}

Aws::S3::Model::GetObjectOutcome ErrorOutcome(Aws::Http::HttpResponseCode code,
    Aws::S3::S3Errors type, const char* name) {
  Aws::S3::S3Error error(Aws::Client::AWSError<Aws::S3::S3Errors>(
      type, name, "injected by the test", false));
  error.SetResponseCode(code);
  return Aws::S3::Model::GetObjectOutcome(error);
}

} // anonymous namespace

/// S3 client that serves objects of bucket 'kBucket' from memory and records the Range
/// header of every GET. GetObjectCallable() of the SDK runs the overridden GetObject() on
/// the client's executor, like for a real client.
class MockS3Client : public Aws::S3::S3Client {
 public:
  explicit MockS3Client(const Aws::S3::S3ClientConfiguration& config)
    : Aws::S3::S3Client(Aws::MakeShared<Aws::Auth::SimpleAWSCredentialsProvider>(
          kTag, "test-access-key", "test-secret-key"), nullptr, config) {}

  void PutObject(const string& key, string data) {
    lock_guard<mutex> l(lock_);
    objects_[key] = move(data);
  }

  /// GETs at 'offset' fail with HTTP 500.
  void FailAt(int64_t offset) {
    lock_guard<mutex> l(lock_);
    fail_offsets_.push_back(offset);
  }

  /// The server ignores the Range header: 200 with the whole object, no Content-Range.
  void IgnoreRange() {
    lock_guard<mutex> l(lock_);
    ignore_range_ = true;
  }

  /// Range headers of the GETs so far, sorted (parallel parts come in any order).
  vector<string> Requests() const {
    lock_guard<mutex> l(lock_);
    vector<string> requests = requests_;
    sort(requests.begin(), requests.end());
    return requests;
  }

  int NumRequests() const {
    lock_guard<mutex> l(lock_);
    return requests_.size();
  }

  void ClearRequests() {
    lock_guard<mutex> l(lock_);
    requests_.clear();
  }

  Aws::S3::Model::GetObjectOutcome GetObject(
      const Aws::S3::Model::GetObjectRequest& request) const override {
    string range(request.GetRange().c_str());
    string data;
    bool ignore_range;
    bool fail = false;
    int64_t first = 0, last = 0;
    bool parsed = ParseRange(range, &first, &last);
    {
      lock_guard<mutex> l(lock_);
      requests_.push_back(range);
      if (string(request.GetBucket().c_str()) != kBucket) {
        return ErrorOutcome(Aws::Http::HttpResponseCode::NOT_FOUND,
            Aws::S3::S3Errors::NO_SUCH_BUCKET, "NoSuchBucket");
      }
      auto it = objects_.find(request.GetKey().c_str());
      if (it == objects_.end()) {
        return ErrorOutcome(Aws::Http::HttpResponseCode::NOT_FOUND,
            Aws::S3::S3Errors::NO_SUCH_KEY, "NoSuchKey");
      }
      data = it->second;
      ignore_range = ignore_range_;
      fail = parsed
          && find(fail_offsets_.begin(), fail_offsets_.end(), first) != fail_offsets_.end();
    }
    if (fail) {
      return ErrorOutcome(Aws::Http::HttpResponseCode::INTERNAL_SERVER_ERROR,
          Aws::S3::S3Errors::INTERNAL_FAILURE, "InternalError");
    }
    int64_t size = data.size();
    Aws::S3::Model::GetObjectResult result;
    if (ignore_range || !parsed) {
      first = 0;
      last = size - 1;
    } else {
      if (first >= size) {
        return ErrorOutcome(Aws::Http::HttpResponseCode::REQUESTED_RANGE_NOT_SATISFIABLE,
            Aws::S3::S3Errors::UNKNOWN, "InvalidRange");
      }
      last = min(last, size - 1);
      result.SetContentRange(Substitute("bytes $0-$1/$2", first, last, size).c_str());
    }
    int64_t len = last - first + 1;
    // The response stream of the request (the caller's buffer or cache blocks); the
    // result owns it, like a real response.
    Aws::IOStream* body = request.GetResponseStreamFactory()();
    body->write(data.data() + first, len);
    result.ReplaceBody(body);
    result.SetContentLength(len);
    return Aws::S3::Model::GetObjectOutcome(move(result));
  }

 private:
  mutable mutex lock_;
  map<string, string> objects_;
  vector<int64_t> fail_offsets_;
  bool ignore_range_ = false;
  mutable vector<string> requests_;
};

class S3NativeFileReaderTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    // The remote data cache registers its metrics in the Impala metrics, which only
    // TestEnv and ExecEnv create; kept for the lifetime of the process like TestEnv's.
    if (ImpaladMetrics::IO_MGR_METRICS == nullptr) {
      static MetricGroup* metrics = new MetricGroup("s3-native-file-reader-test");
      ImpaladMetrics::CreateMetrics(metrics);
    }
    sdk_options_ = new Aws::SDKOptions();
    Aws::InitAPI(*sdk_options_);
  }

  static void TearDownTestCase() {
    Aws::ShutdownAPI(*sdk_options_);
    delete sdk_options_;
    sdk_options_ = nullptr;
  }

  void SetUp() override {
    // No parallel parts, no file cache unless a test sets them up.
    FLAGS_s3_native_parallel_read_min_size = 0;
    FLAGS_s3_native_whole_file_max_size = 0;
    FLAGS_s3_native_block_size = 0;
    FLAGS_s3_native_data_cache_write_on_fetch = true;
    Aws::S3::S3ClientConfiguration config;
    config.region = "us-east-1";
    config.endpointOverride = "http://localhost:1";
    config.executor =
        Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(kTag, 4);
    client_ = Aws::MakeShared<MockS3Client>(kTag, config);
    client_->PutObject("dir/file.parq", MakeObject(1000));
    SetS3NativeClientForTesting(client_);
    old_file_cache_ = SwapS3NativeFileCacheForTesting(nullptr);
    io_mgr_.reset(new DiskIoMgr());
    context_ = io_mgr_->RegisterContext();
    queue_.reset(new DiskQueue(0));
    read_latency_.reset(new HistogramMetric(
        MakeTMetricDef("s3-native-test.read-latency", TMetricKind::HISTOGRAM,
            TUnit::TIME_NS), 60L * 1000 * 1000 * 1000, 3));
    read_size_.reset(new HistogramMetric(
        MakeTMetricDef("s3-native-test.read-size", TMetricKind::HISTOGRAM, TUnit::BYTES),
        1L << 30, 3));
    queue_->set_read_latency(read_latency_.get());
    queue_->set_read_size(read_size_.get());
  }

  void TearDown() override {
    SetS3NativeDataCacheForTesting(nullptr);
    SwapS3NativeFileCacheForTesting(move(old_file_cache_));
    SetS3NativeClientForTesting(nullptr);
    client_.reset();
    io_mgr_->UnregisterContext(context_.get());
    context_.reset();
    queue_.reset();
    io_mgr_.reset();
    if (data_cache_ != nullptr) {
      data_cache_->ReleaseResources();
      data_cache_.reset();
      boost::filesystem::remove_all(data_cache_dir_);
    }
  }

  /// Makes 'cache' the process-wide native S3 file cache for this test.
  void UseFileCache(unique_ptr<S3NativeFileCache> cache) {
    file_cache_ = cache.get();
    SwapS3NativeFileCacheForTesting(move(cache));
  }

  /// Sets up a remote data cache on local disk for the reader. Returns false if the file
  /// system of /tmp cannot hold it (KUDU-1508); the test then ends.
  bool UseDataCache() {
    data_cache_dir_ = ("/tmp" / boost::filesystem::unique_path()).string();
    if (!FileSystemUtil::RemoveAndCreateDirectory(data_cache_dir_).ok()
        || !FileSystemUtil::CheckForBuggyExtFS(data_cache_dir_).ok()) {
      LOG(WARNING) << "Skipping the remote data cache test (KUDU-1508)";
      return false;
    }
    data_cache_.reset(new DataCache(Substitute("$0:$1", data_cache_dir_, 4L << 20)));
    Status status = data_cache_->Init();
    EXPECT_OK(status);
    if (!status.ok()) return false;
    SetS3NativeDataCacheForTesting(data_cache_.get());
    return true;
  }

  /// A scan range over the whole file 'key' of 'file_length' bytes (-1: unknown).
  ScanRange* MakeRange(const string& key, int64_t file_length, int64_t mtime = kMtime,
      int cache_options = BufferOpts::NO_CACHING) {
    return MakeRangeForPath(Substitute("s3a://$0/$1", kBucket, key), file_length, mtime,
        cache_options);
  }

  /// Like MakeRange(), for the file 'path'.
  ScanRange* MakeRangeForPath(const string& path, int64_t file_length,
      int64_t mtime = kMtime, int cache_options = BufferOpts::NO_CACHING) {
    paths_.push_back(path);
    ScanRange::FileInfo info{paths_.back().c_str()};
    info.mtime = mtime;
    info.file_length = file_length;
    ScanRange* range = pool_.Add(new ScanRange);
    range->Reset(info, max<int64_t>(file_length, 0), 0, 0, false,
        BufferOpts(cache_options));
    range->reader_ = context_.get();
    return range;
  }

  /// Reads 'len' bytes at 'offset' of 'range' with a new S3NativeFileReader, like an I/O
  /// thread of DiskIoMgr does.
  Status Read(ScanRange* range, int64_t offset, int64_t len, vector<uint8_t>* buffer,
      int64_t* bytes_read, bool* eof) {
    buffer->assign(len, 0);
    S3NativeFileReader reader(range);
    RETURN_IF_ERROR(reader.Open());
    range->read_in_flight_ = true;
    Status status =
        reader.ReadFromPos(queue_.get(), offset, buffer->data(), len, bytes_read, eof);
    range->read_in_flight_ = false;
    reader.Close();
    return status;
  }

  /// Expects 'n' bytes of 'buffer' to be the object bytes from 'offset'.
  static void ExpectObjectBytes(
      const vector<uint8_t>& buffer, int64_t offset, int64_t n) {
    ASSERT_GE(buffer.size(), n);
    for (int64_t i = 0; i < n; ++i) {
      ASSERT_EQ(ObjectByte(offset + i), buffer[i]) << "at " << offset + i;
    }
  }

  static Aws::SDKOptions* sdk_options_;
  shared_ptr<MockS3Client> client_;
  unique_ptr<S3NativeFileCache> old_file_cache_;
  S3NativeFileCache* file_cache_ = nullptr;
  unique_ptr<DiskIoMgr> io_mgr_;
  unique_ptr<RequestContext> context_;
  unique_ptr<DiskQueue> queue_;
  unique_ptr<HistogramMetric> read_latency_;
  unique_ptr<HistogramMetric> read_size_;
  unique_ptr<DataCache> data_cache_;
  string data_cache_dir_;
  ObjectPool pool_;
  vector<string> paths_;
};

Aws::SDKOptions* S3NativeFileReaderTest::sdk_options_ = nullptr;

// One ranged GET whose body lands in the caller's buffer; Open() sends nothing.
TEST_F(S3NativeFileReaderTest, ReadsRange) {
  ScanRange* range = MakeRange("dir/file.parq", 1000);
  vector<uint8_t> buffer;
  int64_t bytes_read = -1;
  bool eof = true;
  ASSERT_OK(Read(range, 100, 200, &buffer, &bytes_read, &eof));
  EXPECT_EQ(200, bytes_read);
  EXPECT_FALSE(eof);
  ExpectObjectBytes(buffer, 100, 200);
  EXPECT_EQ(vector<string>({"bytes=100-299"}), client_->Requests());

  // A read without bytes sends no request.
  ASSERT_OK(Read(range, 0, 0, &buffer, &bytes_read, &eof));
  EXPECT_EQ(0, bytes_read);
  EXPECT_EQ(1, client_->NumRequests());
}

// A read past the end of the object is short and sets eof; a read that starts at the end
// (HTTP 416) reads nothing.
TEST_F(S3NativeFileReaderTest, ShortReadAtEndOfObject) {
  ScanRange* range = MakeRange("dir/file.parq", 1000);
  vector<uint8_t> buffer;
  int64_t bytes_read = -1;
  bool eof = false;
  ASSERT_OK(Read(range, 900, 200, &buffer, &bytes_read, &eof));
  EXPECT_EQ(100, bytes_read);
  EXPECT_TRUE(eof);
  ExpectObjectBytes(buffer, 900, 100);

  eof = false;
  ASSERT_OK(Read(range, 1000, 100, &buffer, &bytes_read, &eof));
  EXPECT_EQ(0, bytes_read);
  EXPECT_TRUE(eof);
}

// Errors of the server come back as DISK_IO_ERROR with the path, offset and HTTP code.
TEST_F(S3NativeFileReaderTest, ErrorsNameTheObject) {
  ScanRange* missing = MakeRange("dir/missing.parq", 1000);
  vector<uint8_t> buffer;
  int64_t bytes_read = -1;
  bool eof;
  Status status = Read(missing, 0, 100, &buffer, &bytes_read, &eof);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(TErrorCode::DISK_IO_ERROR, status.code());
  EXPECT_STR_CONTAINS(status.GetDetail(), "s3a://bucket/dir/missing.parq");
  EXPECT_STR_CONTAINS(status.GetDetail(), "HTTP 404 NoSuchKey");

  client_->FailAt(300);
  status = Read(MakeRange("dir/file.parq", 1000), 300, 100, &buffer, &bytes_read, &eof);
  ASSERT_FALSE(status.ok());
  EXPECT_STR_CONTAINS(status.GetDetail(), "at offset 300 (100 bytes)");
  EXPECT_STR_CONTAINS(status.GetDetail(), "HTTP 500 InternalError");

  // Not an s3a path with a bucket and a key.
  S3NativeFileReader reader(MakeRangeForPath("s3a://bucket-only", 1000));
  EXPECT_FALSE(reader.Open().ok());
}

// A server that ignores the Range header sends the object from its start: only a read
// at offset 0 may take that body.
TEST_F(S3NativeFileReaderTest, IgnoredRangeIsAnErrorAfterOffsetZero) {
  client_->IgnoreRange();
  ScanRange* range = MakeRange("dir/file.parq", 1000);
  vector<uint8_t> buffer;
  int64_t bytes_read = -1;
  bool eof;
  Status status = Read(range, 100, 100, &buffer, &bytes_read, &eof);
  ASSERT_FALSE(status.ok());
  EXPECT_STR_CONTAINS(status.GetDetail(), "s3a://bucket/dir/file.parq");

  // The whole object does not fit the buffer of a read at 0 either.
  EXPECT_FALSE(Read(range, 0, 100, &buffer, &bytes_read, &eof).ok());
  ASSERT_OK(Read(range, 0, 1000, &buffer, &bytes_read, &eof));
  EXPECT_EQ(1000, bytes_read);
  ExpectObjectBytes(buffer, 0, 1000);
}

// Large reads are split into parts that are fetched in parallel, each into its own
// section of the buffer.
TEST_F(S3NativeFileReaderTest, ParallelParts) {
  FLAGS_s3_native_parallel_read_min_size = 200;
  FLAGS_s3_native_parallel_read_part_size = 100;
  ScanRange* range = MakeRange("dir/file.parq", 1000);
  vector<uint8_t> buffer;
  int64_t bytes_read = -1;
  bool eof = true;
  ASSERT_OK(Read(range, 50, 350, &buffer, &bytes_read, &eof));
  EXPECT_EQ(350, bytes_read);
  EXPECT_FALSE(eof);
  ExpectObjectBytes(buffer, 50, 350);
  EXPECT_EQ(vector<string>({"bytes=150-249", "bytes=250-349", "bytes=350-399",
      "bytes=50-149"}), client_->Requests());

  // Smaller reads are one request.
  client_->ClearRequests();
  ASSERT_OK(Read(range, 0, 150, &buffer, &bytes_read, &eof));
  EXPECT_EQ(1, client_->NumRequests());

  // At the end of the object the parts after the first short one do not count.
  client_->ClearRequests();
  ASSERT_OK(Read(range, 850, 400, &buffer, &bytes_read, &eof));
  EXPECT_EQ(150, bytes_read);
  EXPECT_TRUE(eof);
  ExpectObjectBytes(buffer, 850, 150);
  EXPECT_EQ(4, client_->NumRequests());
}

// A failed part fails the read, after every part has finished (they write into the
// buffer).
TEST_F(S3NativeFileReaderTest, ParallelPartError) {
  FLAGS_s3_native_parallel_read_min_size = 200;
  FLAGS_s3_native_parallel_read_part_size = 100;
  client_->FailAt(200);
  ScanRange* range = MakeRange("dir/file.parq", 1000);
  vector<uint8_t> buffer;
  int64_t bytes_read = -1;
  bool eof;
  Status status = Read(range, 0, 400, &buffer, &bytes_read, &eof);
  ASSERT_FALSE(status.ok());
  EXPECT_STR_CONTAINS(status.GetDetail(), "at offset 200 (100 bytes)");
  EXPECT_EQ(4, client_->NumRequests());
}

// Small files are read whole into the file cache on their first read; later reads of
// the same file version are served from it.
TEST_F(S3NativeFileReaderTest, WholeFileCache) {
  FLAGS_s3_native_whole_file_max_size = 2000;
  UseFileCache(make_unique<S3NativeFileCache>(100000, 80));
  ScanRange* range = MakeRange("dir/file.parq", 1000);
  vector<uint8_t> buffer;
  int64_t bytes_read = -1;
  bool eof = true;
  ASSERT_OK(Read(range, 900, 100, &buffer, &bytes_read, &eof));
  EXPECT_EQ(100, bytes_read);
  EXPECT_FALSE(eof);
  ExpectObjectBytes(buffer, 900, 100);
  EXPECT_EQ(vector<string>({"bytes=0-999"}), client_->Requests());
  EXPECT_EQ(1000, file_cache_->TotalBytes());

  ASSERT_OK(Read(range, 10, 300, &buffer, &bytes_read, &eof));
  EXPECT_EQ(300, bytes_read);
  ExpectObjectBytes(buffer, 10, 300);
  // Past the end of the cached file.
  ASSERT_OK(Read(range, 950, 100, &buffer, &bytes_read, &eof));
  EXPECT_EQ(50, bytes_read);
  EXPECT_TRUE(eof);
  ExpectObjectBytes(buffer, 950, 50);
  EXPECT_EQ(1, client_->NumRequests());

  // Another mtime is another version of the file: read again.
  ASSERT_OK(Read(MakeRange("dir/file.parq", 1000, kMtime + 1), 0, 100, &buffer,
      &bytes_read, &eof));
  EXPECT_EQ(2, client_->NumRequests());
  // Files of unknown length are read without the cache.
  ASSERT_OK(Read(MakeRange("dir/file.parq", -1), 0, 100, &buffer, &bytes_read, &eof));
  EXPECT_EQ(3, client_->NumRequests());
  EXPECT_EQ(2000, file_cache_->TotalBytes());
}

// While the file cache takes no new entries (low headroom), a miss reads only the
// requested bytes instead of the whole file.
TEST_F(S3NativeFileReaderTest, WholeFileMissWhileHeadroomLow) {
  FLAGS_s3_native_whole_file_max_size = 2000;
  unique_ptr<S3NativeFileCache> cache = make_unique<S3NativeFileCache>(100000, 80, 1, 0);
  cache->SetHeadroomFunction([] () { return 0; }, 500);
  UseFileCache(move(cache));
  file_cache_->ReleaseMemory(1);
  ScanRange* range = MakeRange("dir/file.parq", 1000);
  vector<uint8_t> buffer;
  int64_t bytes_read = -1;
  bool eof = true;
  ASSERT_OK(Read(range, 200, 100, &buffer, &bytes_read, &eof));
  EXPECT_EQ(100, bytes_read);
  EXPECT_FALSE(eof);
  ExpectObjectBytes(buffer, 200, 100);
  EXPECT_EQ(vector<string>({"bytes=200-299"}), client_->Requests());
  EXPECT_EQ(0, file_cache_->TotalBytes());
  EXPECT_EQ(100, file_cache_->SkippedBytes());
}

// Larger files are cached in blocks: each run of missing blocks is one GET straight into
// the blocks, blocks already cached are copied.
TEST_F(S3NativeFileReaderTest, BlockCache) {
  FLAGS_s3_native_block_size = 256;
  UseFileCache(make_unique<S3NativeFileCache>(100000, 80));
  ScanRange* range = MakeRange("dir/file.parq", 1000);
  vector<uint8_t> buffer;
  int64_t bytes_read = -1;
  bool eof = true;
  ASSERT_OK(Read(range, 300, 100, &buffer, &bytes_read, &eof));
  EXPECT_EQ(100, bytes_read);
  ExpectObjectBytes(buffer, 300, 100);
  EXPECT_EQ(vector<string>({"bytes=256-511"}), client_->Requests());

  // Blocks 0 and 2 are missing, block 1 is cached.
  client_->ClearRequests();
  ASSERT_OK(Read(range, 200, 400, &buffer, &bytes_read, &eof));
  EXPECT_EQ(400, bytes_read);
  ExpectObjectBytes(buffer, 200, 400);
  EXPECT_EQ(vector<string>({"bytes=0-255", "bytes=512-767"}), client_->Requests());

  // The last block is shorter; reads past the end are short.
  client_->ClearRequests();
  ASSERT_OK(Read(range, 900, 200, &buffer, &bytes_read, &eof));
  EXPECT_EQ(100, bytes_read);
  EXPECT_TRUE(eof);
  ExpectObjectBytes(buffer, 900, 100);
  EXPECT_EQ(vector<string>({"bytes=768-999"}), client_->Requests());

  // Everything is cached now.
  client_->ClearRequests();
  ASSERT_OK(Read(range, 0, 1000, &buffer, &bytes_read, &eof));
  EXPECT_EQ(1000, bytes_read);
  ExpectObjectBytes(buffer, 0, 1000);
  EXPECT_EQ(0, client_->NumRequests());
  EXPECT_EQ(1000, file_cache_->TotalBytes());
}

// A failed block fetch fails the read and leaves nothing in the cache.
TEST_F(S3NativeFileReaderTest, BlockFetchError) {
  FLAGS_s3_native_block_size = 256;
  UseFileCache(make_unique<S3NativeFileCache>(100000, 80));
  client_->FailAt(256);
  ScanRange* range = MakeRange("dir/file.parq", 1000);
  vector<uint8_t> buffer;
  int64_t bytes_read = -1;
  bool eof;
  Status status = Read(range, 300, 100, &buffer, &bytes_read, &eof);
  ASSERT_FALSE(status.ok());
  EXPECT_STR_CONTAINS(status.GetDetail(), "s3a://bucket/dir/file.parq at offset 256");
  EXPECT_EQ(0, file_cache_->TotalBytes());
}

// With the remote data cache, fetched blocks are also stored on local disk, and blocks
// missing from the file cache are found there before they are fetched from S3.
TEST_F(S3NativeFileReaderTest, BlocksThroughDataCache) {
  if (!UseDataCache()) return;
  FLAGS_s3_native_block_size = 256;
  UseFileCache(make_unique<S3NativeFileCache>(100000, 80));
  ScanRange* range =
      MakeRange("dir/file.parq", 1000, kMtime, BufferOpts::USE_DATA_CACHE);
  vector<uint8_t> buffer;
  int64_t bytes_read = -1;
  bool eof;
  ASSERT_OK(Read(range, 0, 600, &buffer, &bytes_read, &eof));
  EXPECT_EQ(600, bytes_read);
  ExpectObjectBytes(buffer, 0, 600);
  EXPECT_EQ(vector<string>({"bytes=0-767"}), client_->Requests());

  // A new, empty file cache: blocks 0-2 come from the data cache, block 3 from S3.
  UseFileCache(make_unique<S3NativeFileCache>(100000, 80));
  client_->ClearRequests();
  ASSERT_OK(Read(range, 0, 1000, &buffer, &bytes_read, &eof));
  EXPECT_EQ(1000, bytes_read);
  ExpectObjectBytes(buffer, 0, 1000);
  EXPECT_EQ(vector<string>({"bytes=768-999"}), client_->Requests());

  // Ranges that do not use the data cache fetch from S3.
  UseFileCache(make_unique<S3NativeFileCache>(100000, 80));
  client_->ClearRequests();
  ASSERT_OK(Read(MakeRange("dir/file.parq", 1000), 0, 256, &buffer, &bytes_read, &eof));
  EXPECT_EQ(vector<string>({"bytes=0-255"}), client_->Requests());
}

// Without the file cache, reads go through the remote data cache like the S3A reader:
// looked up by offset, the rest fetched and the whole read stored.
TEST_F(S3NativeFileReaderTest, ReadThroughDataCacheWithoutFileCache) {
  if (!UseDataCache()) return;
  ScanRange* range =
      MakeRange("dir/file.parq", 1000, kMtime, BufferOpts::USE_DATA_CACHE);
  vector<uint8_t> buffer;
  int64_t bytes_read = -1;
  bool eof;
  ASSERT_OK(Read(range, 100, 300, &buffer, &bytes_read, &eof));
  EXPECT_EQ(300, bytes_read);
  ExpectObjectBytes(buffer, 100, 300);
  EXPECT_EQ(1, client_->NumRequests());

  ASSERT_OK(Read(range, 100, 300, &buffer, &bytes_read, &eof));
  EXPECT_EQ(300, bytes_read);
  ExpectObjectBytes(buffer, 100, 300);
  EXPECT_EQ(1, client_->NumRequests());

  // A longer read at the same offset fetches only the bytes after the cached part.
  client_->ClearRequests();
  ASSERT_OK(Read(range, 100, 500, &buffer, &bytes_read, &eof));
  EXPECT_EQ(500, bytes_read);
  ExpectObjectBytes(buffer, 100, 500);
  EXPECT_EQ(vector<string>({"bytes=400-599"}), client_->Requests());
}

} // namespace io
} // namespace impala

#endif // IMPALA_HAVE_AWS_SDK_CPP
