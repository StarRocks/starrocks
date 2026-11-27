// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "formats/file_input_stream.h"

#include <gtest/gtest.h>
#include <zlib.h>

#include <memory>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "cache/datacache.h"
#include "cache/disk_cache/block_cache.h"
#include "cache/disk_cache/test_cache_utils.h"
#include "cache/scan/cache_input_stream.h"
#include "cache/scan/cache_select_input_stream.hpp"
#include "cache/scan/shared_buffered_input_stream.h"
#include "formats/scan_context.h"
#include "fs/fs_memory.h"
#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"

namespace starrocks::formats {

class FileInputStreamTest : public testing::Test {
protected:
    void SetUp() override {
        _block_cache = std::make_shared<BlockCache>();
        ASSERT_OK(_block_cache->init(BlockCacheOptions{.block_size = 256 * 1024}, nullptr, nullptr));
        DataCache::GetInstance()->set_block_cache(_block_cache);
    }

    void TearDown() override {
        DataCache::GetInstance()->set_block_cache(nullptr);
        _block_cache.reset();
    }

    void write_file(const std::string& path, const std::string& contents) {
        ASSIGN_OR_ABORT(auto file, _fs.new_writable_file(path));
        ASSERT_OK(file->append(contents));
        ASSERT_OK(file->close());
    }

    static std::string gzip(const std::string& contents) {
        z_stream stream{};
        EXPECT_EQ(Z_OK,
                  deflateInit2(&stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED, MAX_WBITS + 16, 8, Z_DEFAULT_STRATEGY));

        std::string compressed(compressBound(contents.size()) + 32, '\0');
        stream.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(contents.data()));
        stream.avail_in = contents.size();
        stream.next_out = reinterpret_cast<Bytef*>(compressed.data());
        stream.avail_out = compressed.size();

        EXPECT_EQ(Z_STREAM_END, deflate(&stream, Z_FINISH));
        compressed.resize(stream.total_out);
        EXPECT_EQ(Z_OK, deflateEnd(&stream));
        return compressed;
    }

    MemoryFileSystem _fs;
    std::shared_ptr<BlockCache> _block_cache;
};

TEST_F(FileInputStreamTest, ReadsFileAndInfersSize) {
    const std::string path = "/input.txt";
    const std::string contents = "format input stream";
    write_file(path, contents);

    FormatScannerStats fs_stats;
    FormatScannerStats app_stats;
    FileInputStreamOptions options{.fs = &_fs, .file_path = path, .fs_stats = &fs_stats, .app_stats = &app_stats};
    std::shared_ptr<SharedBufferedInputStream> shared_buffered_input_stream;
    std::shared_ptr<CacheInputStream> cache_input_stream;

    ASSIGN_OR_ABORT(auto file, create_random_access_file(shared_buffered_input_stream, cache_input_stream, options));
    ASSERT_TRUE(file->get_size().ok());
    EXPECT_EQ(contents.size(), file->get_size().value());
    EXPECT_NE(nullptr, shared_buffered_input_stream);
    EXPECT_EQ(nullptr, cache_input_stream);

    std::string actual(contents.size(), '\0');
    ASSERT_OK(file->read_at_fully(0, actual.data(), actual.size()));
    EXPECT_EQ(contents, actual);
    EXPECT_EQ(contents.size(), fs_stats.bytes_read);
    EXPECT_EQ(1, fs_stats.io_count);
    EXPECT_EQ(contents.size(), app_stats.bytes_read);
    EXPECT_EQ(1, app_stats.io_count);
}

TEST_F(FileInputStreamTest, CreatesConfiguredCacheStream) {
    const std::string path = "/cached-input.txt";
    const std::string contents = "cached input";
    write_file(path, contents);

    FormatScannerStats fs_stats;
    FormatScannerStats app_stats;
    FileInputStreamOptions options{
            .fs = &_fs,
            .file_path = path,
            .file_size = static_cast<int64_t>(contents.size()),
            .fs_stats = &fs_stats,
            .app_stats = &app_stats,
            .datacache_options = DataCacheOptions{.enable_datacache = true, .enable_cache_select = true}};
    std::shared_ptr<SharedBufferedInputStream> shared_buffered_input_stream;
    std::shared_ptr<CacheInputStream> cache_input_stream;

    ASSIGN_OR_ABORT(auto file, create_random_access_file(shared_buffered_input_stream, cache_input_stream, options));
    EXPECT_NE(nullptr, file);
    EXPECT_NE(nullptr, shared_buffered_input_stream);
    EXPECT_NE(nullptr, cache_input_stream);
    EXPECT_NE(nullptr, dynamic_cast<CacheSelectInputStream*>(cache_input_stream.get()));
}

TEST_F(FileInputStreamTest, DecompressesInput) {
    const std::string path = "/input.txt.gz";
    const std::string contents = "compressed format input stream";
    const std::string compressed = gzip(contents);
    write_file(path, compressed);

    FormatScannerStats fs_stats;
    FormatScannerStats app_stats;
    FileInputStreamOptions options{.fs = &_fs,
                                   .file_path = path,
                                   .file_size = static_cast<int64_t>(compressed.size()),
                                   .fs_stats = &fs_stats,
                                   .app_stats = &app_stats,
                                   .compression_type = CompressionTypePB::GZIP};
    std::shared_ptr<SharedBufferedInputStream> shared_buffered_input_stream;
    std::shared_ptr<CacheInputStream> cache_input_stream;

    ASSIGN_OR_ABORT(auto file, create_random_access_file(shared_buffered_input_stream, cache_input_stream, options));
    std::string actual(contents.size(), '\0');
    ASSIGN_OR_ABORT(auto bytes_read, file->read(actual.data(), actual.size()));
    EXPECT_EQ(contents.size(), bytes_read);
    EXPECT_EQ(contents, actual);
}

#if defined(WITH_STARCACHE)
namespace {

MemTracker* g_peek_test_mem_tracker = nullptr;

bool peek_test_env_initialized() {
    return g_peek_test_mem_tracker != nullptr;
}

MemTracker* peek_test_mem_tracker() {
    return g_peek_test_mem_tracker;
}

// SharedBufferedInputStream::get_bytes() checks CurrentThread::mem_tracker() when it loads a shared buffer.
// format_test does not set up a process mem tracker, so install one for the scope of a test.
class ScopedTestMemTracker {
public:
    ScopedTestMemTracker() {
        g_peek_test_mem_tracker = &_mem_tracker;
        CurrentThread::set_mem_tracker_source(peek_test_env_initialized, peek_test_mem_tracker);
        tls_mem_tracker = nullptr;
    }

    ~ScopedTestMemTracker() {
        tls_thread_status.set_mem_tracker(nullptr);
        CurrentThread::set_mem_tracker_source(nullptr, nullptr);
        g_peek_test_mem_tracker = nullptr;
    }

private:
    MemTracker _mem_tracker;
};

} // namespace

// With datacache on and async populate off, peek() writes the peeked block to the datacache synchronously.
// That write must be counted in app io_ns, so it is not charged as scanner CPU time.
TEST_F(FileInputStreamTest, PeekSyncDatacachePopulateIsTimedAsIO) {
    ScopedTestMemTracker scoped_mem_tracker;
    const int64_t block_size = 256 * 1024;
    auto block_cache = TestCacheUtils::create_cache(TestCacheUtils::create_simple_options(block_size, 50 * MB));
    DataCache::GetInstance()->set_block_cache(block_cache);

    const std::string path = "/peek-sync-populate.dat";
    const std::string contents = std::string(block_size, 'a') + std::string(block_size, 'b');
    write_file(path, contents);

    FormatScannerStats fs_stats;
    FormatScannerStats app_stats;
    FileInputStreamOptions options{.fs = &_fs,
                                   .file_path = path,
                                   .file_size = static_cast<int64_t>(contents.size()),
                                   .fs_stats = &fs_stats,
                                   .app_stats = &app_stats,
                                   .datacache_options = DataCacheOptions{.enable_datacache = true,
                                                                         .enable_populate_datacache = true,
                                                                         .enable_datacache_async_populate_mode = false,
                                                                         .modification_time = 1}};
    std::shared_ptr<SharedBufferedInputStream> shared_buffered_input_stream;
    std::shared_ptr<CacheInputStream> cache_input_stream;
    ASSIGN_OR_ABORT(auto file, create_random_access_file(shared_buffered_input_stream, cache_input_stream, options));
    ASSERT_NE(nullptr, cache_input_stream);

    std::vector<SharedBufferedInputStream::IORange> ranges;
    ranges.emplace_back(0, static_cast<int64_t>(contents.size()));
    ASSERT_OK(shared_buffered_input_stream->set_io_ranges(ranges));

    // Cache miss on block 0: loads the shared buffer and populates block 0 inside the timed read path.
    std::string buffer(block_size, '\0');
    ASSERT_OK(file->read_at_fully(0, buffer.data(), block_size));
    ASSERT_EQ(std::string(block_size, 'a'), buffer);

    const int64_t app_io_ns_before = app_stats.io_ns;
    const int64_t write_count_before = cache_input_stream->stats().write_block_cache_count;
    const int64_t write_ns_before = cache_input_stream->stats().write_block_cache_ns;

    // Zero-copy peek of block 1 from the loaded shared buffer populates block 1 synchronously.
    ASSERT_OK(file->stream()->seek(block_size));
    ASSIGN_OR_ABORT(auto view, file->stream()->peek(block_size));
    ASSERT_EQ(std::string(block_size, 'b'), std::string(view));

    ASSERT_EQ(write_count_before + 1, cache_input_stream->stats().write_block_cache_count);
    const int64_t peek_write_ns = cache_input_stream->stats().write_block_cache_ns - write_ns_before;
    ASSERT_GT(peek_write_ns, 0);
    ASSERT_GE(app_stats.io_ns - app_io_ns_before, peek_write_ns);
}
#endif

} // namespace starrocks::formats
