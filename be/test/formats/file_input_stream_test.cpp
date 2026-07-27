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
#include "cache/scan/cache_input_stream.h"
#include "cache/scan/cache_select_input_stream.hpp"
#include "cache/scan/shared_buffered_input_stream.h"
#include "formats/scan_context.h"
#include "fs/fs_memory.h"

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

} // namespace starrocks::formats
