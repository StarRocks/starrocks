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

#include "cache/block_cache/block_cache.h"

#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>

#include "cache/block_cache/test_cache_utils.h"
#include "cache/datacache_utils.h"
#include "common/logging.h"
#include "common/statusor.h"
#include "fs/fs_util.h"
#include "testutil/assert.h"

namespace starrocks {

class BlockCacheTest : public ::testing::Test {
protected:
    static void SetUpTestCase() {}

    static void TearDownTestCase() {}

    void SetUp() override {
        _saved_enable_auto_adjust = config::enable_datacache_disk_auto_adjust;
        config::enable_datacache_disk_auto_adjust = false;
    }
    void TearDown() override { config::enable_datacache_disk_auto_adjust = _saved_enable_auto_adjust; }

    bool _saved_enable_auto_adjust = false;
};

TEST_F(BlockCacheTest, copy_to_iobuf) {
    // Create an iobuffer which contains 3 blocks
    const size_t buf_block_size = 100;
    void* data1 = malloc(buf_block_size);
    void* data2 = malloc(buf_block_size);
    void* data3 = malloc(buf_block_size);
    memset(data1, 1, buf_block_size);
    memset(data2, 2, buf_block_size);
    memset(data3, 3, buf_block_size);

    IOBuffer buffer;
    buffer.append_user_data(data1, buf_block_size, nullptr);
    buffer.append_user_data(data2, buf_block_size, nullptr);
    buffer.append_user_data(data3, buf_block_size, nullptr);

    // Copy the last 150 bytes of iobuffer to a target buffer
    const off_t offset = 150;
    const size_t size = 150;
    char result[size] = {0};
    buffer.copy_to(result, size, offset);

    // Check the target buffer content
    char expect[size] = {0};
    memset(expect, 2, 50);
    memset(expect + 50, 3, 100);
    ASSERT_EQ(memcmp(result, expect, size), 0);
}

TEST_F(BlockCacheTest, hybrid_cache) {
    const std::string cache_dir = "./block_disk_cache3";
    ASSERT_TRUE(fs::create_directories(cache_dir).ok());

    const size_t block_size = 256 * 1024;
    DiskCacheOptions options = TestCacheUtils::create_simple_options(block_size, 2 * MB);
    options.dir_spaces.push_back({.path = cache_dir, .size = 50 * MB});
    auto cache = TestCacheUtils::create_cache(options);

    const size_t batch_size = block_size;
    const size_t rounds = 10;
    const std::string cache_key = "test_file";

    // write cache
    for (size_t i = 0; i < rounds; ++i) {
        char ch = 'a' + i % 26;
        std::string value(batch_size, ch);
        Status st = cache->write(cache_key + std::to_string(i), 0, batch_size, value.c_str());
        ASSERT_TRUE(st.ok()) << st.message();
    }

    // read cache
    for (size_t i = 0; i < rounds; ++i) {
        char ch = 'a' + i % 26;
        std::string expect_value(batch_size, ch);
        char value[batch_size] = {0};
        auto res = cache->read(cache_key + std::to_string(i), 0, batch_size, value);
        ASSERT_TRUE(res.status().ok()) << res.status().message();
        ASSERT_EQ(memcmp(value, expect_value.c_str(), batch_size), 0);
    }

    // remove cache
    char value[1024] = {0};
    ASSERT_OK(cache->remove(cache_key + std::to_string(0), 0, batch_size));

    auto res = cache->read(cache_key + std::to_string(0), 0, batch_size, value);
    ASSERT_TRUE(res.status().is_not_found());

    // not found
    res = cache->read(cache_key, block_size * 1000, batch_size, value);
    ASSERT_TRUE(res.status().is_not_found());

    ASSERT_OK(cache->shutdown());
    ASSERT_OK(fs::remove_all(cache_dir));
}

TEST_F(BlockCacheTest, write_with_overwrite_option) {
    const size_t block_size = 1024 * 1024;

    DiskCacheOptions options = TestCacheUtils::create_simple_options(block_size, 20 * MB);
    options.inline_item_count_limit = 1000;
    auto cache = TestCacheUtils::create_cache(options);

    const size_t cache_size = 1024;
    const std::string cache_key = "test_file";

    std::string value(cache_size, 'a');
    ASSERT_OK(cache->write(cache_key, 0, cache_size, value.c_str()));

    WriteCacheOptions write_options;
    std::string value2(cache_size, 'b');
    Status st = cache->write(cache_key, 0, cache_size, value2.c_str(), &write_options);
    ASSERT_TRUE(st.is_already_exist());

    write_options.overwrite = true;
    ASSERT_OK(cache->write(cache_key, 0, cache_size, value2.c_str(), &write_options));

    char rvalue[cache_size] = {0};
    auto res = cache->read(cache_key, 0, cache_size, rvalue);
    ASSERT_TRUE(res.status().ok());
    std::string expect_value(cache_size, 'b');
    ASSERT_EQ(memcmp(rvalue, expect_value.c_str(), cache_size), 0);

    write_options.overwrite = false;
    std::string value3(cache_size, 'c');
    st = cache->write(cache_key, 0, cache_size, value3.c_str(), &write_options);
    ASSERT_TRUE(st.is_already_exist());

    ASSERT_OK(cache->shutdown());
}

TEST_F(BlockCacheTest, read_cache_with_adaptor) {
    const std::string cache_dir = "./block_disk_cache4";
    ASSERT_TRUE(fs::create_directories(cache_dir).ok());

    const size_t block_size = 1024 * 1024;
    DiskCacheOptions options = TestCacheUtils::create_simple_options(block_size, 0);
    options.dir_spaces.push_back({.path = cache_dir, .size = 500 * MB});
    options.skip_read_factor = 1;
    auto cache = TestCacheUtils::create_cache(options);

    const size_t batch_size = block_size - 1234;
    const size_t rounds = 20;
    const std::string cache_key = "test_file";

    // write cache
    for (size_t i = 0; i < rounds; ++i) {
        char ch = 'a' + i % 26;
        std::string value(batch_size, ch);
        Status st = cache->write(cache_key + std::to_string(i), 0, batch_size, value.c_str());
        ASSERT_TRUE(st.ok());
    }

    const int kAdaptorWindowSize = 50;

    // record read latency to ensure cache latency > remote latency
    for (size_t i = 0; i < kAdaptorWindowSize; ++i) {
        cache->record_read_local_cache(batch_size, 1000000000);
        cache->record_read_remote_storage(batch_size, 10, true);
    }

    // all reads will be rejected by cache adaptor
    for (size_t i = 0; i < rounds; ++i) {
        char ch = 'a' + i % 26;
        std::string expect_value(batch_size, ch);
        char value[batch_size] = {0};
        ReadCacheOptions opts;
        opts.use_adaptor = true;
        auto res = cache->read(cache_key + std::to_string(i), 0, batch_size, value, &opts);
        ASSERT_TRUE(res.status().is_resource_busy());
    }

    // record read latencyr to ensure cache latency < remote latency
    for (size_t i = 0; i < kAdaptorWindowSize; ++i) {
        cache->record_read_local_cache(batch_size, 10);
        cache->record_read_remote_storage(batch_size, 1000000000, true);
    }

    // all reads will be accepted by cache adaptor
    for (size_t i = 0; i < rounds; ++i) {
        char ch = 'a' + i % 26;
        std::string expect_value(batch_size, ch);
        char value[batch_size] = {0};
        ReadCacheOptions opts;
        opts.use_adaptor = true;
        auto res = cache->read(cache_key + std::to_string(i), 0, batch_size, value, &opts);
        ASSERT_TRUE(res.status().ok());
    }

    ASSERT_OK(cache->shutdown());
    ASSERT_OK(fs::remove_all(cache_dir));
}

TEST_F(BlockCacheTest, update_cache_quota) {
    const std::string cache_dir = "./block_disk_cache5";
    ASSERT_TRUE(fs::create_directories(cache_dir).ok());

    std::unique_ptr<BlockCache> block_cache(new BlockCache);
    const size_t block_size = 256 * 1024;
    size_t quota = 50 * MB;
    DiskCacheOptions options = TestCacheUtils::create_simple_options(block_size, 1 * MB);
    options.dir_spaces.push_back({.path = cache_dir, .size = quota});
    auto cache = TestCacheUtils::create_cache(options);
    auto local_cache = cache->local_cache();

    {
        auto metrics = local_cache->cache_metrics();
        ASSERT_EQ(metrics.mem_quota_bytes, options.mem_space_size);
        ASSERT_EQ(metrics.disk_quota_bytes, quota);
    }

    {
        size_t new_disk_quota = 100 * 1024 * 1024;
        std::vector<DirSpace> dir_spaces;
        dir_spaces.push_back({.path = cache_dir, .size = new_disk_quota});
        ASSERT_TRUE(local_cache->update_disk_spaces(dir_spaces).ok());
        auto metrics = local_cache->cache_metrics();
        ASSERT_EQ(metrics.disk_quota_bytes, new_disk_quota);
    }

    ASSERT_OK(cache->shutdown());
    ASSERT_OK(fs::remove_all(cache_dir));
}

TEST_F(BlockCacheTest, clear_residual_blockfiles) {
    const std::string cache_dir = "./block_disk_cache6";
    ASSERT_TRUE(fs::create_directories(cache_dir).ok());

    const size_t block_size = 256 * 1024;
    DiskCacheOptions options = TestCacheUtils::create_simple_options(block_size, 0);
    options.dir_spaces.push_back({.path = cache_dir, .size = 50 * MB});
    auto cache = TestCacheUtils::create_cache(options);

    // write cache
    {
        const size_t batch_size = block_size;
        const size_t rounds = 20;
        const std::string cache_key = "test_file";

        for (size_t i = 0; i < rounds; ++i) {
            char ch = 'a' + i % 26;
            std::string value(batch_size, ch);
            Status st = cache->write(cache_key + std::to_string(i), 0, batch_size, value.c_str());
            ASSERT_TRUE(st.ok());
        }
    }

    {
        std::vector<std::string> files;
        auto st = fs::get_children(cache_dir, &files);
        ASSERT_GT(files.size(), 0);
    }

    ASSERT_OK(cache->shutdown());
    DataCacheUtils::clean_residual_datacache(cache_dir);

    {
        std::vector<std::string> files;
        auto st = fs::get_children(cache_dir, &files);
        ASSERT_EQ(files.size(), 0);
    }

    ASSERT_OK(fs::remove_all(cache_dir));
}

TEST_F(BlockCacheTest, read_peer_cache) {
    DiskCacheOptions options = TestCacheUtils::create_simple_options(256 * KB, 1 * MB);
    auto cache = TestCacheUtils::create_cache(options);

    IOBuffer iobuf;
    ReadCacheOptions read_options;
    read_options.remote_host = "127.0.0.1";
    read_options.remote_port = 0;
    auto st = cache->read_buffer_from_remote_cache("test_key", 0, 100, &iobuf, &read_options);
    ASSERT_FALSE(st.ok());

    ASSERT_OK(cache->shutdown());
}
} // namespace starrocks
