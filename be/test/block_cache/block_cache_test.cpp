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

#include "block_cache/block_cache.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>

#include "common/logging.h"
#include "common/statusor.h"
#include "fs/fs_util.h"
#include "storage/options.h"

namespace starrocks {

class BlockCacheTest : public ::testing::Test {
protected:
    static void SetUpTestCase() {}

    static void TearDownTestCase() {}

    void SetUp() override {}
    void TearDown() override {}
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

TEST_F(BlockCacheTest, parse_cache_space_size_str) {
    const std::string cache_dir = "./block_disk_cache1";
    ASSERT_TRUE(fs::create_directories(cache_dir).ok());

    uint64_t mem_size = 10;
    ASSERT_EQ(parse_conf_datacache_mem_size("10", 0), mem_size);
    mem_size *= 1024;
    ASSERT_EQ(parse_conf_datacache_mem_size("10K", 0), mem_size);
    mem_size *= 1024;
    ASSERT_EQ(parse_conf_datacache_mem_size("10M", 0), mem_size);
    mem_size *= 1024;
    ASSERT_EQ(parse_conf_datacache_mem_size("10G", 0), mem_size);
    mem_size *= 1024;
    ASSERT_EQ(parse_conf_datacache_mem_size("10T", 0), mem_size);
    ASSERT_EQ(parse_conf_datacache_mem_size("10%", 10 * 1024), 1024);

    std::string disk_path = cache_dir;
    const int64_t kMaxLimit = 20L * 1024 * 1024 * 1024 * 1024; // 20T
    int64_t disk_size = 10;
    ASSERT_EQ(parse_conf_datacache_disk_size(disk_path, "10", kMaxLimit), disk_size);
    disk_size *= 1024;
    ASSERT_EQ(parse_conf_datacache_disk_size(disk_path, "10K", kMaxLimit), disk_size);
    disk_size *= 1024;
    ASSERT_EQ(parse_conf_datacache_disk_size(disk_path, "10M", kMaxLimit), disk_size);
    disk_size *= 1024;
    ASSERT_EQ(parse_conf_datacache_disk_size(disk_path, "10G", kMaxLimit), disk_size);
    disk_size *= 1024;
    ASSERT_EQ(parse_conf_datacache_disk_size(disk_path, "10T", kMaxLimit), disk_size);

    // The disk size exceed disk limit
    ASSERT_EQ(parse_conf_datacache_disk_size(disk_path, "10T", 1024), 1024);

    disk_size = parse_conf_datacache_disk_size(disk_path, "10%", kMaxLimit);
    ASSERT_EQ(disk_size, int64_t(10.0 / 100.0 * kMaxLimit));

    fs::remove_all(cache_dir).ok();
}

TEST_F(BlockCacheTest, parse_cache_space_paths) {
    const std::string cache_dir = "./block_disk_cache2";
    ASSERT_TRUE(fs::create_directories(cache_dir).ok());

    const std::string cwd = std::filesystem::current_path().string();
    const std::string s_normal_path = fmt::format("{}/block_disk_cache2/cache1;{}/block_disk_cache2/cache2", cwd, cwd);
    std::vector<std::string> paths;
    ASSERT_TRUE(parse_conf_datacache_disk_paths(s_normal_path, &paths, true).ok());
    ASSERT_EQ(paths.size(), 2);

    paths.clear();
    const std::string s_space_path =
            fmt::format(" {}/block_disk_cache2/cache3 ; {}/block_disk_cache2/cache4 ", cwd, cwd);
    ASSERT_TRUE(parse_conf_datacache_disk_paths(s_space_path, &paths, true).ok());
    ASSERT_EQ(paths.size(), 2);

    paths.clear();
    const std::string s_empty_path = fmt::format("//;{}/block_disk_cache2/cache4 ", cwd);
    ASSERT_FALSE(parse_conf_datacache_disk_paths(s_empty_path, &paths, true).ok());
    ASSERT_EQ(paths.size(), 1);

    paths.clear();
    const std::string s_invalid_path = fmt::format(" /block_disk_cache2/cache5;{}/+/cache6", cwd);
    ASSERT_FALSE(parse_conf_datacache_disk_paths(s_invalid_path, &paths, true).ok());
    ASSERT_EQ(paths.size(), 0);

    paths.clear();
    const std::string s_duplicated_path =
            fmt::format(" {}/block_disk_cache2/cache7 ; {}/block_disk_cache2/cache7 ", cwd, cwd);
    ASSERT_TRUE(parse_conf_datacache_disk_paths(s_duplicated_path, &paths, true).ok());
    ASSERT_EQ(paths.size(), 1);

    fs::remove_all(cache_dir).ok();
}

#ifdef WITH_STARCACHE
TEST_F(BlockCacheTest, hybrid_cache) {
    const std::string cache_dir = "./block_disk_cache3";
    ASSERT_TRUE(fs::create_directories(cache_dir).ok());

    std::unique_ptr<BlockCache> cache(new BlockCache);
    const size_t block_size = 256 * 1024;

    CacheOptions options;
    options.mem_space_size = 2 * 1024 * 1024;
    size_t quota = 50 * 1024 * 1024;
    options.disk_spaces.push_back({.path = cache_dir, .size = quota});
    options.block_size = block_size;
    options.max_concurrent_inserts = 100000;
    options.max_flying_memory_mb = 100;
    options.enable_direct_io = false;
    options.engine = "starcache";
    Status status = cache->init(options);
    ASSERT_TRUE(status.ok());

    const size_t batch_size = block_size;
    const size_t rounds = 10;
    const std::string cache_key = "test_file";

    // write cache
    for (size_t i = 0; i < rounds; ++i) {
        char ch = 'a' + i % 26;
        std::string value(batch_size, ch);
        Status st = cache->write_buffer(cache_key + std::to_string(i), 0, batch_size, value.c_str());
        ASSERT_TRUE(st.ok()) << st.message();
    }

    // read cache
    for (size_t i = 0; i < rounds; ++i) {
        char ch = 'a' + i % 26;
        std::string expect_value(batch_size, ch);
        char value[batch_size] = {0};
        auto res = cache->read_buffer(cache_key + std::to_string(i), 0, batch_size, value);
        ASSERT_TRUE(res.status().ok()) << res.status().message();
        ASSERT_EQ(memcmp(value, expect_value.c_str(), batch_size), 0);
    }

    // remove cache
    char value[1024] = {0};
    status = cache->remove(cache_key, 0, batch_size);
    ASSERT_TRUE(status.ok());

    auto res = cache->read_buffer(cache_key, 0, batch_size, value);
    ASSERT_TRUE(res.status().is_not_found());

    // not found
    res = cache->read_buffer(cache_key, block_size * 1000, batch_size, value);
    ASSERT_TRUE(res.status().is_not_found());

    cache->shutdown();
    fs::remove_all(cache_dir).ok();
}

TEST_F(BlockCacheTest, write_with_overwrite_option) {
    std::unique_ptr<BlockCache> cache(new BlockCache);
    const size_t block_size = 1024 * 1024;

    CacheOptions options;
    options.mem_space_size = 20 * 1024 * 1024;
    options.block_size = block_size;
    options.max_concurrent_inserts = 100000;
    options.max_flying_memory_mb = 100;
    options.engine = "starcache";
    Status status = cache->init(options);
    ASSERT_TRUE(status.ok());

    const size_t cache_size = 1024;
    const std::string cache_key = "test_file";

    std::string value(cache_size, 'a');
    Status st = cache->write_buffer(cache_key, 0, cache_size, value.c_str());
    ASSERT_TRUE(st.ok());

    WriteCacheOptions write_options;
    std::string value2(cache_size, 'b');
    st = cache->write_buffer(cache_key, 0, cache_size, value2.c_str(), &write_options);
    ASSERT_TRUE(st.is_already_exist());

    write_options.overwrite = true;
    st = cache->write_buffer(cache_key, 0, cache_size, value2.c_str(), &write_options);
    ASSERT_TRUE(st.ok());

    char rvalue[cache_size] = {0};
    auto res = cache->read_buffer(cache_key, 0, cache_size, rvalue);
    ASSERT_TRUE(res.status().ok());
    std::string expect_value(cache_size, 'b');
    ASSERT_EQ(memcmp(rvalue, expect_value.c_str(), cache_size), 0);

    write_options.overwrite = false;
    std::string value3(cache_size, 'c');
    st = cache->write_buffer(cache_key, 0, cache_size, value3.c_str(), &write_options);
    ASSERT_TRUE(st.is_already_exist());

    cache->shutdown();
}

TEST_F(BlockCacheTest, read_cache_with_adaptor) {
    const std::string cache_dir = "./block_disk_cache4";
    ASSERT_TRUE(fs::create_directories(cache_dir).ok());

    std::unique_ptr<BlockCache> cache(new BlockCache);
    const size_t block_size = 1024 * 1024;

    CacheOptions options;
    options.mem_space_size = 0;
    size_t quota = 500 * 1024 * 1024;
    options.disk_spaces.push_back({.path = cache_dir, .size = quota});
    options.block_size = block_size;
    options.max_concurrent_inserts = 100000;
    options.max_flying_memory_mb = 100;
    options.engine = "starcache";
    options.skip_read_factor = 1;
    Status status = cache->init(options);
    ASSERT_TRUE(status.ok());

    const size_t batch_size = block_size - 1234;
    const size_t rounds = 20;
    const std::string cache_key = "test_file";

    // write cache
    for (size_t i = 0; i < rounds; ++i) {
        char ch = 'a' + i % 26;
        std::string value(batch_size, ch);
        Status st = cache->write_buffer(cache_key + std::to_string(i), 0, batch_size, value.c_str());
        ASSERT_TRUE(st.ok());
    }

    const int kAdaptorWindowSize = 50;

    // record read latencyr to ensure cache latency > remote latency
    for (size_t i = 0; i < kAdaptorWindowSize; ++i) {
        cache->record_read_cache(batch_size, 1000000000);
        cache->record_read_remote(batch_size, 10);
    }

    // all reads will be reject by cache adaptor
    for (size_t i = 0; i < rounds; ++i) {
        char ch = 'a' + i % 26;
        std::string expect_value(batch_size, ch);
        char value[batch_size] = {0};
        ReadCacheOptions opts;
        opts.use_adaptor = true;
        auto res = cache->read_buffer(cache_key + std::to_string(i), 0, batch_size, value, &opts);
        ASSERT_TRUE(res.status().is_resource_busy());
    }

    // record read latencyr to ensure cache latency < remote latency
    for (size_t i = 0; i < kAdaptorWindowSize; ++i) {
        cache->record_read_cache(batch_size, 10);
        cache->record_read_remote(batch_size, 1000000000);
    }

    // all reads will be accepted by cache adaptor
    for (size_t i = 0; i < rounds; ++i) {
        char ch = 'a' + i % 26;
        std::string expect_value(batch_size, ch);
        char value[batch_size] = {0};
        ReadCacheOptions opts;
        opts.use_adaptor = true;
        auto res = cache->read_buffer(cache_key + std::to_string(i), 0, batch_size, value, &opts);
        ASSERT_TRUE(res.status().ok());
    }

    cache->shutdown();
    fs::remove_all(cache_dir).ok();
}

TEST_F(BlockCacheTest, update_cache_quota) {
    const std::string cache_dir = "./block_disk_cache5";
    ASSERT_TRUE(fs::create_directories(cache_dir).ok());

    std::unique_ptr<BlockCache> cache(new BlockCache);
    const size_t block_size = 256 * 1024;

    CacheOptions options;
    options.mem_space_size = 1 * 1024 * 1024;
    size_t quota = 50 * 1024 * 1024;
    options.disk_spaces.push_back({.path = cache_dir, .size = quota});
    options.block_size = block_size;
    options.max_concurrent_inserts = 100000;
    options.max_flying_memory_mb = 100;
    options.enable_direct_io = false;
    options.engine = "starcache";
    Status status = cache->init(options);
    ASSERT_TRUE(status.ok());

    {
        auto metrics = cache->cache_metrics();
        ASSERT_EQ(metrics.mem_quota_bytes, options.mem_space_size);
        ASSERT_EQ(metrics.disk_quota_bytes, quota);
    }

    {
        size_t new_mem_quota = 2 * 1024 * 1024;
        ASSERT_TRUE(cache->update_mem_quota(new_mem_quota, false).ok());
        auto metrics = cache->cache_metrics();
        ASSERT_EQ(metrics.mem_quota_bytes, new_mem_quota);
    }

    {
        size_t new_disk_quota = 100 * 1024 * 1024;
        std::vector<DirSpace> dir_spaces;
        dir_spaces.push_back({.path = cache_dir, .size = new_disk_quota});
        ASSERT_TRUE(cache->update_disk_spaces(dir_spaces).ok());
        auto metrics = cache->cache_metrics();
        ASSERT_EQ(metrics.disk_quota_bytes, new_disk_quota);
    }

    cache->shutdown();
    fs::remove_all(cache_dir).ok();
}

#endif

#ifdef WITH_CACHELIB
TEST_F(BlockCacheTest, custom_lru_insertion_point) {
    std::unique_ptr<BlockCache> cache(new BlockCache);
    const size_t block_size = 1024 * 1024;

    CacheOptions options;
    options.mem_space_size = 20 * 1024 * 1024;
    options.block_size = block_size;
    options.max_concurrent_inserts = 100000;
    options.max_flying_memory_mb = 100;
    options.engine = "cachelib";
    Status status = cache->init(options);
    ASSERT_TRUE(status.ok());

    const size_t rounds = 20;
    const size_t batch_size = block_size;
    const std::string cache_key = "test_file";
    // write cache
    // only 12 blocks can be cached
    for (size_t i = 0; i < rounds; ++i) {
        char ch = 'a' + i % 26;
        std::string value(batch_size, ch);
        Status st = cache->write_buffer(cache_key + std::to_string(i), 0, batch_size, value.c_str());
        ASSERT_TRUE(st.ok());
    }

    // read cache
    // with the 1/2 lru insertion point, the test_file1 items will not be evicted
    char value[batch_size] = {0};
    auto res = cache->read_buffer(cache_key + std::to_string(1), 0, batch_size, value);
    ASSERT_TRUE(res.status().ok());

    cache->shutdown();
}
#endif

} // namespace starrocks
