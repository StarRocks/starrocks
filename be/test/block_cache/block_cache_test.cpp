// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "block_cache/block_cache.h"

#include <gtest/gtest.h>

#include <cstring>

#include "common/logging.h"
#include "common/statusor.h"
#include "fs/fs_util.h"

namespace starrocks {

class BlockCacheTest : public ::testing::Test {
protected:
    void SetUp() override { ASSERT_TRUE(fs::create_directories("./ut_dir/block_disk_cache").ok()); }
    void TearDown() override { ASSERT_TRUE(fs::remove_all("./ut_dir").ok()); }
};

TEST_F(BlockCacheTest, hybrid_cache) {
    std::unique_ptr<BlockCache> cache(new BlockCache);
    const size_t block_size = 1 * 1024 * 1024;

    CacheOptions options;
    options.mem_space_size = 20 * 1024 * 1024;
    size_t quota = 500 * 1024 * 1024;
    options.disk_spaces.push_back({.path = "./ut_dir/block_disk_cache", .size = quota});
    options.block_size = block_size;
    Status status = cache->init(options);
    ASSERT_TRUE(status.ok());

    const size_t batch_size = 2 * block_size + 1234;
    const size_t rounds = 20;
    const std::string cache_key = "test_file";

    // write cache
    off_t offset = 0;
    for (size_t i = 0; i < rounds; ++i) {
        char ch = 'a' + i % 26;
        std::string value(batch_size, ch);
        Status st = cache->write_cache(cache_key + std::to_string(i), 0, batch_size, value.c_str());
        ASSERT_TRUE(st.ok());
        offset += batch_size;
    }

    // read cache
    offset = 0;
    for (size_t i = 0; i < rounds; ++i) {
        char ch = 'a' + i % 26;
        std::string expect_value(batch_size, ch);
        char value[batch_size] = {0};
        auto res = cache->read_cache(cache_key + std::to_string(i), 0, batch_size, value);
        ASSERT_TRUE(res.status().ok());
        ASSERT_EQ(memcmp(value, expect_value.c_str(), batch_size), 0);
        offset += batch_size;
    }

    // remove cache
    char value[1024] = {0};
    status = cache->remove_cache(cache_key, 0, batch_size);
    ASSERT_TRUE(status.ok());

    auto res = cache->read_cache(cache_key, 0, batch_size, value);
    ASSERT_TRUE(res.status().is_not_found());

    // invalid offset
    res = cache->read_cache(cache_key, 1000000, batch_size, value);
    ASSERT_TRUE(res.status().is_invalid_argument());

    // not found
    res = cache->read_cache(cache_key, block_size * 1000, batch_size, value);
    ASSERT_TRUE(res.status().is_not_found());
}

TEST_F(BlockCacheTest, auto_create_disk_cache_path) {
    std::unique_ptr<BlockCache> cache(new BlockCache);
    const size_t block_size = 1 * 1024 * 1024;

    CacheOptions options;
    options.mem_space_size = 20 * 1024 * 1024;
    size_t quota = 500 * 1024 * 1024;
    options.disk_spaces.push_back({.path = "./ut_dir/final_entry_not_exist", .size = quota});
    options.block_size = block_size;
    options.engine = "starcache";
    Status status = cache->init(options);
    ASSERT_TRUE(status.ok());

    const size_t batch_size = block_size - 1234;
    const size_t rounds = 3;
    const std::string cache_key = "test_file";

    // write cache
    off_t offset = 0;
    for (size_t i = 0; i < rounds; ++i) {
        char ch = 'a' + i % 26;
        std::string value(batch_size, ch);
        Status st = cache->write_cache(cache_key + std::to_string(i), 0, batch_size, value.c_str());
        ASSERT_TRUE(st.ok());
        offset += batch_size;
    }

    // read cache
    offset = 0;
    for (size_t i = 0; i < rounds; ++i) {
        char ch = 'a' + i % 26;
        std::string expect_value(batch_size, ch);
        char value[batch_size] = {0};
        auto res = cache->read_cache(cache_key + std::to_string(i), 0, batch_size, value);
        ASSERT_TRUE(res.status().ok());
        ASSERT_EQ(memcmp(value, expect_value.c_str(), batch_size), 0);
        offset += batch_size;
    }

    cache->shutdown();
}

} // namespace starrocks
