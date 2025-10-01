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

#include "cache/starcache_engine.h"

#include <gtest/gtest.h>

#include "cache/block_cache/test_cache_utils.h"
#include "fs/fs_util.h"
#include "testutil/assert.h"

namespace starrocks {

class StarCacheEngineTest : public ::testing::Test {
protected:
    void SetUp() override;
    void TearDown() override;

    void _init_local_cache();
    static std::string _int_to_string(size_t length, int num);

    std::string _cache_dir = "./starcache_engine_test";
    std::shared_ptr<StarCacheEngine> _cache;

    int64_t _mem_quota = 64 * 1024 * 1024;
};

void StarCacheEngineTest::SetUp() {
    ASSERT_OK(fs::create_directories(_cache_dir));
    _init_local_cache();
}

void StarCacheEngineTest::TearDown() {
    ASSERT_OK(_cache->shutdown());
    ASSERT_OK(fs::remove_all(_cache_dir));
}

std::string StarCacheEngineTest::_int_to_string(size_t length, int num) {
    std::ostringstream oss;
    oss << std::setw(length) << std::setfill('0') << num;
    return oss.str();
}

void StarCacheEngineTest::_init_local_cache() {
    DiskCacheOptions options = TestCacheUtils::create_simple_options(256 * KB, _mem_quota);
    options.dir_spaces.push_back({.path = _cache_dir, .size = 50 * MB});

    _cache = std::make_shared<StarCacheEngine>();
    ASSERT_OK(_cache->init(options));
}

static void empty_deleter(void*) {}

TEST_F(StarCacheEngineTest, adjust_inline_cache_count_limit) {
    std::string key = "inline1";
    std::string val = "inline1";
    {
        IOBuffer buffer;
        buffer.append_user_data((void*)val.data(), val.size(), empty_deleter);
        ASSERT_OK(_cache->write(key, buffer, nullptr));
    }

    ASSERT_OK(_cache->update_inline_cache_count_limit(0));

    {
        IOBuffer buffer;
        ASSERT_TRUE(_cache->read(key, 0, val.size(), &buffer, nullptr).is_not_found());
    }

    ASSERT_OK(_cache->update_inline_cache_count_limit(131072));

    {
        IOBuffer buffer;
        buffer.append_user_data((void*)val.data(), val.size(), empty_deleter);
        ASSERT_OK(_cache->write(key, buffer, nullptr));
    }

    {
        IOBuffer buffer;
        ASSERT_OK(_cache->read(key, 0, val.size(), &buffer, nullptr));
    }
}
} // namespace starrocks
