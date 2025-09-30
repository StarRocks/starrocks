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

#include "storage/rowset/page_handle.h"

#include <gtest/gtest.h>

#include "cache/mem_cache/lrucache_engine.h"
#include "cache/mem_cache/page_cache.h"
#include "testutil/assert.h"

namespace starrocks {
class PageHandleTest : public testing::Test {
public:
    void SetUp() override;
    void TearDown() override {}

protected:
    std::shared_ptr<LRUCacheEngine> _cache_engine;
    std::shared_ptr<StoragePageCache> _page_cache;
};

void PageHandleTest::SetUp() {
    MemCacheOptions options{.mem_space_size = 10 * 1024 * 1024};
    _cache_engine = std::make_shared<LRUCacheEngine>();
    ASSERT_OK(_cache_engine->init(options));
    _page_cache = std::make_shared<StoragePageCache>(_cache_engine.get());
}

TEST_F(PageHandleTest, test_operator_not_owner) {
    ObjectCacheWriteOptions opts;

    std::vector<uint8_t>* p1 = new std::vector<uint8_t>(2);
    (*p1)[0] = 0;
    (*p1)[1] = 1;
    PageCacheHandle cache_handle_1;
    ASSERT_OK(_page_cache->insert("key1", p1, opts, &cache_handle_1));
    PageHandle handle1(std::move(cache_handle_1));

    std::vector<uint8_t>* p2 = new std::vector<uint8_t>(2);
    (*p2)[0] = 1;
    (*p2)[1] = 0;
    PageCacheHandle cache_handle_2;
    ASSERT_OK(_page_cache->insert("key2", p2, opts, &cache_handle_2));
    PageHandle handle2(std::move(cache_handle_2));

    handle1 = std::move(handle2);
    const auto* data = handle1.data();
    ASSERT_EQ((*data)[0], 1);
    ASSERT_EQ((*data)[1], 0);
}

TEST_F(PageHandleTest, test_operator_owner) {
    std::vector<uint8_t>* p1 = new std::vector<uint8_t>(2);
    (*p1)[0] = 0;
    (*p1)[1] = 1;
    PageHandle handle1(p1);

    std::vector<uint8_t>* p2 = new std::vector<uint8_t>(2);
    (*p2)[0] = 1;
    (*p2)[1] = 0;
    PageHandle handle2(p2);

    handle1 = std::move(handle2);
    const auto* data = handle1.data();
    ASSERT_EQ((*data)[0], 1);
    ASSERT_EQ((*data)[1], 0);
}
} // namespace starrocks