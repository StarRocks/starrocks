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

#include "storage/storage_engine.h"

#include <gtest/gtest.h>

#include <cstdlib>

#include "base/testutil/parallel_test.h"
#include "base/time/time.h"
#include "base/utility/defer_op.h"
#include "common/config_storage_fwd.h"
#include "common/config_vector_index_fwd.h"
#include "storage/index/vector/vector_index_cache.h"
#include "storage/storage_env.h"

#ifdef WITH_TENANN
#include "tenann/index/index.h"
#include "tenann/index/index_cache.h"
#endif

namespace starrocks {

PARALLEL_TEST(StorageEngineTest, test_garbage_sweep_interval_calculator) {
    config::min_garbage_sweep_interval = 100;
    config::max_garbage_sweep_interval = 10000;
    GarbageSweepIntervalCalculator calculator;

    struct TestCase {
        TestCase(int32_t original_min, int32_t original_max, bool expected_changed, int32_t expected_min,
                 int32_t expected_max)
                : original_min(original_min),
                  original_max(original_max),
                  expected_changed(expected_changed),
                  expected_min(expected_min),
                  expected_max(expected_max) {}

        const int32_t original_min;
        const int32_t original_max;

        const bool expected_changed;
        const int32_t expected_min;
        const int32_t expected_max;
    };
    TestCase test_cases[] = {{100, 2, true, 1, 2},  {-1, 4, true, 1, 4},  {-10, -2, true, 1, 1},
                             {0, 4, true, 1, 4},    {0, 0, true, 1, 1},   {2, 10, true, 2, 10},
                             {2, 10, false, 2, 10}, {3, 10, true, 3, 10}, {3, 11, true, 3, 11}};

    for (const auto& c : test_cases) {
        config::min_garbage_sweep_interval = c.original_min;
        config::max_garbage_sweep_interval = c.original_max;
        ASSERT_EQ(c.expected_changed, calculator.maybe_interval_updated());

        calculator.mutable_disk_usage() = 1000; // Make disk usage large enough to use min_interval as curr_interval.
        ASSERT_EQ(c.expected_min, calculator.curr_interval());
        calculator.mutable_disk_usage() = -1; // Make disk usage small enough to use max_interval as curr_interval.
        ASSERT_EQ(c.expected_max, calculator.curr_interval());

        for (double usage = -1; usage <= 2.0; usage += 0.1) {
            calculator.mutable_disk_usage() = usage;
            int32_t curr = calculator.curr_interval();
            ASSERT_GE(curr, c.expected_min);
            ASSERT_LE(curr, c.expected_max);
        }
    }
}

#ifdef WITH_TENANN

class StorageEngineCacheExpireTest : public testing::Test {
protected:
    static void expire_caches(StorageEngine* engine, int64_t vector_cache_now) {
        engine->_expire_caches(vector_cache_now);
    }
};

TEST_F(StorageEngineCacheExpireTest, expire_caches_includes_vector_cache) {
    auto* engine = StorageEngine::instance();
    auto* cache = StorageEnv::GetInstance()->vector_index_cache();
    ASSERT_NE(engine, nullptr);
    ASSERT_NE(cache, nullptr);
    ASSERT_FALSE(engine->bg_worker_stopped());

    const int32_t saved_vector_expire_sec = config::vector_index_cache_expire_sec;
    DeferOp restore([&] { config::vector_index_cache_expire_sec = saved_vector_expire_sec; });

    config::vector_index_cache_expire_sec = 1;

    constexpr size_t kBytes = 1024;
    void* buffer = std::malloc(kBytes);
    auto ref = std::make_shared<tenann::Index>(
            buffer, tenann::IndexType::kFaissHnsw, [](void* value) { std::free(value); }, kBytes);
    tenann::IndexCacheHandle handle;
    const tenann::CacheKey key("/storage-engine-cache-expire-worker.vi");
    cache->Insert(key, std::move(ref), &handle);
    handle = tenann::IndexCacheHandle{};

    expire_caches(engine, MonotonicMillis() + 1100);

    tenann::IndexCacheHandle probe;
    EXPECT_FALSE(cache->Lookup(key, &probe));
}

#endif

} // namespace starrocks
