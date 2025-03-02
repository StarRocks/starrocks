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

#include "cache/block_cache/block_cache_hit_rate_counter.hpp"

#include <gtest/gtest.h>

namespace starrocks {

class BlockCacheHitRateCounterTest : public ::testing::Test {
protected:
    static void SetUpTestCase() {}

    static void TearDownTestCase() {}

    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(BlockCacheHitRateCounterTest, app_hit_rate) {
    BlockCacheHitRateCounter counter{};
    EXPECT_EQ(0, counter.hit_rate());
    EXPECT_EQ(0, counter.get_hit_bytes_last_minute());
    EXPECT_EQ(0, counter.get_miss_bytes_last_minute());
    EXPECT_EQ(0, counter.hit_rate_last_minute());

    counter.update(3, 10);

    EXPECT_EQ(3, counter.get_hit_bytes());
    EXPECT_EQ(10, counter.get_miss_bytes());
    EXPECT_EQ(0.23, counter.hit_rate());
}
} // namespace starrocks