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

#include "util/ratelimit.h"

#include <gtest/gtest.h>

#include "common/logging.h"

namespace starrocks {

class RateLimitTest : public testing::Test {
public:
    RateLimitTest() = default;
    ~RateLimitTest() override = default;
};

TEST_F(RateLimitTest, rate_limit) {
    int count = 0;
    int64_t start = starrocks::UnixMillis();
    for (int i = 0; i < 100; i++) {
        RATE_LIMIT(count++, 100); // inc each 0.1s
        RATE_LIMIT(std::cout << "skip log cnt: " << RATE_LIMIT_SKIP_CNT << std::endl, 100);
        starrocks::SleepForMs(10); // execute inc each 10ms, which is probably not accurate.
    }
    int64_t end = starrocks::UnixMillis();
    ASSERT_TRUE(count <= (end - start) / 100);
}

TEST_F(RateLimitTest, rate_limit_by_tag) {
    int count = 0;
    int64_t start = starrocks::UnixMillis();
    for (int i = 0; i < 100; i++) {
        RATE_LIMIT_BY_TAG(i % 2, count++, 100); // inc each 0.1s
        starrocks::SleepForMs(10);              // execute inc each 10ms, which is probably not accurate.
    }
    int64_t end = starrocks::UnixMillis();
    ASSERT_TRUE(count <= (end - start) / 100 * 2);
}

} // namespace starrocks
