// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
    for (int i = 0; i < 100; i++) {
        RATE_LIMIT(count++, 100); // inc each 0.1s
        RATE_LIMIT(std::cout << "skip log cnt: " << RATE_LIMIT_SKIP_CNT << std::endl, 100);
        usleep(10000); // execute inc each 10ms
    }
    ASSERT_TRUE(count <= 10);
}

TEST_F(RateLimitTest, rate_limit_by_tag) {
    int count = 0;
    for (int i = 0; i < 100; i++) {
        RATE_LIMIT_BY_TAG(i % 2, count++, 100); // inc each 0.1s
        usleep(10000);                          // execute inc each 10ms
    }
    ASSERT_TRUE(count <= 20);
}

} // namespace starrocks
