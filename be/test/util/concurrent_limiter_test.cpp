// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "util/concurrent_limiter.h"

#include <gtest/gtest.h>

#include <iostream>

namespace starrocks {

class ConcurrentLimiterTest : public testing::Test {
public:
    ConcurrentLimiterTest() = default;
    ~ConcurrentLimiterTest() override = default;
};

TEST_F(ConcurrentLimiterTest, test_concurrent_limiter) {
    ConcurrentLimiter limiter0(0);
    ASSERT_FALSE(limiter0.inc());
    for (int i = 1; i <= 100; i++) {
        ConcurrentLimiter limiter(i);
        // repeate inc and dec
        for (int j = 0; j < 100; j++) {
            ASSERT_TRUE(limiter.inc());
            limiter.dec();
        }
        // inc until fail
        for (int j = 0; j < i; j++) {
            ASSERT_TRUE(limiter.inc());
        }
        ASSERT_FALSE(limiter.inc());
        ASSERT_FALSE(limiter.inc());
        for (int j = 0; j < i; j++) {
            limiter.dec();
        }
    }
}

TEST_F(ConcurrentLimiterTest, test_concurrent_limiter_guard) {
    ConcurrentLimiter limiter(1);
    for (int j = 0; j < 10; j++) {
        ConcurrentLimiterGuard guard;
        ASSERT_TRUE(guard.set_limiter(&limiter));
    }
}

} // namespace starrocks