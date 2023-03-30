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

#include "util/assert_mutex.h"

#include <gtest/gtest.h>

#include <iostream>

namespace starrocks {

TEST(TestAssertMutex, TestAssertMutex) {
    AssertHeldMutex mutex;
    EXPECT_FALSE(mutex.assert_held());
    {
        std::lock_guard<AssertHeldMutex> guard(mutex);
        EXPECT_TRUE(mutex.assert_held());
    }
    EXPECT_FALSE(mutex.assert_held());
    {
        std::unique_lock<AssertHeldMutex> guard(mutex);
        EXPECT_TRUE(mutex.assert_held());
    }
    EXPECT_FALSE(mutex.assert_held());
}

TEST(TestAssertMutex, TestAssertSharedMutex) {
    AssertHeldSharedMutex mutex;
    EXPECT_FALSE(mutex.assert_held_shared());
    EXPECT_FALSE(mutex.assert_held_exclusive());
    {
        std::shared_lock<AssertHeldSharedMutex> guard(mutex);
        EXPECT_TRUE(mutex.assert_held_shared());
        EXPECT_FALSE(mutex.assert_held_exclusive());
    }
    EXPECT_FALSE(mutex.assert_held_shared());
    EXPECT_FALSE(mutex.assert_held_exclusive());
    {
        std::unique_lock<AssertHeldSharedMutex> guard(mutex);
        EXPECT_FALSE(mutex.assert_held_shared());
        EXPECT_TRUE(mutex.assert_held_exclusive());
    }
    EXPECT_FALSE(mutex.assert_held_shared());
    EXPECT_FALSE(mutex.assert_held_exclusive());
}

TEST(TestAssertMutex, TestAssertSharedMutexReentrance) {
    AssertHeldSharedMutex mutex;
    EXPECT_FALSE(mutex.assert_held_shared());
    EXPECT_FALSE(mutex.assert_held_exclusive());
    {
        std::shared_lock<AssertHeldSharedMutex> guard(mutex);
        EXPECT_TRUE(mutex.assert_held_shared());
        EXPECT_EQ(mutex.TEST_get_shared_lock_cnt(), 1);
        {
            std::shared_lock<AssertHeldSharedMutex> guard(mutex);
            EXPECT_TRUE(mutex.assert_held_shared());
            EXPECT_EQ(mutex.TEST_get_shared_lock_cnt(), 2);
            {
                std::shared_lock<AssertHeldSharedMutex> guard(mutex);
                EXPECT_TRUE(mutex.assert_held_shared());
                EXPECT_EQ(mutex.TEST_get_shared_lock_cnt(), 3);
                {
                    std::shared_lock<AssertHeldSharedMutex> guard(mutex);
                    EXPECT_TRUE(mutex.assert_held_shared());
                    EXPECT_EQ(mutex.TEST_get_shared_lock_cnt(), 4);
                }
                EXPECT_TRUE(mutex.assert_held_shared());
                EXPECT_EQ(mutex.TEST_get_shared_lock_cnt(), 3);
            }
            EXPECT_TRUE(mutex.assert_held_shared());
            EXPECT_EQ(mutex.TEST_get_shared_lock_cnt(), 2);
        }
        EXPECT_TRUE(mutex.assert_held_shared());
        EXPECT_EQ(mutex.TEST_get_shared_lock_cnt(), 1);
    }
    EXPECT_FALSE(mutex.assert_held_shared());
    EXPECT_EQ(mutex.TEST_get_shared_lock_cnt(), 0);
}

TEST(TestAssertMutex, TestAssertMutexConcurrent) {
    AssertHeldMutex mutex;
    std::vector<std::thread> workers;
    int cnt = 0;
    for (int i = 0; i < 10; i++) {
        workers.emplace_back([&]() {
            std::lock_guard<AssertHeldMutex> guard(mutex);
            cnt += 1;
            EXPECT_TRUE(mutex.assert_held());
        });
    }
    for (int i = 0; i < 10; i++) {
        workers[i].join();
    }
    EXPECT_TRUE(cnt == 10);
}

} // namespace starrocks