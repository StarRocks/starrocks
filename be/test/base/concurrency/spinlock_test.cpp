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

#include "base/concurrency/spinlock.h"

#include <gtest/gtest.h>

#include <thread>
#include <vector>

namespace starrocks {

TEST(SpinLockTest, ProtectsSharedCounter) {
    SpinLock lock;
    int counter = 0;
    constexpr int kThreads = 4;
    constexpr int kIterations = 10000;

    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&] {
            for (int j = 0; j < kIterations; ++j) {
                lock.lock();
                ++counter;
                lock.unlock();
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(counter, kThreads * kIterations);
}

} // namespace starrocks
