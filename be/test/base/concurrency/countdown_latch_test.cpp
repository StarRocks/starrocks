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

#include "base/concurrency/countdown_latch.h"

#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <thread>

#include "base/time/monotime.h"

namespace starrocks {

static void decrement_latch(CountDownLatch* latch, int amount) {
    if (amount == 1) {
        latch->count_down();
        return;
    }
    latch->count_down(amount);
}

// Tests that we can decrement the latch by arbitrary amounts, as well
// as 1 by one.
TEST(TestCountDownLatch, TestLatch) {
    CountDownLatch latch(1000);

    // Decrement the count by 1 in another thread, this should not fire the
    // latch.
    std::thread decrement_once([&] { decrement_latch(&latch, 1); });
    decrement_once.join();
    ASSERT_FALSE(latch.wait_for(MonoDelta::FromMilliseconds(200)));
    ASSERT_EQ(999, latch.count());

    // Now decrement by 1000 this should decrement to 0 and fire the latch
    // (even though 1000 is one more than the current count).
    std::thread decrement_all([&] { decrement_latch(&latch, 1000); });
    latch.wait();
    decrement_all.join();
    ASSERT_EQ(0, latch.count());
}

// Test that resetting to zero while there are waiters lets the waiters
// continue.
TEST(TestCountDownLatch, TestResetToZero) {
    CountDownLatch cdl(100);
    std::thread waiter([&] { cdl.wait(); });

    // Sleep for a bit until it's likely the other thread is waiting on the latch.
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    cdl.reset(0);
    waiter.join();
}

} // namespace starrocks
