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

#include "base/brpc/reusable_closure.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

namespace starrocks {

namespace {

class TrackingReusableClosure final : public ReusableClosure<int> {
public:
    ~TrackingReusableClosure() override { ++destroyed_count; }

    static std::atomic<int> destroyed_count;
};

std::atomic<int> TrackingReusableClosure::destroyed_count{0};

} // namespace

TEST(ReusableClosureTest, RunUpdatesLatencyAndRefCount) {
    auto* closure = new ReusableClosure<int>();

    closure->ref();
    closure->ref();
    EXPECT_EQ(2, closure->count());

    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    closure->Run();

    EXPECT_EQ(1, closure->count());
    EXPECT_GE(closure->latency(), 0);
    EXPECT_FALSE(closure->join());
    closure->cancel();

    if (closure->unref()) {
        delete closure;
    }
}

TEST(ReusableClosureTest, JoinCancelNoopOnInvalidCallId) {
    auto* closure = new ReusableClosure<int>();
    closure->ref();
    EXPECT_FALSE(closure->join());
    closure->cancel();

    if (closure->unref()) {
        delete closure;
    }
}

TEST(ReusableClosureTest, RunDeletesOnLastRef) {
    TrackingReusableClosure::destroyed_count.store(0);
    auto* closure = new TrackingReusableClosure();
    closure->ref();
    closure->Run();
    EXPECT_EQ(1, TrackingReusableClosure::destroyed_count.load());
}

} // namespace starrocks
