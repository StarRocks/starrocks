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

#include "base/brpc/ref_count_closure.h"

#include <gtest/gtest.h>

#include <atomic>

namespace starrocks {

namespace {

class TrackingRefCountClosure final : public RefCountClosure<int> {
public:
    ~TrackingRefCountClosure() override { ++destroyed_count; }

    static std::atomic<int> destroyed_count;
};

std::atomic<int> TrackingRefCountClosure::destroyed_count{0};

} // namespace

TEST(RefCountClosureTest, RefAndUnref) {
    auto* closure = new RefCountClosure<int>();
    EXPECT_EQ(0, closure->count());

    closure->ref();
    EXPECT_EQ(1, closure->count());

    EXPECT_TRUE(closure->unref());
    delete closure;
}

TEST(RefCountClosureTest, RunDeletesOnLastRef) {
    TrackingRefCountClosure::destroyed_count.store(0);
    auto* closure = new TrackingRefCountClosure();
    closure->ref();
    closure->Run();
    EXPECT_EQ(1, TrackingRefCountClosure::destroyed_count.load());
}

} // namespace starrocks
