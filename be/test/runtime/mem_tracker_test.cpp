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

#include "runtime/mem_tracker.h"

#include <gtest/gtest.h>

namespace starrocks {

class MemTrackerTest : public testing::Test {
protected:
    void SetUp() override {
        _process_mem_tracker = std::make_unique<MemTracker>(-1, "process");
        _query_pool_mem_tracker = std::make_unique<MemTracker>(-1, "query_pool", _process_mem_tracker.get());
        _query_mem_tracker = std::make_unique<MemTracker>(-1, "query", _query_pool_mem_tracker.get());
    }

    std::unique_ptr<MemTracker> _process_mem_tracker;
    std::unique_ptr<MemTracker> _query_pool_mem_tracker;
    std::unique_ptr<MemTracker> _query_mem_tracker;
};

TEST_F(MemTrackerTest, consume) {
    _query_mem_tracker->consume(10);
    ASSERT_EQ(_query_mem_tracker->consumption(), 10);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), 10);
    ASSERT_EQ(_process_mem_tracker->consumption(), 10);

    _query_mem_tracker->consume(-30);
    ASSERT_EQ(_query_mem_tracker->consumption(), -20);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), -20);
    ASSERT_EQ(_process_mem_tracker->consumption(), -20);

    _query_mem_tracker->consume(0);
    ASSERT_EQ(_query_mem_tracker->consumption(), -20);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), -20);
    ASSERT_EQ(_process_mem_tracker->consumption(), -20);

    ASSERT_EQ(_query_mem_tracker->peak_consumption(), 10);
    ASSERT_EQ(_query_pool_mem_tracker->peak_consumption(), 10);
    ASSERT_EQ(_process_mem_tracker->peak_consumption(), 10);
}

TEST_F(MemTrackerTest, release) {
    _query_mem_tracker->release(10);
    ASSERT_EQ(_query_mem_tracker->consumption(), -10);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), -10);
    ASSERT_EQ(_process_mem_tracker->consumption(), -10);

    _query_mem_tracker->release(-30);
    ASSERT_EQ(_query_mem_tracker->consumption(), 20);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), 20);
    ASSERT_EQ(_process_mem_tracker->consumption(), 20);

    _query_mem_tracker->release(0);
    ASSERT_EQ(_query_mem_tracker->consumption(), 20);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), 20);
    ASSERT_EQ(_process_mem_tracker->consumption(), 20);

    ASSERT_EQ(_query_mem_tracker->peak_consumption(), 20);
    ASSERT_EQ(_query_pool_mem_tracker->peak_consumption(), 20);
    ASSERT_EQ(_process_mem_tracker->peak_consumption(), 20);
}

TEST_F(MemTrackerTest, consume_without_root) {
    _query_mem_tracker->consume(10);
    ASSERT_EQ(_query_mem_tracker->consumption(), 10);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), 10);
    ASSERT_EQ(_process_mem_tracker->consumption(), 10);

    _process_mem_tracker->consume(10);
    ASSERT_EQ(_query_mem_tracker->consumption(), 10);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), 10);
    ASSERT_EQ(_process_mem_tracker->consumption(), 20);

    _query_mem_tracker->consume_without_root(5);
    ASSERT_EQ(_query_mem_tracker->consumption(), 15);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), 15);
    ASSERT_EQ(_process_mem_tracker->consumption(), 20);

    _query_mem_tracker->consume_without_root(0);
    ASSERT_EQ(_query_mem_tracker->consumption(), 15);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), 15);
    ASSERT_EQ(_process_mem_tracker->consumption(), 20);

    _process_mem_tracker->consume_without_root(10);
    ASSERT_EQ(_query_mem_tracker->consumption(), 15);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), 15);
    ASSERT_EQ(_process_mem_tracker->consumption(), 20);

    _query_mem_tracker->consume_without_root(-5);
    ASSERT_EQ(_query_mem_tracker->consumption(), 10);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), 10);
    ASSERT_EQ(_process_mem_tracker->consumption(), 20);

    _query_mem_tracker->release_without_root();
    ASSERT_EQ(_query_mem_tracker->consumption(), 0);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), 0);
    ASSERT_EQ(_process_mem_tracker->consumption(), 20);
}

TEST_F(MemTrackerTest, release_without_root) {
    _query_mem_tracker->consume(10);
    ASSERT_EQ(_query_mem_tracker->consumption(), 10);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), 10);
    ASSERT_EQ(_process_mem_tracker->consumption(), 10);

    _query_mem_tracker->release_without_root(5);
    ASSERT_EQ(_query_mem_tracker->consumption(), 5);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), 5);
    ASSERT_EQ(_process_mem_tracker->consumption(), 10);

    _query_mem_tracker->release_without_root(0);
    ASSERT_EQ(_query_mem_tracker->consumption(), 5);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), 5);
    ASSERT_EQ(_process_mem_tracker->consumption(), 10);

    _process_mem_tracker->release_without_root(5);
    ASSERT_EQ(_query_mem_tracker->consumption(), 5);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), 5);
    ASSERT_EQ(_process_mem_tracker->consumption(), 10);

    _query_mem_tracker->release_without_root(-8);
    ASSERT_EQ(_query_mem_tracker->consumption(), 13);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), 13);
    ASSERT_EQ(_process_mem_tracker->consumption(), 10);
}

} // namespace starrocks