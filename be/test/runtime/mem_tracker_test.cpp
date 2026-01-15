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
        using MemTrackerType::PROCESS;
        using MemTrackerType::QUERY_POOL;
        _process_mem_tracker = std::make_unique<MemTracker>(PROCESS, 1024, "process");
        auto parent = _process_mem_tracker.get();
        _query_pool_mem_tracker = std::make_unique<MemTracker>(QUERY_POOL, 512, "query_pool", parent);
        _query_1 = std::make_unique<MemTracker>(128, "query_1", _query_pool_mem_tracker.get());
        _query_2 = std::make_unique<MemTracker>(128, "query_2", _query_pool_mem_tracker.get());
    }

    std::unique_ptr<MemTracker> _process_mem_tracker;
    std::unique_ptr<MemTracker> _query_pool_mem_tracker;
    std::unique_ptr<MemTracker> _query_1;
    std::unique_ptr<MemTracker> _query_2;
    ObjectPool _pool;
};

TEST_F(MemTrackerTest, label_type_convert) {
    const auto& mem_types = MemTracker::mem_types();
    ASSERT_GT(mem_types.size(), 0);

    for (const auto& item : mem_types) {
        ASSERT_EQ(MemTracker::type_to_label(item.first), item.second);
        ASSERT_EQ(MemTracker::label_to_type(item.second), item.first);
    }
    ASSERT_EQ(MemTracker::type_to_label(MemTrackerType::QUERY), "");
    ASSERT_EQ(MemTracker::label_to_type("not_exist_label"), MemTrackerType::NO_SET);
}

TEST_F(MemTrackerTest, get_snapshot) {
    _query_1->consume(10);
    _query_2->consume(20);

    auto* snapshot = _process_mem_tracker->get_snapshot(&_pool, 2);
    ASSERT_EQ(snapshot->debug_string(),
              "{\"label:\"process\",\"level:\"1\",\"limit:\"1024\",\"cur_mem_usage:\"30\",\"peak_mem_usage:\"30\","
              "\"child\":[{\"label:\"query_pool\",\"level:\"2\",\"limit:\"512\",\"cur_mem_usage:\"30\",\"peak_mem_"
              "usage:\"30\",\"child\":[]}]}");

    snapshot = _process_mem_tracker->get_snapshot(&_pool, 10);
    ASSERT_EQ(snapshot->debug_string(),
              "{\"label:\"process\",\"level:\"1\",\"limit:\"1024\",\"cur_mem_usage:\"30\",\"peak_mem_usage:\"30\","
              "\"child\":[{\"label:\"query_pool\",\"level:\"2\",\"limit:\"512\",\"cur_mem_usage:\"30\",\"peak_mem_"
              "usage:\"30\",\"child\":[{\"label:\"query_1\",\"level:\"3\",\"limit:\"128\",\"cur_mem_usage:\"10\","
              "\"peak_mem_usage:\"10\",\"child\":[]},{\"label:\"query_2\",\"level:\"3\",\"limit:\"128\",\"cur_mem_"
              "usage:\"20\",\"peak_mem_usage:\"20\",\"child\":[]}]}]}");
}

TEST_F(MemTrackerTest, consume) {
    _query_1->consume(10);
    ASSERT_EQ(_query_1->consumption(), 10);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), 10);
    ASSERT_EQ(_process_mem_tracker->consumption(), 10);

    _query_1->consume(-30);
    ASSERT_EQ(_query_1->consumption(), -20);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), -20);
    ASSERT_EQ(_process_mem_tracker->consumption(), -20);

    _query_1->consume(0);
    ASSERT_EQ(_query_1->consumption(), -20);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), -20);
    ASSERT_EQ(_process_mem_tracker->consumption(), -20);

    ASSERT_EQ(_query_1->peak_consumption(), 10);
    ASSERT_EQ(_query_pool_mem_tracker->peak_consumption(), 10);
    ASSERT_EQ(_process_mem_tracker->peak_consumption(), 10);
}

TEST_F(MemTrackerTest, release) {
    _query_1->release(10);
    ASSERT_EQ(_query_1->consumption(), -10);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), -10);
    ASSERT_EQ(_process_mem_tracker->consumption(), -10);

    _query_1->release(-30);
    ASSERT_EQ(_query_1->consumption(), 20);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), 20);
    ASSERT_EQ(_process_mem_tracker->consumption(), 20);

    _query_1->release(0);
    ASSERT_EQ(_query_1->consumption(), 20);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), 20);
    ASSERT_EQ(_process_mem_tracker->consumption(), 20);

    ASSERT_EQ(_query_1->peak_consumption(), 20);
    ASSERT_EQ(_query_pool_mem_tracker->peak_consumption(), 20);
    ASSERT_EQ(_process_mem_tracker->peak_consumption(), 20);
}

TEST_F(MemTrackerTest, consume_without_root) {
    _query_1->consume(10);
    ASSERT_EQ(_query_1->consumption(), 10);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), 10);
    ASSERT_EQ(_process_mem_tracker->consumption(), 10);

    _process_mem_tracker->consume(10);
    ASSERT_EQ(_query_1->consumption(), 10);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), 10);
    ASSERT_EQ(_process_mem_tracker->consumption(), 20);

    _query_1->consume_without_root(5);
    ASSERT_EQ(_query_1->consumption(), 15);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), 15);
    ASSERT_EQ(_process_mem_tracker->consumption(), 20);

    _query_1->consume_without_root(0);
    ASSERT_EQ(_query_1->consumption(), 15);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), 15);
    ASSERT_EQ(_process_mem_tracker->consumption(), 20);

    _process_mem_tracker->consume_without_root(10);
    ASSERT_EQ(_query_1->consumption(), 15);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), 15);
    ASSERT_EQ(_process_mem_tracker->consumption(), 20);

    _query_1->consume_without_root(-5);
    ASSERT_EQ(_query_1->consumption(), 10);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), 10);
    ASSERT_EQ(_process_mem_tracker->consumption(), 20);

    _query_1->release_without_root();
    ASSERT_EQ(_query_1->consumption(), 0);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), 0);
    ASSERT_EQ(_process_mem_tracker->consumption(), 20);
}

TEST_F(MemTrackerTest, release_without_root) {
    _query_1->consume(10);
    ASSERT_EQ(_query_1->consumption(), 10);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), 10);
    ASSERT_EQ(_process_mem_tracker->consumption(), 10);

    _query_1->release_without_root(5);
    ASSERT_EQ(_query_1->consumption(), 5);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), 5);
    ASSERT_EQ(_process_mem_tracker->consumption(), 10);

    _query_1->release_without_root(0);
    ASSERT_EQ(_query_1->consumption(), 5);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), 5);
    ASSERT_EQ(_process_mem_tracker->consumption(), 10);

    _process_mem_tracker->release_without_root(5);
    ASSERT_EQ(_query_1->consumption(), 5);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), 5);
    ASSERT_EQ(_process_mem_tracker->consumption(), 10);

    _query_1->release_without_root(-8);
    ASSERT_EQ(_query_1->consumption(), 13);
    ASSERT_EQ(_query_pool_mem_tracker->consumption(), 13);
    ASSERT_EQ(_process_mem_tracker->consumption(), 10);
}

TEST_F(MemTrackerTest, try_consume_with_limited) {
    ASSERT_EQ(_query_1->try_consume_with_limited(10, 500), nullptr);
    ASSERT_EQ(_query_2->try_consume_with_limited(10, 500), _query_pool_mem_tracker.get());
    ASSERT_EQ(_query_1->try_consume_with_limited(10, 1000), _query_pool_mem_tracker.get());
}

TEST_F(MemTrackerTest, has_enough_reserved_memory) {
    _query_1->consume(100);
    _query_2->consume(50);

    ASSERT_TRUE(_query_1->has_enough_reserved_memory(200));
    ASSERT_FALSE(_query_1->has_enough_reserved_memory(1000));
}

} // namespace starrocks