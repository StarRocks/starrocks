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

#include "util/priority_queue.h"

#include <gtest/gtest.h>

namespace starrocks {

TEST(PriorityTest, test) {
    PriorityQueue<2, std::string> queue;
    ASSERT_TRUE(queue.empty());
    ASSERT_EQ(0, queue.size());

    queue.push_back(0, "a");

    queue.emplace_back(1, "b");

    std::string s0("c");
    queue.push_back(0, std::move(s0));

    std::string s1("d");
    queue.emplace_back(1, std::move(s1));

    ASSERT_FALSE(queue.empty());
    ASSERT_EQ(4, queue.size());

    ASSERT_EQ("b", queue.front());
    queue.pop_front();
    ASSERT_EQ(3, queue.size());

    ASSERT_EQ("d", queue.front());
    queue.pop_front();
    ASSERT_EQ(2, queue.size());

    ASSERT_EQ("a", queue.front());
    queue.pop_front();
    ASSERT_EQ(1, queue.size());

    ASSERT_EQ("c", queue.front());
    queue.pop_front();
    ASSERT_EQ(0, queue.size());
    ASSERT_TRUE(queue.empty());
}

} // namespace starrocks
