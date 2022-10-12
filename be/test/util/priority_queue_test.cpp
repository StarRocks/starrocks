// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
