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

#include "base/container/heap.h"

#include <gtest/gtest.h>

#include <vector>

namespace starrocks {

// Test basic constructor and comparator functionality
// std::greater creates a min-heap (smallest element at top)
TEST(HeapTest, ConstructorAndComparator) {
    SortingHeap<int, std::vector<int>, std::greater<int>> heap{std::greater<int>()};
    EXPECT_TRUE(heap.empty());
    EXPECT_EQ(heap.size(), 0);
}

// Test push and top operations with min-heap
// std::greater creates a min-heap, so smallest element is at top
TEST(HeapTest, PushAndTop_MinHeap) {
    SortingHeap<int, std::vector<int>, std::greater<int>> heap{std::greater<int>()};
    heap.push(5);
    heap.push(10);
    heap.push(3);

    EXPECT_FALSE(heap.empty());
    EXPECT_EQ(heap.size(), 3);
    EXPECT_EQ(heap.top(), 3); // Smallest element should be at top (min-heap)
}

// Test push and top operations with max-heap
// std::less creates a max-heap, so largest element is at top
TEST(HeapTest, PushAndTop_MaxHeap) {
    SortingHeap<int, std::vector<int>, std::less<int>> heap{std::less<int>()};
    heap.push(5);
    heap.push(10);
    heap.push(3);

    EXPECT_FALSE(heap.empty());
    EXPECT_EQ(heap.size(), 3);
    EXPECT_EQ(heap.top(), 10); // Largest element should be at top (max-heap)
}

// Test push with move semantics
TEST(HeapTest, PushWithMove) {
    SortingHeap<int, std::vector<int>, std::greater<int>> heap{std::greater<int>()};
    int val = 15;
    heap.push(std::move(val));

    EXPECT_EQ(heap.size(), 1);
    EXPECT_EQ(heap.top(), 15);
}

// Test remove_top operation with min-heap
TEST(HeapTest, RemoveTop_MinHeap) {
    SortingHeap<int, std::vector<int>, std::greater<int>> heap{std::greater<int>()};
    heap.push(10);
    heap.push(20);
    heap.push(15);

    EXPECT_EQ(heap.size(), 3);
    EXPECT_EQ(heap.top(), 10); // 10 is the smallest

    heap.remove_top();
    EXPECT_EQ(heap.size(), 2);
    EXPECT_EQ(heap.top(), 15); // Now 15 should be the smallest
}

// Test remove_top operation with max-heap
TEST(HeapTest, RemoveTop_MaxHeap) {
    SortingHeap<int, std::vector<int>, std::less<int>> heap{std::less<int>()};
    heap.push(10);
    heap.push(20);
    heap.push(15);

    EXPECT_EQ(heap.size(), 3);
    EXPECT_EQ(heap.top(), 20); // 20 is the largest

    heap.remove_top();
    EXPECT_EQ(heap.size(), 2);
    EXPECT_EQ(heap.top(), 15); // Now 15 should be the largest
}

// Test remove_top until empty
TEST(HeapTest, RemoveTopUntilEmpty) {
    SortingHeap<int, std::vector<int>, std::greater<int>> heap{std::greater<int>()};
    heap.push(5);
    heap.push(10);
    heap.push(15);

    while (!heap.empty()) {
        heap.remove_top();
    }

    EXPECT_TRUE(heap.empty());
    EXPECT_EQ(heap.size(), 0);
}

// Test replace_top operation with min-heap
TEST(HeapTest, ReplaceTop_MinHeap) {
    SortingHeap<int, std::vector<int>, std::greater<int>> heap{std::greater<int>()};
    heap.push(10);
    heap.push(20);
    heap.push(15);

    EXPECT_EQ(heap.top(), 10);

    // Replace top with a new value
    heap.replace_top(5);
    EXPECT_EQ(heap.top(), 5);
    EXPECT_EQ(heap.size(), 3);
}

// Test replace_top operation with max-heap
TEST(HeapTest, ReplaceTop_MaxHeap) {
    SortingHeap<int, std::vector<int>, std::less<int>> heap{std::less<int>()};
    heap.push(10);
    heap.push(20);
    heap.push(15);

    EXPECT_EQ(heap.top(), 20);

    // Replace top with a new value
    heap.replace_top(25);
    EXPECT_EQ(heap.top(), 25);
    EXPECT_EQ(heap.size(), 3);
}

// Test replace_top with smaller value in max-heap
TEST(HeapTest, ReplaceTopWithSmallerValue) {
    SortingHeap<int, std::vector<int>, std::less<int>> heap{std::less<int>()};
    heap.push(10);
    heap.push(20);

    EXPECT_EQ(heap.top(), 20);

    // Replace top with a smaller value
    heap.replace_top(5);
    EXPECT_EQ(heap.top(), 10); // 10 should now be at top
    EXPECT_EQ(heap.size(), 2);
}

// Test replace_top_if_less in min-heap
// In min-heap, replace_top_if_less replaces top if new val is less than current top
TEST(HeapTest, ReplaceTopIfLess_MinHeap) {
    SortingHeap<int, std::vector<int>, std::greater<int>> heap{std::greater<int>()};
    heap.push(10);
    heap.push(20);

    EXPECT_EQ(heap.top(), 10); // min-heap: 10 is smallest

    // 8 is less than 10, so it should replace
    heap.replace_top_if_less(28);
    EXPECT_EQ(heap.top(), 20);
    EXPECT_EQ(heap.size(), 2);
}

// Test replace_top_if_less when condition is not met
TEST(HeapTest, ReplaceTopIfLess_NotMet) {
    SortingHeap<int, std::vector<int>, std::greater<int>> heap{std::greater<int>()};
    heap.push(10);
    heap.push(20);

    EXPECT_EQ(heap.top(), 10);

    // 15 is not less than 10, so it should not replace
    heap.replace_top_if_less(5);
    EXPECT_EQ(heap.top(), 10); // 10 should still be at top
    EXPECT_EQ(heap.size(), 2);
}

// Test sorted_seq returns elements in sorted order (ascending for min-heap)
TEST(HeapTest, SortedSequence_MinHeap) {
    SortingHeap<int, std::vector<int>, std::greater<int>> heap{std::greater<int>()};
    heap.push(5);
    heap.push(10);
    heap.push(3);
    heap.push(8);

    // Get sorted sequence
    auto sorted = heap.sorted_seq();
    std::vector<int> expected = {10, 8, 5, 3};
    EXPECT_EQ(sorted, expected);
}

// Test sorted_seq returns elements in descending order for max-heap
TEST(HeapTest, SortedSequence_MaxHeap) {
    SortingHeap<int, std::vector<int>, std::less<int>> heap{std::less<int>()};
    heap.push(5);
    heap.push(10);
    heap.push(3);
    heap.push(8);

    // Get sorted sequence
    auto sorted = heap.sorted_seq();
    std::vector<int> expected = {3, 5, 8, 10};
    EXPECT_EQ(sorted, expected);
}

// Test sorted_seq consumes the heap
TEST(HeapTest, SortedSequenceConsumesHeap) {
    SortingHeap<int, std::vector<int>, std::greater<int>> heap{std::greater<int>()};
    heap.push(5);
    heap.push(10);

    auto sorted = heap.sorted_seq();
    EXPECT_TRUE(heap.empty()); // Heap should be empty after sorted_seq
}

// Test container access
TEST(HeapTest, ContainerAccess) {
    SortingHeap<int, std::vector<int>, std::greater<int>> heap{std::greater<int>()};
    heap.push(5);
    heap.push(10);
    heap.push(3);

    std::vector<int>& container = heap.container();
    EXPECT_EQ(container.size(), 3);

    // Container should contain all pushed elements
    EXPECT_EQ(container.size(), heap.size());
}

// Test reserve functionality
TEST(HeapTest, Reserve) {
    SortingHeap<int, std::vector<int>, std::greater<int>> heap{std::greater<int>()};
    heap.reserve(100);

    heap.push(5);
    heap.push(10);

    EXPECT_EQ(heap.size(), 2);
    // Capacity should be at least 100
    EXPECT_GE(heap.container().capacity(), 100);
}

// Test next_child functionality in min-heap
TEST(HeapTest, NextChild_MinHeap) {
    SortingHeap<int, std::vector<int>, std::greater<int>> heap{std::greater<int>()};
    heap.push(10);
    heap.push(20);
    heap.push(15);

    // After pushing 3 elements in min-heap with root 10:
    //     10
    //    /  \
    //  20    15
    // next_child should return the smaller child (15)
    EXPECT_EQ(heap.next_child(), 15);
}

// Test next_child functionality in max-heap
TEST(HeapTest, NextChild_MaxHeap) {
    SortingHeap<int, std::vector<int>, std::less<int>> heap{std::less<int>()};
    heap.push(10);
    heap.push(20);
    heap.push(15);

    // After pushing 3 elements in max-heap with root 20:
    //     20
    //    /  \
    //  10    15
    // next_child should return the larger child (15)
    EXPECT_EQ(heap.next_child(), 15);
}

// Test with single element
TEST(HeapTest, SingleElement) {
    SortingHeap<int, std::vector<int>, std::greater<int>> heap{std::greater<int>()};
    heap.push(42);

    EXPECT_EQ(heap.size(), 1);
    EXPECT_FALSE(heap.empty());
    EXPECT_EQ(heap.top(), 42);
}

// Test with duplicate values
TEST(HeapTest, DuplicateValues) {
    SortingHeap<int, std::vector<int>, std::greater<int>> heap{std::greater<int>()};
    heap.push(10);
    heap.push(10);
    heap.push(10);

    EXPECT_EQ(heap.size(), 3);
    EXPECT_EQ(heap.top(), 10);

    heap.remove_top();
    EXPECT_EQ(heap.size(), 2);
    EXPECT_EQ(heap.top(), 10);
}

// Test with negative values in min-heap
TEST(HeapTest, NegativeValues_MinHeap) {
    SortingHeap<int, std::vector<int>, std::greater<int>> heap{std::greater<int>()};
    heap.push(-5);
    heap.push(-10);
    heap.push(-3);

    EXPECT_EQ(heap.top(), -10); // Smallest (most negative) should be at top
}

// Test with negative values in max-heap
TEST(HeapTest, NegativeValues_MaxHeap) {
    SortingHeap<int, std::vector<int>, std::less<int>> heap{std::less<int>()};
    heap.push(-5);
    heap.push(-10);
    heap.push(-3);

    EXPECT_EQ(heap.top(), -3); // Largest (least negative) should be at top
}

// Test replace_top_if_less with equal values
TEST(HeapTest, ReplaceTopIfLess_EqualValues) {
    SortingHeap<int, std::vector<int>, std::greater<int>> heap{std::greater<int>()};
    heap.push(10);
    heap.push(20);

    EXPECT_EQ(heap.top(), 10);

    // 10 is not less than 10, so it should not replace
    heap.replace_top_if_less(10);
    EXPECT_EQ(heap.top(), 10);
    EXPECT_EQ(heap.size(), 2);
}

// Test large number of elements in min-heap
TEST(HeapTest, LargeNumberOfElements_MinHeap) {
    SortingHeap<int, std::vector<int>, std::greater<int>> heap{std::greater<int>()};

    for (int i = 0; i < 1000; ++i) {
        heap.push(std::move(i));
    }

    EXPECT_EQ(heap.size(), 1000);
    EXPECT_EQ(heap.top(), 0); // Smallest element should be at top

    // Remove all elements and verify ascending order
    int prev = -1;
    while (!heap.empty()) {
        int current = heap.top();
        EXPECT_GE(current, prev); // Should be non-decreasing (ascending)
        prev = current;
        heap.remove_top();
    }
}

// Test large number of elements in max-heap
TEST(HeapTest, LargeNumberOfElements_MaxHeap) {
    SortingHeap<int, std::vector<int>, std::less<int>> heap{std::less<int>()};

    for (int i = 0; i < 1000; ++i) {
        heap.push(std::move(i));
    }

    EXPECT_EQ(heap.size(), 1000);
    EXPECT_EQ(heap.top(), 999); // Largest element should be at top

    // Remove all elements and verify descending order
    int prev = 1000;
    while (!heap.empty()) {
        int current = heap.top();
        EXPECT_LE(current, prev); // Should be non-increasing (descending)
        prev = current;
        heap.remove_top();
    }
}

// Test sorted_seq with descending input in min-heap
TEST(HeapTest, SortedSequenceDescendingInput) {
    SortingHeap<int, std::vector<int>, std::greater<int>> heap{std::greater<int>()};

    // Push in descending order
    for (int i = 10; i >= 1; --i) {
        heap.push(std::move(i));
    }

    auto sorted = heap.sorted_seq();
    std::vector<int> expected = {10, 9, 8, 7, 6, 5, 4, 3, 2, 1};
    EXPECT_EQ(sorted, expected);
}

// Test sorted_seq with ascending input in min-heap
TEST(HeapTest, SortedSequenceAscendingInput) {
    SortingHeap<int, std::vector<int>, std::greater<int>> heap{std::greater<int>()};

    // Push in ascending order
    for (int i = 1; i <= 10; ++i) {
        heap.push(std::move(i));
    }

    auto sorted = heap.sorted_seq();
    std::vector<int> expected = {10, 9, 8, 7, 6, 5, 4, 3, 2, 1};
    EXPECT_EQ(sorted, expected);
}

// Test multiple push and remove operations interleaved in min-heap
TEST(HeapTest, InterleavedPushRemove) {
    SortingHeap<int, std::vector<int>, std::greater<int>> heap{std::greater<int>()};

    heap.push(5);
    heap.push(15);
    EXPECT_EQ(heap.top(), 5); // min-heap: 5 is smallest

    heap.push(10);
    EXPECT_EQ(heap.top(), 5); // still 5

    heap.remove_top();
    EXPECT_EQ(heap.top(), 10); // now 10 is smallest

    heap.push(20);
    EXPECT_EQ(heap.top(), 10); // still 10

    heap.remove_top();
    EXPECT_EQ(heap.top(), 15); // now 15
}

// Test replace_top followed by push in max-heap
TEST(HeapTest, ReplaceTopThenPush_MaxHeap) {
    SortingHeap<int, std::vector<int>, std::less<int>> heap{std::less<int>()};
    heap.push(10);
    heap.push(20);
    heap.push(30);

    EXPECT_EQ(heap.top(), 30);

    heap.replace_top(25);
    EXPECT_EQ(heap.top(), 25);

    heap.push(35);
    EXPECT_EQ(heap.top(), 35);
}

// Test with string type using std::greater (lexicographical min-heap)
TEST(HeapTest, StringType_MinHeap) {
    SortingHeap<std::string, std::vector<std::string>, std::greater<std::string>> heap{std::greater<std::string>()};
    heap.push("apple");
    heap.push("banana");
    heap.push("cherry");

    EXPECT_EQ(heap.top(), "apple"); // Lexicographically smallest

    heap.remove_top();
    EXPECT_EQ(heap.top(), "banana");
}

// Test with string type using std::less (lexicographical max-heap)
TEST(HeapTest, StringType_MaxHeap) {
    SortingHeap<std::string, std::vector<std::string>, std::less<std::string>> heap{std::less<std::string>()};
    heap.push("apple");
    heap.push("banana");
    heap.push("cherry");

    EXPECT_EQ(heap.top(), "cherry"); // Lexicographically largest

    heap.remove_top();
    EXPECT_EQ(heap.top(), "banana");
}

// Test min-heap (std::greater)
TEST(HeapTest, MinHeap) {
    SortingHeap<int, std::vector<int>, std::greater<int>> heap{std::greater<int>()};
    heap.push(5);
    heap.push(10);
    heap.push(3);

    EXPECT_EQ(heap.top(), 3); // Smallest element at top

    heap.remove_top();
    EXPECT_EQ(heap.top(), 5);
}

// Test max-heap (std::less)
TEST(HeapTest, MaxHeap) {
    SortingHeap<int, std::vector<int>, std::less<int>> heap{std::less<int>()};
    heap.push(5);
    heap.push(10);
    heap.push(3);

    EXPECT_EQ(heap.top(), 10); // Largest element at top

    heap.remove_top();
    EXPECT_EQ(heap.top(), 5);
}

} // namespace starrocks
