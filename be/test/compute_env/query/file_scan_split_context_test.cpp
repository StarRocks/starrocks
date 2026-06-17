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

#include "compute_env/query/file_scan_split_context.h"

#include <gtest/gtest.h>

#include "gutil/casts.h"

namespace starrocks {

namespace {

struct TestFileScanSplitContext : public FileScanSplitContext {
    explicit TestFileScanSplitContext(int tag_) : tag(tag_) {}

    FileScanSplitContextPtr clone() override {
        auto ctx = std::make_unique<TestFileScanSplitContext>(tag);
        return ctx;
    }

    int tag = 0;
};

FileScanSplitContextPtr make_split(size_t start_offset, size_t end_offset, int tag) {
    auto ctx = std::make_unique<TestFileScanSplitContext>(tag);
    ctx->start_offset = start_offset;
    ctx->end_offset = end_offset;
    return ctx;
}

const TestFileScanSplitContext* test_split(const FileScanSplitContextPtr& split) {
    return down_cast<const TestFileScanSplitContext*>(split.get());
}

} // namespace

TEST(FileScanSplitContextTest, MergeContiguousSplitsWithinMaxSize) {
    std::vector<FileScanSplitContextPtr> split_tasks;
    split_tasks.emplace_back(make_split(0, 10, 1));
    split_tasks.emplace_back(make_split(10, 20, 2));
    split_tasks.emplace_back(make_split(20, 30, 3));

    merge_file_scan_split_tasks(&split_tasks, 100);

    ASSERT_EQ(1, split_tasks.size());
    EXPECT_EQ(0, split_tasks[0]->start_offset);
    EXPECT_EQ(30, split_tasks[0]->end_offset);
    EXPECT_EQ(1, test_split(split_tasks[0])->tag);
}

TEST(FileScanSplitContextTest, KeepsSplitAtGap) {
    std::vector<FileScanSplitContextPtr> split_tasks;
    split_tasks.emplace_back(make_split(0, 10, 1));
    split_tasks.emplace_back(make_split(15, 25, 2));

    merge_file_scan_split_tasks(&split_tasks, 100);

    ASSERT_EQ(2, split_tasks.size());
    EXPECT_EQ(0, split_tasks[0]->start_offset);
    EXPECT_EQ(10, split_tasks[0]->end_offset);
    EXPECT_EQ(1, test_split(split_tasks[0])->tag);
    EXPECT_EQ(15, split_tasks[1]->start_offset);
    EXPECT_EQ(25, split_tasks[1]->end_offset);
    EXPECT_EQ(2, test_split(split_tasks[1])->tag);
}

TEST(FileScanSplitContextTest, KeepsSplitWhenMergedRangeExceedsMaxSize) {
    std::vector<FileScanSplitContextPtr> split_tasks;
    split_tasks.emplace_back(make_split(0, 10, 1));
    split_tasks.emplace_back(make_split(10, 20, 2));
    split_tasks.emplace_back(make_split(20, 40, 3));

    merge_file_scan_split_tasks(&split_tasks, 25);

    ASSERT_EQ(2, split_tasks.size());
    EXPECT_EQ(0, split_tasks[0]->start_offset);
    EXPECT_EQ(20, split_tasks[0]->end_offset);
    EXPECT_EQ(1, test_split(split_tasks[0])->tag);
    EXPECT_EQ(20, split_tasks[1]->start_offset);
    EXPECT_EQ(40, split_tasks[1]->end_offset);
    EXPECT_EQ(3, test_split(split_tasks[1])->tag);
}

TEST(FileScanSplitContextTest, MergesSmallConsecutiveTailIntoPreviousSplit) {
    std::vector<FileScanSplitContextPtr> split_tasks;
    split_tasks.emplace_back(make_split(0, 30, 1));
    split_tasks.emplace_back(make_split(30, 70, 2));
    split_tasks.emplace_back(make_split(70, 85, 3));

    merge_file_scan_split_tasks(&split_tasks, 50);

    ASSERT_EQ(2, split_tasks.size());
    EXPECT_EQ(0, split_tasks[0]->start_offset);
    EXPECT_EQ(30, split_tasks[0]->end_offset);
    EXPECT_EQ(1, test_split(split_tasks[0])->tag);
    EXPECT_EQ(30, split_tasks[1]->start_offset);
    EXPECT_EQ(85, split_tasks[1]->end_offset);
    EXPECT_EQ(2, test_split(split_tasks[1])->tag);
}

TEST(FileScanSplitContextTest, LeavesNullAndSingleSplitInputsUnchanged) {
    merge_file_scan_split_tasks(nullptr, 50);

    std::vector<FileScanSplitContextPtr> split_tasks;
    split_tasks.emplace_back(make_split(0, 10, 1));

    merge_file_scan_split_tasks(&split_tasks, 50);

    ASSERT_EQ(1, split_tasks.size());
    EXPECT_EQ(0, split_tasks[0]->start_offset);
    EXPECT_EQ(10, split_tasks[0]->end_offset);
    EXPECT_EQ(1, test_split(split_tasks[0])->tag);
}

} // namespace starrocks
