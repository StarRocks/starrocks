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

#include "storage/lake/compaction_task_context.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace starrocks::lake {

class LakeCompactionTaskProgressTest : public testing::Test {
protected:
    Progress progress;
};

TEST_F(LakeCompactionTaskProgressTest, ValueInitiallyZero) {
    EXPECT_EQ(0, progress.value());
}

TEST_F(LakeCompactionTaskProgressTest, UpdateValue) {
    progress.update(42);
    EXPECT_EQ(42, progress.value());
}

class CompactionTaskContextTest : public testing::Test {
public:
    CompactionTaskContextTest() = default;
    ~CompactionTaskContextTest() override = default;

protected:
    // Implement a mock version of CompactionTaskCallback if needed
    std::shared_ptr<CompactionTaskCallback> callback;
    CompactionTaskContext context{123, 456, 789, callback};

    void SetUp() override {
        // Initialize your context or mock callback here if necessary
    }
};

TEST_F(CompactionTaskContextTest, test_constructor) {
    EXPECT_EQ(123, context.txn_id);
    EXPECT_EQ(456, context.tablet_id);
    EXPECT_EQ(789, context.version);
}

TEST_F(CompactionTaskContextTest, test_to_json_stats) {
    // Set up some stats to test the JSON output
    context.stats->reader_time_ns = 1000000;
    context.stats->io_ns = 2000000;
    context.stats->segment_init_ns = 2000000;
    context.stats->io_count_remote = 3;
    context.stats->io_count_local_disk = 2;
    context.stats->compressed_bytes_read = 1024;
    context.stats->segment_init_ns = 3000000;
    context.stats->column_iterator_init_ns = 4000000;
    context.stats->segment_write_ns = 5000000;

    // Call the method under test
    std::string json_stats = context.to_json_stats();

    // Verify the JSON output
    EXPECT_THAT(json_stats, testing::HasSubstr(R"("reader_total_time_ms":1)"));
    EXPECT_THAT(json_stats, testing::HasSubstr(R"("reader_io_ms":2)"));
    EXPECT_THAT(json_stats, testing::HasSubstr(R"("reader_io_count_remote":3)"));
    EXPECT_THAT(json_stats, testing::HasSubstr(R"("segment_write_ms":5)"));
}
} // namespace starrocks::lake