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

#include "storage/olap_common.h"

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
    CompactionTaskContext context{123, 456, 789, false, callback};

    void SetUp() override {
        // Initialize your context or mock callback here if necessary
    }
};

TEST_F(CompactionTaskContextTest, test_constructor) {
    EXPECT_EQ(123, context.txn_id);
    EXPECT_EQ(456, context.tablet_id);
    EXPECT_EQ(789, context.version);
}

TEST_F(CompactionTaskContextTest, test_calculation) {
    CompactionTaskStats stats;

    OlapReaderStatistics reader_stats;
    reader_stats.io_ns_remote = 200;
    reader_stats.io_ns_read_local_disk = 300;
    reader_stats.segment_init_ns = 400;
    reader_stats.column_iterator_init_ns = 500;
    reader_stats.io_count_local_disk = 600;
    reader_stats.io_count_remote = 700;
    reader_stats.compressed_bytes_read_remote = 1024;
    reader_stats.compressed_bytes_read_local_disk = 1024;

    stats.in_queue_time_sec = 5;
    stats.pk_sst_merge_ns = 5;
    stats.collect(reader_stats);

    EXPECT_EQ(stats.io_ns_remote, 200);
    EXPECT_EQ(stats.io_ns_local_disk, 300);
    EXPECT_EQ(stats.segment_init_ns, 400);
    EXPECT_EQ(stats.column_iterator_init_ns, 500);
    EXPECT_EQ(stats.io_count_local_disk, 600);
    EXPECT_EQ(stats.io_count_remote, 700);
    EXPECT_EQ(stats.io_bytes_read_remote, 1024);
    EXPECT_EQ(stats.io_bytes_read_local_disk, 1024);
    EXPECT_EQ(stats.in_queue_time_sec, 5);
    EXPECT_EQ(stats.pk_sst_merge_ns, 5);

    CompactionTaskStats after_add = stats + stats;

    EXPECT_EQ(after_add.io_ns_remote, 400);
    EXPECT_EQ(after_add.io_ns_local_disk, 600);
    EXPECT_EQ(after_add.segment_init_ns, 800);
    EXPECT_EQ(after_add.column_iterator_init_ns, 1000);
    EXPECT_EQ(after_add.io_count_local_disk, 1200);
    EXPECT_EQ(after_add.io_count_remote, 1400);
    EXPECT_EQ(after_add.io_bytes_read_remote, 2048);
    EXPECT_EQ(after_add.io_bytes_read_local_disk, 2048);
    EXPECT_EQ(after_add.in_queue_time_sec, 10);
    EXPECT_EQ(after_add.pk_sst_merge_ns, 10);

    CompactionTaskStats after_minus = stats - stats;

    EXPECT_EQ(after_minus.io_ns_remote, 0);
    EXPECT_EQ(after_minus.io_ns_local_disk, 0);
    EXPECT_EQ(after_minus.segment_init_ns, 0);
    EXPECT_EQ(after_minus.column_iterator_init_ns, 0);
    EXPECT_EQ(after_minus.io_count_local_disk, 0);
    EXPECT_EQ(after_minus.io_count_remote, 0);
    EXPECT_EQ(after_minus.io_bytes_read_remote, 0);
    EXPECT_EQ(after_minus.io_bytes_read_local_disk, 0);
    EXPECT_EQ(after_minus.in_queue_time_sec, 0);
    EXPECT_EQ(after_minus.pk_sst_merge_ns, 0);
}

TEST_F(CompactionTaskContextTest, test_to_json_stats) {
    static constexpr long TIME_UNIT_NS_PER_SECOND = 1000000000;

    // Set up some stats to test the JSON output
    context.stats->io_bytes_read_remote = 1 * 1048576;
    context.stats->io_bytes_read_local_disk = 1 * 1048576;
    context.stats->io_ns_remote = 1 * TIME_UNIT_NS_PER_SECOND;
    context.stats->io_ns_local_disk = 9 * TIME_UNIT_NS_PER_SECOND;
    context.stats->segment_init_ns = 2 * TIME_UNIT_NS_PER_SECOND;
    context.stats->io_count_remote = 3;
    context.stats->io_count_local_disk = 2;
    context.stats->segment_init_ns = 3 * TIME_UNIT_NS_PER_SECOND;
    context.stats->column_iterator_init_ns = 4 * TIME_UNIT_NS_PER_SECOND;
    context.stats->in_queue_time_sec = 5;
    context.stats->pk_sst_merge_ns = 5 * TIME_UNIT_NS_PER_SECOND;

    // Call the method under test
    std::string json_stats = context.stats->to_json_stats();

    // Verify the JSON output
    EXPECT_THAT(json_stats, testing::HasSubstr(R"("read_remote_mb":1)"));
    EXPECT_THAT(json_stats, testing::HasSubstr(R"("read_local_mb":1)"));
    EXPECT_THAT(json_stats, testing::HasSubstr(R"("read_remote_sec":1)"));
    EXPECT_THAT(json_stats, testing::HasSubstr(R"("read_local_sec":9)"));
    EXPECT_THAT(json_stats, testing::HasSubstr(R"("read_remote_count":3)"));
    EXPECT_THAT(json_stats, testing::HasSubstr(R"("read_local_count":2)"));
    EXPECT_THAT(json_stats, testing::HasSubstr(R"("in_queue_sec":5)"));
    EXPECT_THAT(json_stats, testing::HasSubstr(R"("pk_sst_merge_sec":5)"));
}
} // namespace starrocks::lake