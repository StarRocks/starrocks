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

#include "base/time/time.h"
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
    CompactionTaskContext context{123, 456, 789, false, false, callback};

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
    reader_stats.create_segment_iter_ns = 101;
    reader_stats.decompress_ns = 102;
    reader_stats.block_load_ns = 103;
    reader_stats.block_fetch_ns = 104;
    reader_stats.block_seek_ns = 105;
    reader_stats.block_seek_num = 106;
    reader_stats.decode_dict_ns = 107;
    reader_stats.get_rowsets_ns = 108;
    reader_stats.get_delvec_ns = 109;
    reader_stats.get_delta_column_group_ns = 110;
    reader_stats.del_filter_ns = 111;
    reader_stats.blocks_load = 112;
    reader_stats.raw_rows_read = 113;
    reader_stats.compressed_bytes_read = 114;
    reader_stats.uncompressed_bytes_read = 115;

    stats.in_queue_time_sec = 5;
    stats.pk_sst_merge_ns = 5;
    stats.collect(reader_stats);

    EXPECT_EQ(stats.io_ns_read_remote, 200);
    EXPECT_EQ(stats.io_ns_read_local_disk, 300);
    EXPECT_EQ(stats.segment_init_ns, 400);
    EXPECT_EQ(stats.column_iterator_init_ns, 500);
    EXPECT_EQ(stats.io_count_local_disk, 600);
    EXPECT_EQ(stats.io_count_remote, 700);
    EXPECT_EQ(stats.io_bytes_read_remote, 1024);
    EXPECT_EQ(stats.io_bytes_read_local_disk, 1024);
    EXPECT_EQ(stats.create_segment_iter_ns, 101);
    EXPECT_EQ(stats.decompress_ns, 102);
    EXPECT_EQ(stats.block_load_ns, 103);
    EXPECT_EQ(stats.block_fetch_ns, 104);
    EXPECT_EQ(stats.block_seek_ns, 105);
    EXPECT_EQ(stats.block_seek_count, 106);
    EXPECT_EQ(stats.decode_dict_ns, 107);
    EXPECT_EQ(stats.get_rowsets_ns, 108);
    EXPECT_EQ(stats.get_delvec_ns, 109);
    EXPECT_EQ(stats.get_delta_column_group_ns, 110);
    EXPECT_EQ(stats.del_filter_ns, 111);
    EXPECT_EQ(stats.blocks_load, 112);
    EXPECT_EQ(stats.raw_rows_read, 113);
    EXPECT_EQ(stats.compressed_bytes_read, 114);
    EXPECT_EQ(stats.uncompressed_bytes_read, 115);
    EXPECT_EQ(stats.in_queue_time_sec, 5);
    EXPECT_EQ(stats.pk_sst_merge_ns, 5);

    CompactionTaskStats after_add = stats + stats;

    EXPECT_EQ(after_add.io_ns_read_remote, 400);
    EXPECT_EQ(after_add.io_ns_read_local_disk, 600);
    EXPECT_EQ(after_add.segment_init_ns, 800);
    EXPECT_EQ(after_add.column_iterator_init_ns, 1000);
    EXPECT_EQ(after_add.io_count_local_disk, 1200);
    EXPECT_EQ(after_add.io_count_remote, 1400);
    EXPECT_EQ(after_add.io_bytes_read_remote, 2048);
    EXPECT_EQ(after_add.io_bytes_read_local_disk, 2048);
    EXPECT_EQ(after_add.in_queue_time_sec, 10);
    EXPECT_EQ(after_add.pk_sst_merge_ns, 10);

    CompactionTaskStats after_minus = stats - stats;

    EXPECT_EQ(after_minus.io_ns_read_remote, 0);
    EXPECT_EQ(after_minus.io_ns_read_local_disk, 0);
    EXPECT_EQ(after_minus.segment_init_ns, 0);
    EXPECT_EQ(after_minus.column_iterator_init_ns, 0);
    EXPECT_EQ(after_minus.io_count_local_disk, 0);
    EXPECT_EQ(after_minus.io_count_remote, 0);
    EXPECT_EQ(after_minus.io_bytes_read_remote, 0);
    EXPECT_EQ(after_minus.io_bytes_read_local_disk, 0);
    EXPECT_EQ(after_minus.in_queue_time_sec, 0);
    EXPECT_EQ(after_minus.pk_sst_merge_ns, 0);

    OlapWriterStatistics writer_stats;
    writer_stats.write_remote_ns = 10;
    writer_stats.bytes_write_remote = 100;
    writer_stats.segment_count = 1000;
    stats.collect(writer_stats);
    EXPECT_EQ(stats.io_ns_write_remote, 10);
    EXPECT_EQ(stats.write_segment_bytes, 100);
    EXPECT_EQ(stats.write_segment_count, 1000);
}

TEST_F(CompactionTaskContextTest, test_task_timing_accounting) {
    CompactionTaskStats stats;
    stats.compaction_type = "horizontal";
    stats.task_attempt_count = 1;
    stats.task_prepare_ns = 10;
    stats.input_prepare_ns = 20;
    stats.reader_get_next_ns = 30;
    stats.writer_write_ns = 40;
    stats.pk_sst_merge_ns = 50;
    stats.task_execute_ns = 190;
    stats.task_total_ns = 200;

    // Nested reader metrics explain reader_get_next_ns but do not participate
    // in top-level wall-time accounting.
    stats.io_ns_read_remote = 1000;
    stats.block_load_ns = 500;

    EXPECT_EQ(150, stats.task_accounted_ns());
    EXPECT_EQ(50, stats.task_unaccounted_ns());

    auto combined = stats + stats;
    EXPECT_EQ("horizontal", combined.compaction_type);
    EXPECT_EQ(2, combined.task_attempt_count);
    EXPECT_EQ(400, combined.task_total_ns);
    EXPECT_EQ(300, combined.task_accounted_ns());
    EXPECT_EQ(100, combined.task_unaccounted_ns());

    CompactionTaskStats vertical;
    vertical.compaction_type = "vertical";
    EXPECT_EQ("mixed", (stats + vertical).compaction_type);

    std::string json_stats = stats.to_json_stats();
    EXPECT_THAT(json_stats, testing::HasSubstr(R"("profile_version":1)"));
    EXPECT_THAT(json_stats, testing::HasSubstr(R"("profile_final":true)"));
    EXPECT_THAT(json_stats, testing::HasSubstr(R"("compaction_type":"horizontal")"));
    EXPECT_THAT(json_stats, testing::HasSubstr(R"("task_total_ns":200)"));
    EXPECT_THAT(json_stats, testing::HasSubstr(R"("task_accounted_ns":150)"));
    EXPECT_THAT(json_stats, testing::HasSubstr(R"("task_unaccounted_ns":50)"));
    EXPECT_THAT(json_stats, testing::HasSubstr(R"("read_remote_ns":1000)"));
}

TEST_F(CompactionTaskContextTest, test_slow_log_threshold) {
    CompactionTaskStats stats;

    stats.task_total_ns = 4'999'999'999;
    EXPECT_FALSE(stats.is_slow(5000));

    stats.task_total_ns = 5'000'000'000;
    EXPECT_TRUE(stats.is_slow(5000));
    EXPECT_TRUE(stats.is_slow(0));
}

TEST_F(CompactionTaskContextTest, test_per_attempt_stats_reset_for_retry) {
    context.stats->compaction_type = "horizontal";
    context.stats->task_attempt_count = 1;
    context.stats->task_total_ns = 6'000'000'000;
    context.stats->raw_rows_read = 100;
    context.stats->write_segment_bytes = 200;
    EXPECT_TRUE(context.stats->is_slow(5000));

    context.reset_attempt_stats();
    EXPECT_EQ(0, context.stats->task_attempt_count);
    EXPECT_EQ(0, context.stats->task_total_ns);
    EXPECT_EQ(0, context.stats->raw_rows_read);
    EXPECT_EQ(0, context.stats->write_segment_bytes);

    context.stats->compaction_type = "horizontal";
    context.stats->task_attempt_count = 2;
    context.stats->task_total_ns = 3'000'000'000;
    context.stats->raw_rows_read = 50;
    context.stats->write_segment_bytes = 75;
    EXPECT_FALSE(context.stats->is_slow(5000));
    context.publish_stats_snapshot();

    auto latest_attempt = context.stats_snapshot(false);
    EXPECT_EQ(2, latest_attempt.task_attempt_count);
    EXPECT_EQ(3'000'000'000, latest_attempt.task_total_ns);
    EXPECT_EQ(50, latest_attempt.raw_rows_read);
    EXPECT_EQ(75, latest_attempt.write_segment_bytes);
}

TEST_F(CompactionTaskContextTest, test_to_json_stats) {
    static constexpr long TIME_UNIT_NS_PER_SECOND = 1000000000;

    // Set up some stats to test the JSON output
    context.stats->io_bytes_read_remote = 1 * 1048576;
    context.stats->io_bytes_read_local_disk = 1 * 1048576;
    context.stats->io_ns_read_remote = 1 * TIME_UNIT_NS_PER_SECOND;
    context.stats->io_ns_read_local_disk = 9 * TIME_UNIT_NS_PER_SECOND;
    context.stats->segment_init_ns = 2 * TIME_UNIT_NS_PER_SECOND;
    context.stats->io_count_remote = 3;
    context.stats->io_count_local_disk = 2;
    context.stats->segment_init_ns = 3 * TIME_UNIT_NS_PER_SECOND;
    context.stats->column_iterator_init_ns = 4 * TIME_UNIT_NS_PER_SECOND;
    context.stats->write_segment_count = 2;
    context.stats->write_segment_bytes = 1 * 1048576;
    context.stats->io_ns_write_remote = 3 * TIME_UNIT_NS_PER_SECOND;
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
    EXPECT_THAT(json_stats, testing::HasSubstr(R"("write_segment_count":2)"));
    EXPECT_THAT(json_stats, testing::HasSubstr(R"("write_remote_mb":1)"));
    EXPECT_THAT(json_stats, testing::HasSubstr(R"("write_remote_sec":3)"));
    EXPECT_THAT(json_stats, testing::HasSubstr(R"("in_queue_sec":5)"));
    EXPECT_THAT(json_stats, testing::HasSubstr(R"("pk_sst_merge_sec":5)"));
}

TEST_F(CompactionTaskContextTest, test_to_json_stats_with_subtask_metadata) {
    static constexpr long TIME_UNIT_NS_PER_SECOND = 1000000000;

    context.stats->io_bytes_read_remote = 2 * 1048576;
    context.stats->io_ns_read_remote = 4 * TIME_UNIT_NS_PER_SECOND;
    context.stats->write_segment_count = 7;
    context.stats->in_queue_time_sec = 11;

    std::string json_profile = context.stats->to_json_stats_with_subtask_metadata(
            /*subtask_id=*/3, /*input_rowsets=*/5);

    // Stats fields must still be present (this is the regression that was being lost).
    EXPECT_THAT(json_profile, testing::HasSubstr(R"("read_remote_mb":2)"));
    EXPECT_THAT(json_profile, testing::HasSubstr(R"("read_remote_sec":4)"));
    EXPECT_THAT(json_profile, testing::HasSubstr(R"("write_segment_count":7)"));
    EXPECT_THAT(json_profile, testing::HasSubstr(R"("in_queue_sec":11)"));

    // Subtask metadata must be appended alongside the stats. The planned input_bytes
    // is intentionally omitted because the actual read volume is already reported via
    // read_local_mb / read_remote_mb in the stats fields above.
    EXPECT_THAT(json_profile, testing::HasSubstr(R"("subtask_id":3)"));
    EXPECT_THAT(json_profile, testing::HasSubstr(R"("input_rowsets":5)"));
    EXPECT_THAT(json_profile, testing::Not(testing::HasSubstr(R"("input_bytes")")));
    EXPECT_THAT(json_profile, testing::HasSubstr(R"("is_parallel_subtask":true)"));
}

TEST_F(CompactionTaskContextTest, test_live_stats_snapshot) {
    context.stats->task_execute_ns = 100;
    context.stats->task_total_ns = 200;

    const int64_t now_ns = MonotonicNanos();
    context.task_attempt_start_ns.store(now_ns - 5'000'000, std::memory_order_release);
    context.task_execute_start_ns.store(now_ns - 3'000'000, std::memory_order_release);
    context.publish_stats_snapshot();

    auto live_stats = context.stats_snapshot(true);
    EXPECT_GE(live_stats.task_total_ns, 5'000'200);
    EXPECT_GE(live_stats.task_execute_ns, 3'000'100);
    EXPECT_THAT(live_stats.to_json_stats(false), testing::HasSubstr(R"("profile_final":false)"));

    auto final_stats = context.stats_snapshot(false);
    EXPECT_EQ(final_stats.task_total_ns, 200);
    EXPECT_EQ(final_stats.task_execute_ns, 100);
}

TEST_F(CompactionTaskContextTest, test_running_stats_use_latest_published_attempt) {
    context.stats->compaction_type = "horizontal";
    context.stats->task_attempt_count = 1;
    context.stats->raw_rows_read = 100;
    context.publish_stats_snapshot();

    context.reset_attempt_stats();
    auto previous_attempt = context.stats_snapshot(true);
    EXPECT_EQ("horizontal", previous_attempt.compaction_type);
    EXPECT_EQ(1, previous_attempt.task_attempt_count);
    EXPECT_EQ(100, previous_attempt.raw_rows_read);

    context.stats->compaction_type = "vertical";
    context.stats->task_attempt_count = 2;
    context.stats->raw_rows_read = 50;
    context.publish_stats_snapshot();

    auto latest_attempt = context.stats_snapshot(true);
    EXPECT_EQ("vertical", latest_attempt.compaction_type);
    EXPECT_EQ(2, latest_attempt.task_attempt_count);
    EXPECT_EQ(50, latest_attempt.raw_rows_read);
}
} // namespace starrocks::lake
