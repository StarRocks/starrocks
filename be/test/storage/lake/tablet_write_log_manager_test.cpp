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

#include "storage/lake/tablet_write_log_manager.h"

#include <gtest/gtest.h>

#include "common/config_storage_fwd.h"
#include "fs/fs.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/lake/compaction_task.h"
#include "storage/lake/lake_persistent_index.h"
#include "storage/lake/lake_primary_index.h"

namespace starrocks::lake {

class TabletWriteLogManagerTest : public testing::Test {
public:
    void SetUp() override {
        // Clear logs by setting a very future timestamp
        TabletWriteLogManager::instance()->cleanup_old_logs(std::numeric_limits<int64_t>::max());
    }
    void TearDown() override {
        TabletWriteLogManager::instance()->cleanup_old_logs(std::numeric_limits<int64_t>::max());
    }
};

TEST_F(TabletWriteLogManagerTest, test_add_and_get_logs) {
    auto mgr = TabletWriteLogManager::instance();
    mgr->add_load_log(1, 100, 200, 300, 400, 10, 1000, 10, 2000, 5, "label1", 10000, 20000, 2, 8192);

    auto logs = mgr->get_logs();
    ASSERT_EQ(1, logs.size());
    auto& log = logs[0];
    EXPECT_EQ(1, log.backend_id);
    EXPECT_EQ(100, log.txn_id);
    EXPECT_EQ(lake::LogType::LOAD, log.log_type);
    EXPECT_EQ("label1", log.label);
    EXPECT_EQ(10, log.input_rows);
    EXPECT_EQ(1000, log.input_bytes);
    EXPECT_EQ(0, log.input_segments);
    EXPECT_EQ(5, log.output_segments);
    EXPECT_EQ(0, log.sst_input_files);
    EXPECT_EQ(0, log.sst_input_bytes);
    EXPECT_EQ(2, log.sst_output_files);
    EXPECT_EQ(8192, log.sst_output_bytes);
}

TEST_F(TabletWriteLogManagerTest, test_compaction_sst_stats) {
    auto mgr = TabletWriteLogManager::instance();
    mgr->add_compaction_log(1, 200, 30, 10, 20, 100, 5000, 80, 4000, 10, 3, 80, "base", 10000, 20000, 5, 20480, 2,
                            8192);

    auto logs = mgr->get_logs();
    ASSERT_EQ(1, logs.size());
    auto& log = logs[0];
    EXPECT_EQ(1, log.backend_id);
    EXPECT_EQ(200, log.txn_id);
    EXPECT_EQ(30, log.tablet_id);
    EXPECT_EQ(10, log.table_id);
    EXPECT_EQ(20, log.partition_id);
    EXPECT_EQ(lake::LogType::COMPACTION, log.log_type);
    EXPECT_EQ(100, log.input_rows);
    EXPECT_EQ(5000, log.input_bytes);
    EXPECT_EQ(80, log.output_rows);
    EXPECT_EQ(4000, log.output_bytes);
    EXPECT_EQ(10, log.input_segments);
    EXPECT_EQ(3, log.output_segments);
    EXPECT_EQ(80, log.compaction_score);
    EXPECT_EQ("base", log.compaction_type);
    EXPECT_EQ(10000, log.begin_time);
    EXPECT_EQ(20000, log.finish_time);
    EXPECT_EQ(5, log.sst_input_files);
    EXPECT_EQ(20480, log.sst_input_bytes);
    EXPECT_EQ(2, log.sst_output_files);
    EXPECT_EQ(8192, log.sst_output_bytes);
}

TEST_F(TabletWriteLogManagerTest, test_load_without_sst_stats) {
    auto mgr = TabletWriteLogManager::instance();
    // Call without SST params (default to 0)
    mgr->add_load_log(1, 100, 200, 300, 400, 10, 1000, 10, 2000, 5, "label_no_sst", 10000, 20000);

    auto logs = mgr->get_logs();
    ASSERT_EQ(1, logs.size());
    auto& log = logs[0];
    EXPECT_EQ(lake::LogType::LOAD, log.log_type);
    EXPECT_EQ("label_no_sst", log.label);
    EXPECT_EQ(0, log.sst_input_files);
    EXPECT_EQ(0, log.sst_input_bytes);
    EXPECT_EQ(0, log.sst_output_files);
    EXPECT_EQ(0, log.sst_output_bytes);
}

TEST_F(TabletWriteLogManagerTest, test_compaction_without_sst_stats) {
    auto mgr = TabletWriteLogManager::instance();
    // Call without SST params (default to 0)
    mgr->add_compaction_log(1, 200, 30, 10, 20, 100, 5000, 80, 4000, 10, 3, 80, "cumulative", 10000, 20000);

    auto logs = mgr->get_logs();
    ASSERT_EQ(1, logs.size());
    auto& log = logs[0];
    EXPECT_EQ(lake::LogType::COMPACTION, log.log_type);
    EXPECT_EQ("cumulative", log.compaction_type);
    EXPECT_EQ(0, log.sst_input_files);
    EXPECT_EQ(0, log.sst_input_bytes);
    EXPECT_EQ(0, log.sst_output_files);
    EXPECT_EQ(0, log.sst_output_bytes);
}

TEST_F(TabletWriteLogManagerTest, test_buffer_overflow) {
    auto mgr = TabletWriteLogManager::instance();
    int32_t original_size = config::tablet_write_log_buffer_size;
    config::tablet_write_log_buffer_size = 5;

    for (int i = 0; i < 10; ++i) {
        mgr->add_load_log(1, i, 200, 300, 400, 10, 1000, 10, 2000, 5, "label", 10000 + i, 20000 + i);
    }

    ASSERT_EQ(5, mgr->size());
    auto logs = mgr->get_logs();
    ASSERT_EQ(5, logs.size());
    // Should contain the last 5 logs (5, 6, 7, 8, 9)
    EXPECT_EQ(5, logs[0].txn_id);
    EXPECT_EQ(9, logs[4].txn_id);

    config::tablet_write_log_buffer_size = original_size;
}

TEST_F(TabletWriteLogManagerTest, test_cleanup_old_logs) {
    auto mgr = TabletWriteLogManager::instance();
    mgr->add_load_log(1, 1, 200, 300, 400, 10, 1000, 10, 2000, 5, "label1", 10000, 20000); // finish 20000
    mgr->add_load_log(1, 2, 200, 300, 400, 10, 1000, 10, 2000, 5, "label2", 30000, 40000); // finish 40000

    mgr->cleanup_old_logs(30000); // Remove logs finished before 30000
    ASSERT_EQ(1, mgr->size());
    auto logs = mgr->get_logs();
    EXPECT_EQ(2, logs[0].txn_id);
}

TEST_F(TabletWriteLogManagerTest, test_filters) {
    auto mgr = TabletWriteLogManager::instance();
    // Log 1: Table 10, Partition 20, Tablet 30, LOAD
    mgr->add_load_log(1, 1, 30, 10, 20, 10, 1000, 10, 2000, 5, "label1", 10000, 20000);
    // Log 2: Table 11, Partition 21, Tablet 31, COMPACTION
    mgr->add_compaction_log(1, 2, 31, 11, 21, 10, 1000, 10, 2000, 5, 5, 100, "base", 30000, 40000);

    // Filter by table_id
    auto logs = mgr->get_logs(10);
    ASSERT_EQ(1, logs.size());
    EXPECT_EQ(1, logs[0].txn_id);

    // Filter by partition_id
    logs = mgr->get_logs(0, 21);
    ASSERT_EQ(1, logs.size());
    EXPECT_EQ(2, logs[0].txn_id);

    // Filter by tablet_id
    logs = mgr->get_logs(0, 0, 30);
    ASSERT_EQ(1, logs.size());
    EXPECT_EQ(1, logs[0].txn_id);

    // Filter by log_type
    logs = mgr->get_logs(0, 0, 0, (int64_t)lake::LogType::COMPACTION);
    ASSERT_EQ(1, logs.size());
    EXPECT_EQ(2, logs[0].txn_id);

    // Filter by time range
    logs = mgr->get_logs(0, 0, 0, 0, 30000); // start_finish_time = 30000
    ASSERT_EQ(1, logs.size());
    EXPECT_EQ(2, logs[0].txn_id);

    logs = mgr->get_logs(0, 0, 0, 0, 0, 30000); // end_finish_time = 30000
    ASSERT_EQ(1, logs.size());
    EXPECT_EQ(1, logs[0].txn_id);
}

TEST_F(TabletWriteLogManagerTest, test_publish_log) {
    auto mgr = TabletWriteLogManager::instance();
    mgr->add_publish_log(1, 300, 30, 10, 20, 10000, 20000, 3, 12288);

    auto logs = mgr->get_logs();
    ASSERT_EQ(1, logs.size());
    auto& log = logs[0];
    EXPECT_EQ(lake::LogType::PUBLISH, log.log_type);
    EXPECT_EQ(1, log.backend_id);
    EXPECT_EQ(300, log.txn_id);
    EXPECT_EQ(30, log.tablet_id);
    EXPECT_EQ(10, log.table_id);
    EXPECT_EQ(20, log.partition_id);
    EXPECT_EQ(10000, log.begin_time);
    EXPECT_EQ(20000, log.finish_time);
    EXPECT_EQ(0, log.input_rows);
    EXPECT_EQ(0, log.input_bytes);
    EXPECT_EQ(0, log.output_rows);
    EXPECT_EQ(0, log.output_bytes);
    EXPECT_EQ(0, log.input_segments);
    EXPECT_EQ(0, log.output_segments);
    EXPECT_EQ(0, log.sst_input_files);
    EXPECT_EQ(0, log.sst_input_bytes);
    EXPECT_EQ(3, log.sst_output_files);
    EXPECT_EQ(12288, log.sst_output_bytes);
}

TEST_F(TabletWriteLogManagerTest, test_filter_by_publish_log_type) {
    auto mgr = TabletWriteLogManager::instance();
    mgr->add_load_log(1, 1, 30, 10, 20, 10, 1000, 10, 2000, 5, "label1", 10000, 20000, 1, 4096);
    mgr->add_compaction_log(1, 2, 31, 11, 21, 10, 1000, 10, 2000, 5, 5, 100, "base", 30000, 40000, 2, 8192, 1, 4096);
    mgr->add_publish_log(1, 3, 32, 12, 22, 50000, 60000, 3, 12288);

    // Filter PUBLISH only
    auto logs = mgr->get_logs(0, 0, 0, (int64_t)lake::LogType::PUBLISH);
    ASSERT_EQ(1, logs.size());
    EXPECT_EQ(3, logs[0].txn_id);
    EXPECT_EQ(lake::LogType::PUBLISH, logs[0].log_type);

    // Filter LOAD only
    logs = mgr->get_logs(0, 0, 0, (int64_t)lake::LogType::LOAD);
    ASSERT_EQ(1, logs.size());
    EXPECT_EQ(1, logs[0].txn_id);

    // No filter - returns all 3
    logs = mgr->get_logs();
    ASSERT_EQ(3, logs.size());
}

TEST_F(TabletWriteLogManagerTest, test_combined_filters) {
    auto mgr = TabletWriteLogManager::instance();
    // Same table, different partitions and types
    mgr->add_load_log(1, 1, 30, 10, 20, 10, 1000, 10, 2000, 5, "label1", 10000, 20000);
    mgr->add_load_log(1, 2, 31, 10, 21, 10, 1000, 10, 2000, 5, "label2", 30000, 40000);
    mgr->add_compaction_log(1, 3, 30, 10, 20, 10, 1000, 10, 2000, 5, 5, 100, "base", 50000, 60000);

    // Filter by table_id AND log_type
    auto logs = mgr->get_logs(10, 0, 0, (int64_t)lake::LogType::LOAD);
    ASSERT_EQ(2, logs.size());

    // Filter by table_id AND partition_id AND log_type
    logs = mgr->get_logs(10, 20, 0, (int64_t)lake::LogType::LOAD);
    ASSERT_EQ(1, logs.size());
    EXPECT_EQ(1, logs[0].txn_id);

    // Filter by tablet_id AND time range
    logs = mgr->get_logs(0, 0, 30, 0, 30000); // tablet 30, finish_time >= 30000
    ASSERT_EQ(1, logs.size());
    EXPECT_EQ(3, logs[0].txn_id);
}

TEST_F(TabletWriteLogManagerTest, test_empty_buffer) {
    auto mgr = TabletWriteLogManager::instance();
    EXPECT_EQ(0, mgr->size());
    auto logs = mgr->get_logs();
    EXPECT_TRUE(logs.empty());

    // Cleanup on empty buffer should not crash
    mgr->cleanup_old_logs(std::numeric_limits<int64_t>::max());
    EXPECT_EQ(0, mgr->size());
}

TEST_F(TabletWriteLogManagerTest, test_entry_default_values) {
    TabletWriteLogEntry entry;
    EXPECT_EQ(0, entry.begin_time);
    EXPECT_EQ(0, entry.finish_time);
    EXPECT_EQ(0, entry.backend_id);
    EXPECT_EQ(0, entry.txn_id);
    EXPECT_EQ(0, entry.tablet_id);
    EXPECT_EQ(0, entry.table_id);
    EXPECT_EQ(0, entry.partition_id);
    EXPECT_EQ(LogType::LOAD, entry.log_type);
    EXPECT_EQ(0, entry.input_rows);
    EXPECT_EQ(0, entry.input_bytes);
    EXPECT_EQ(0, entry.output_rows);
    EXPECT_EQ(0, entry.output_bytes);
    EXPECT_EQ(0, entry.input_segments);
    EXPECT_EQ(0, entry.output_segments);
    EXPECT_TRUE(entry.label.empty());
    EXPECT_EQ(0, entry.compaction_score);
    EXPECT_TRUE(entry.compaction_type.empty());
    EXPECT_EQ(0, entry.sst_input_files);
    EXPECT_EQ(0, entry.sst_input_bytes);
    EXPECT_EQ(0, entry.sst_output_files);
    EXPECT_EQ(0, entry.sst_output_bytes);
}

TEST_F(TabletWriteLogManagerTest, test_publish_log_fields_zeroed) {
    auto mgr = TabletWriteLogManager::instance();
    mgr->add_publish_log(1, 100, 30, 10, 20, 5000, 6000, 5, 40960);

    auto logs = mgr->get_logs();
    ASSERT_EQ(1, logs.size());
    auto& log = logs[0];
    // Publish logs should have zero values for data fields
    EXPECT_EQ(0, log.input_rows);
    EXPECT_EQ(0, log.input_bytes);
    EXPECT_EQ(0, log.output_rows);
    EXPECT_EQ(0, log.output_bytes);
    EXPECT_EQ(0, log.input_segments);
    EXPECT_EQ(0, log.output_segments);
    EXPECT_TRUE(log.label.empty());
    EXPECT_EQ(0, log.compaction_score);
    EXPECT_TRUE(log.compaction_type.empty());
    EXPECT_EQ(0, log.sst_input_files);
    EXPECT_EQ(0, log.sst_input_bytes);
    // But SST output should be set
    EXPECT_EQ(5, log.sst_output_files);
    EXPECT_EQ(40960, log.sst_output_bytes);
}

TEST_F(TabletWriteLogManagerTest, test_load_log_sst_input_always_zero) {
    auto mgr = TabletWriteLogManager::instance();
    // Load logs should always have sst_input = 0 regardless of params
    mgr->add_load_log(1, 100, 200, 300, 400, 10, 1000, 10, 2000, 5, "label1", 10000, 20000, 3, 12288);

    auto logs = mgr->get_logs();
    ASSERT_EQ(1, logs.size());
    auto& log = logs[0];
    EXPECT_EQ(0, log.sst_input_files);
    EXPECT_EQ(0, log.sst_input_bytes);
    EXPECT_EQ(3, log.sst_output_files);
    EXPECT_EQ(12288, log.sst_output_bytes);
    // Load-specific fields
    EXPECT_EQ("label1", log.label);
    EXPECT_EQ(0, log.compaction_score);
    EXPECT_TRUE(log.compaction_type.empty());
    EXPECT_EQ(0, log.input_segments);
}

TEST_F(TabletWriteLogManagerTest, test_compaction_log_all_fields) {
    auto mgr = TabletWriteLogManager::instance();
    mgr->add_compaction_log(2, 300, 40, 15, 25, 500, 50000, 400, 40000, 20, 8, 95, "cumulative", 100000, 200000, 10,
                            102400, 5, 51200);

    auto logs = mgr->get_logs();
    ASSERT_EQ(1, logs.size());
    auto& log = logs[0];
    EXPECT_EQ(2, log.backend_id);
    EXPECT_EQ(300, log.txn_id);
    EXPECT_EQ(40, log.tablet_id);
    EXPECT_EQ(15, log.table_id);
    EXPECT_EQ(25, log.partition_id);
    EXPECT_EQ(LogType::COMPACTION, log.log_type);
    EXPECT_EQ(500, log.input_rows);
    EXPECT_EQ(50000, log.input_bytes);
    EXPECT_EQ(400, log.output_rows);
    EXPECT_EQ(40000, log.output_bytes);
    EXPECT_EQ(20, log.input_segments);
    EXPECT_EQ(8, log.output_segments);
    EXPECT_EQ(95, log.compaction_score);
    EXPECT_EQ("cumulative", log.compaction_type);
    EXPECT_TRUE(log.label.empty());
    EXPECT_EQ(100000, log.begin_time);
    EXPECT_EQ(200000, log.finish_time);
    EXPECT_EQ(10, log.sst_input_files);
    EXPECT_EQ(102400, log.sst_input_bytes);
    EXPECT_EQ(5, log.sst_output_files);
    EXPECT_EQ(51200, log.sst_output_bytes);
}

TEST_F(TabletWriteLogManagerTest, test_filter_negative_values_match_all) {
    auto mgr = TabletWriteLogManager::instance();
    mgr->add_load_log(1, 1, 30, 10, 20, 10, 1000, 10, 2000, 5, "l1", 10000, 20000);
    mgr->add_compaction_log(1, 2, 31, 11, 21, 10, 1000, 10, 2000, 5, 5, 100, "base", 30000, 40000);
    mgr->add_publish_log(1, 3, 32, 12, 22, 50000, 60000, 3, 12288);

    // Default filters (-1) should return all logs
    auto logs = mgr->get_logs(-1, -1, -1, -1, 0, 0);
    ASSERT_EQ(3, logs.size());

    // Filter with table_id=0 should also return all (0 is not > 0)
    logs = mgr->get_logs(0, 0, 0, 0, 0, 0);
    ASSERT_EQ(3, logs.size());
}

TEST_F(TabletWriteLogManagerTest, test_cleanup_preserves_newer_logs) {
    auto mgr = TabletWriteLogManager::instance();
    mgr->add_load_log(1, 1, 30, 10, 20, 10, 1000, 10, 2000, 5, "l1", 10000, 10000);
    mgr->add_compaction_log(1, 2, 31, 11, 21, 10, 1000, 10, 2000, 5, 5, 100, "base", 20000, 20000);
    mgr->add_publish_log(1, 3, 32, 12, 22, 30000, 30000, 3, 12288);

    // Cleanup logs with finish_time < 25000 (removes first two)
    mgr->cleanup_old_logs(25000);
    ASSERT_EQ(1, mgr->size());
    auto logs = mgr->get_logs();
    EXPECT_EQ(3, logs[0].txn_id);
    EXPECT_EQ(LogType::PUBLISH, logs[0].log_type);
}

TEST_F(TabletWriteLogManagerTest, test_singleton_instance) {
    auto* mgr1 = TabletWriteLogManager::instance();
    auto* mgr2 = TabletWriteLogManager::instance();
    EXPECT_EQ(mgr1, mgr2);
}

TEST_F(TabletWriteLogManagerTest, test_large_sst_values) {
    auto mgr = TabletWriteLogManager::instance();
    // Test with large SST byte values (multi-GB)
    int64_t large_bytes = 10LL * 1024 * 1024 * 1024; // 10 GB
    mgr->add_compaction_log(1, 100, 30, 10, 20, 100, 5000, 80, 4000, 10, 3, 80, "base", 10000, 20000, 100, large_bytes,
                            50, large_bytes / 2);

    auto logs = mgr->get_logs();
    ASSERT_EQ(1, logs.size());
    EXPECT_EQ(100, logs[0].sst_input_files);
    EXPECT_EQ(large_bytes, logs[0].sst_input_bytes);
    EXPECT_EQ(50, logs[0].sst_output_files);
    EXPECT_EQ(large_bytes / 2, logs[0].sst_output_bytes);
}

TEST_F(TabletWriteLogManagerTest, test_time_range_filter_both_bounds) {
    auto mgr = TabletWriteLogManager::instance();
    mgr->add_load_log(1, 1, 30, 10, 20, 10, 1000, 10, 2000, 5, "l1", 10000, 10000);
    mgr->add_load_log(1, 2, 30, 10, 20, 10, 1000, 10, 2000, 5, "l2", 20000, 20000);
    mgr->add_load_log(1, 3, 30, 10, 20, 10, 1000, 10, 2000, 5, "l3", 30000, 30000);
    mgr->add_load_log(1, 4, 30, 10, 20, 10, 1000, 10, 2000, 5, "l4", 40000, 40000);

    // Both start and end time filter
    auto logs = mgr->get_logs(0, 0, 0, 0, 20000, 30000);
    ASSERT_EQ(2, logs.size());
    EXPECT_EQ(2, logs[0].txn_id);
    EXPECT_EQ(3, logs[1].txn_id);
}

// ============================================================
// Tests for CompactionTask::compute_sst_stats (static method)
// ============================================================

class ComputeSstStatsTest : public testing::Test {};

TEST_F(ComputeSstStatsTest, test_empty_ssts_no_txn_log) {
    std::vector<FileInfo> ssts;
    auto stats = CompactionTask::compute_sst_stats(ssts, nullptr);
    EXPECT_EQ(0, stats.input_files);
    EXPECT_EQ(0, stats.input_bytes);
    EXPECT_EQ(0, stats.output_files);
    EXPECT_EQ(0, stats.output_bytes);
}

TEST_F(ComputeSstStatsTest, test_writer_ssts_only) {
    std::vector<FileInfo> ssts;
    ssts.push_back(FileInfo{.path = "sst1.sst", .size = 4096});
    ssts.push_back(FileInfo{.path = "sst2.sst", .size = 8192});
    ssts.push_back(FileInfo{.path = "sst3.sst", .size = std::nullopt}); // size unknown

    auto stats = CompactionTask::compute_sst_stats(ssts, nullptr);
    EXPECT_EQ(0, stats.input_files);
    EXPECT_EQ(0, stats.input_bytes);
    EXPECT_EQ(3, stats.output_files);
    EXPECT_EQ(4096 + 8192, stats.output_bytes); // nullopt treated as 0
}

TEST_F(ComputeSstStatsTest, test_txn_log_without_op_compaction) {
    std::vector<FileInfo> ssts;
    ssts.push_back(FileInfo{.path = "sst1.sst", .size = 1024});

    TxnLogPB txn_log;
    // No op_compaction set
    auto stats = CompactionTask::compute_sst_stats(ssts, &txn_log);
    EXPECT_EQ(0, stats.input_files);
    EXPECT_EQ(0, stats.input_bytes);
    EXPECT_EQ(1, stats.output_files);
    EXPECT_EQ(1024, stats.output_bytes);
}

TEST_F(ComputeSstStatsTest, test_txn_log_with_input_sstables) {
    std::vector<FileInfo> ssts;
    ssts.push_back(FileInfo{.path = "eager.sst", .size = 2048});

    TxnLogPB txn_log;
    auto* op = txn_log.mutable_op_compaction();
    auto* input1 = op->add_input_sstables();
    input1->set_filesize(10240);
    auto* input2 = op->add_input_sstables();
    input2->set_filesize(20480);

    auto stats = CompactionTask::compute_sst_stats(ssts, &txn_log);
    EXPECT_EQ(2, stats.input_files);
    EXPECT_EQ(10240 + 20480, stats.input_bytes);
    EXPECT_EQ(1, stats.output_files); // only from writer
    EXPECT_EQ(2048, stats.output_bytes);
}

TEST_F(ComputeSstStatsTest, test_txn_log_with_output_sstables) {
    std::vector<FileInfo> ssts;

    TxnLogPB txn_log;
    auto* op = txn_log.mutable_op_compaction();
    auto* input1 = op->add_input_sstables();
    input1->set_filesize(5000);

    auto* output1 = op->add_output_sstables();
    output1->set_filesize(3000);
    auto* output2 = op->add_output_sstables();
    output2->set_filesize(2000);

    auto stats = CompactionTask::compute_sst_stats(ssts, &txn_log);
    EXPECT_EQ(1, stats.input_files);
    EXPECT_EQ(5000, stats.input_bytes);
    EXPECT_EQ(2, stats.output_files); // from output_sstables
    EXPECT_EQ(3000 + 2000, stats.output_bytes);
}

TEST_F(ComputeSstStatsTest, test_txn_log_with_output_sstable_singular) {
    std::vector<FileInfo> ssts;
    ssts.push_back(FileInfo{.path = "eager.sst", .size = 1000});

    TxnLogPB txn_log;
    auto* op = txn_log.mutable_op_compaction();
    auto* input = op->add_input_sstables();
    input->set_filesize(8000);

    // Set the singular output_sstable
    op->mutable_output_sstable()->set_filesize(4000);

    auto stats = CompactionTask::compute_sst_stats(ssts, &txn_log);
    EXPECT_EQ(1, stats.input_files);
    EXPECT_EQ(8000, stats.input_bytes);
    EXPECT_EQ(2, stats.output_files); // 1 from writer + 1 from output_sstable
    EXPECT_EQ(1000 + 4000, stats.output_bytes);
}

TEST_F(ComputeSstStatsTest, test_txn_log_with_all_sst_sources) {
    // Writer SSTs (from eager build)
    std::vector<FileInfo> ssts;
    ssts.push_back(FileInfo{.path = "eager1.sst", .size = 1000});
    ssts.push_back(FileInfo{.path = "eager2.sst", .size = 2000});

    TxnLogPB txn_log;
    auto* op = txn_log.mutable_op_compaction();

    // Input SSTs from major compaction
    op->add_input_sstables()->set_filesize(10000);
    op->add_input_sstables()->set_filesize(20000);
    op->add_input_sstables()->set_filesize(30000);

    // Output SSTs from major compaction (plural)
    op->add_output_sstables()->set_filesize(5000);

    // Output SST from major compaction (singular)
    op->mutable_output_sstable()->set_filesize(7000);

    auto stats = CompactionTask::compute_sst_stats(ssts, &txn_log);
    EXPECT_EQ(3, stats.input_files);
    EXPECT_EQ(10000 + 20000 + 30000, stats.input_bytes);
    EXPECT_EQ(2 + 1 + 1, stats.output_files); // 2 writer + 1 output_sstables + 1 output_sstable
    EXPECT_EQ(1000 + 2000 + 5000 + 7000, stats.output_bytes);
}

// ============================================================
// Tests for LakePrimaryIndex publish SST stats accessors
// ============================================================

class LakePrimaryIndexSstStatsTest : public testing::Test {};

TEST_F(LakePrimaryIndexSstStatsTest, test_publish_sst_stats_disabled) {
    LakePrimaryIndex index;
    // persistent index is disabled by default
    index.set_enable_persistent_index(false);

    EXPECT_EQ(0, index.publish_sst_flush_count());
    EXPECT_EQ(0, index.publish_sst_flush_bytes());

    // reset should not crash when disabled
    index.reset_publish_sst_stats();
    EXPECT_EQ(0, index.publish_sst_flush_count());
    EXPECT_EQ(0, index.publish_sst_flush_bytes());
}

TEST_F(LakePrimaryIndexSstStatsTest, test_publish_sst_stats_enabled_no_persistent_index) {
    LakePrimaryIndex index;
    // Enable persistent index but don't actually create one
    // (the internal _persistent_index will be nullptr or not a LakePersistentIndex)
    index.set_enable_persistent_index(true);

    // Should return 0 when persistent_index is not LakePersistentIndex
    EXPECT_EQ(0, index.publish_sst_flush_count());
    EXPECT_EQ(0, index.publish_sst_flush_bytes());

    // reset should not crash
    index.reset_publish_sst_stats();
}

// ============================================================
// Tests for LakePersistentIndex publish SST stats getters
// ============================================================

class LakePersistentIndexSstStatsTest : public testing::Test {};

TEST_F(LakePersistentIndexSstStatsTest, test_publish_sst_stats_default) {
    // Create with nullptr tablet_mgr - safe for just testing stats getters
    LakePersistentIndex index(nullptr, 0);

    // Default values should be 0
    EXPECT_EQ(0, index.publish_sst_flush_count());
    EXPECT_EQ(0, index.publish_sst_flush_bytes());

    // Reset should keep them at 0
    index.reset_publish_sst_stats();
    EXPECT_EQ(0, index.publish_sst_flush_count());
    EXPECT_EQ(0, index.publish_sst_flush_bytes());
}

} // namespace starrocks::lake
