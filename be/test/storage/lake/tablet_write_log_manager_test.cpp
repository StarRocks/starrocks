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

#include "common/config.h"

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

} // namespace starrocks::lake
