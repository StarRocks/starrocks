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
        config::tablet_write_log_buffer_size = 100;
        config::tablet_write_log_retention_time_ms = 10000;
    }
};

TEST_F(TabletWriteLogManagerTest, test_add_and_get_logs) {
    auto manager = TabletWriteLogManager::instance();
    
    int64_t now = 1000000000000; // Some timestamp in ms
    int64_t begin_time = now - 5000; // 5 seconds ago
    int64_t finish_time = now;
    
    // Add load log
    manager->add_load_log(1001, 2001, 3001, 4001, 5001, 100, 1024, 100, 1000, 1, "load_label", begin_time, finish_time);
    
    // Add compaction log
    manager->add_compaction_log(1001, 3001, 3001, 4001, 5001, 200, 2048, 180, 1800, 2, 1, 50, "vertical", begin_time, finish_time);

    // Query logs
    auto logs = manager->get_logs();
    ASSERT_GE(logs.size(), 2);

    // Check load log
    bool found_load = false;
    for (const auto& log : logs) {
        if (log.log_type == LogType::LOAD && log.txn_id == 2001) {
            found_load = true;
            ASSERT_EQ(log.backend_id, 1001);
            ASSERT_EQ(log.tablet_id, 3001);
            ASSERT_EQ(log.input_rows, 100);
            ASSERT_EQ(log.label, "load_label");
            ASSERT_EQ(log.begin_time, begin_time);
            ASSERT_EQ(log.finish_time, finish_time);
            break;
        }
    }
    ASSERT_TRUE(found_load);

    // Check compaction log
    bool found_compaction = false;
    for (const auto& log : logs) {
        if (log.log_type == LogType::COMPACTION && log.tablet_id == 3001 && log.input_segments == 2) {
            found_compaction = true;
            ASSERT_EQ(log.backend_id, 1001);
            ASSERT_EQ(log.txn_id, 3001);
            ASSERT_EQ(log.output_segments, 1);
            ASSERT_EQ(log.compaction_type, "vertical");
            ASSERT_EQ(log.begin_time, begin_time);
            ASSERT_EQ(log.finish_time, finish_time);
            break;
        }
    }
    ASSERT_TRUE(found_compaction);
}

TEST_F(TabletWriteLogManagerTest, test_filter) {
    auto manager = TabletWriteLogManager::instance();
    
    int64_t now = 1000000000000;
    
    // Add logs for different tablets
    manager->add_load_log(1001, 2002, 3002, 4002, 5002, 100, 100, 100, 100, 1, "label2", now, now);
    manager->add_load_log(1001, 2003, 3003, 4003, 5003, 100, 100, 100, 100, 1, "label3", now, now);

    // Filter by tablet_id
    auto logs = manager->get_logs(-1, -1, 3002);
    ASSERT_GE(logs.size(), 1);
    ASSERT_EQ(logs[0].tablet_id, 3002);
}

} // namespace starrocks::lake

