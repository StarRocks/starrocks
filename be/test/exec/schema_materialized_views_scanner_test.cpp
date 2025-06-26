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

#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "exec/schema_scanner/schema_fe_tablet_schedules_scanner.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"

namespace starrocks {

class SchemaMaterializedViewsScannerTest : public ::testing::Test {
private:
    ChunkPtr create_chunk(const std::vector<SlotDescriptor*> slot_descs) {
        ChunkPtr chunk = std::make_shared<Chunk>();
        for (const auto* slot_desc : slot_descs) {
            MutableColumnPtr column = ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());
            chunk->append_column(std::move(column), slot_desc->id());
        }
        return chunk;
    }

protected:
    void SetUp() override {
        _params.ip = &_ip;
        _params.port = 9020;
        _state = std::make_unique<RuntimeState>(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
    }

    SchemaScannerParam _params;
    std::string _ip = "127.0.0.1";
    ObjectPool _pool;
    std::unique_ptr<RuntimeState> _state;
};

TEST_F(SchemaFeTabletSchedulesScannerTest, test_normal) {
    SchemaFeTabletSchedulesScanner scanner;

    // prepare tablet schedule data
    std::vector<TTabletSchedule> infos;
    // normal
    auto& s1 = infos.emplace_back();
    s1.__set_table_id(1);
    s1.__set_partition_id(2);
    s1.__set_tablet_id(10);
    s1.__set_type("REPAIR");
    s1.__set_priority("HIGH");
    s1.__set_state("FINISHED");
    s1.__set_tablet_status("DISK_MIGRATION");
    s1.__set_create_time(1749711000);
    s1.__set_schedule_time(1749711001);
    s1.__set_finish_time(1749711002);
    s1.__set_clone_src(10001);
    s1.__set_clone_dest(10002);
    s1.__set_clone_bytes(1);
    s1.__set_clone_duration(1);
    s1.__set_error_msg("success");
    // xxx_time is null
    auto& s2 = infos.emplace_back();
    s2.__set_table_id(1);
    s2.__set_partition_id(2);
    s2.__set_tablet_id(11);
    s2.__set_type("BALANCE");
    s2.__set_priority("NORMAL");
    s2.__set_state("PENDING");
    s2.__set_tablet_status("DISK_MIGRATION");
    s2.__set_create_time(0);
    s2.__set_schedule_time(0);
    s2.__set_finish_time(-0.001);
    s2.__set_clone_src(10001);
    s2.__set_clone_dest(10002);
    s2.__set_clone_bytes(0);
    s2.__set_clone_duration(0);
    s2.__set_error_msg("");

    // init and start scanner
    EXPECT_OK(scanner.init(&_params, &_pool));
    EXPECT_OK(scanner.TEST_start(_state.get(), infos));

    // check get_next
    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_FALSE(eos);
    EXPECT_EQ(1, chunk->num_rows());
    EXPECT_EQ(
            "[1, 2, 10, 'REPAIR', 'HIGH', 'FINISHED', 'DISK_MIGRATION', 2025-06-12 14:50:00, 2025-06-12 14:50:01, "
            "2025-06-12 14:50:02, 10001, 10002, 1, 1, 'success']",
            chunk->debug_row(0));

    chunk->reset();
    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_FALSE(eos);
    EXPECT_EQ(1, chunk->num_rows());
    EXPECT_EQ("[1, 2, 11, 'BALANCE', 'NORMAL', 'PENDING', 'DISK_MIGRATION', NULL, NULL, NULL, 10001, 10002, 0, 0, '']",
              chunk->debug_row(0));

    chunk->reset();
    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_TRUE(eos);
}

TEST_F(SchemaFeTabletSchedulesScannerTest, test_empty_data) {
    SchemaFeTabletSchedulesScanner scanner;

    // init and start scanner with empty data
    EXPECT_OK(scanner.init(&_params, &_pool));
    EXPECT_OK(scanner.TEST_start(_state.get(), {}));

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_TRUE(eos);
    EXPECT_EQ(0, chunk->num_rows());
}

TEST_F(SchemaFeTabletSchedulesScannerTest, test_large_values) {
    SchemaFeTabletSchedulesScanner scanner;

    std::vector<TTabletSchedule> infos;
    auto& s = infos.emplace_back();
    s.__set_table_id(INT64_MAX);
    s.__set_partition_id(INT64_MAX);
    s.__set_tablet_id(INT64_MAX);
    s.__set_type("VERY_LONG_TYPE_NAME_THAT_MIGHT_CAUSE_ISSUES");
    s.__set_priority("VERY_HIGH_PRIORITY_LEVEL");
    s.__set_state("VERY_LONG_STATE_DESCRIPTION");
    s.__set_tablet_status("VERY_LONG_TABLET_STATUS_DESCRIPTION");
    s.__set_create_time(1749711000.123456);
    s.__set_schedule_time(1749711001.654321);
    s.__set_finish_time(1749711002.999999);
    s.__set_clone_src(INT64_MAX);
    s.__set_clone_dest(INT64_MAX);
    s.__set_clone_bytes(INT64_MAX);
    s.__set_clone_duration(DBL_MAX);
    s.__set_error_msg("Very long error message that might contain special characters: !@#$%^&*()_+-=[]{}|;':\",./<>?");

    EXPECT_OK(scanner.init(&_params, &_pool));
    EXPECT_OK(scanner.TEST_start(_state.get(), infos));

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_FALSE(eos);
    EXPECT_EQ(1, chunk->num_rows());

    // Verify the large values are handled correctly
    auto row = chunk->debug_row(0);
    EXPECT_TRUE(row.find("9223372036854775807") != std::string::npos); // INT64_MAX
    EXPECT_TRUE(row.find("VERY_LONG_TYPE_NAME_THAT_MIGHT_CAUSE_ISSUES") != std::string::npos);
    EXPECT_TRUE(row.find("Very long error message") != std::string::npos);
}

TEST_F(SchemaFeTabletSchedulesScannerTest, test_negative_values) {
    SchemaFeTabletSchedulesScanner scanner;

    std::vector<TTabletSchedule> infos;
    auto& s = infos.emplace_back();
    s.__set_table_id(-1);
    s.__set_partition_id(-2);
    s.__set_tablet_id(-3);
    s.__set_type("REPAIR");
    s.__set_priority("LOW");
    s.__set_state("FAILED");
    s.__set_tablet_status("UNHEALTHY");
    s.__set_create_time(-1749711000);
    s.__set_schedule_time(-1749711001);
    s.__set_finish_time(-1749711002);
    s.__set_clone_src(-10001);
    s.__set_clone_dest(-10002);
    s.__set_clone_bytes(-1000);
    s.__set_clone_duration(-1.5);
    s.__set_error_msg("negative test");

    EXPECT_OK(scanner.init(&_params, &_pool));
    EXPECT_OK(scanner.TEST_start(_state.get(), infos));

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_FALSE(eos);
    EXPECT_EQ(1, chunk->num_rows());

    auto row = chunk->debug_row(0);
    EXPECT_TRUE(row.find("-1") != std::string::npos);
    EXPECT_TRUE(row.find("-2") != std::string::npos);
    EXPECT_TRUE(row.find("-3") != std::string::npos);
    EXPECT_TRUE(row.find("-10001") != std::string::npos);
    EXPECT_TRUE(row.find("-10002") != std::string::npos);
    EXPECT_TRUE(row.find("-1000") != std::string::npos);
    EXPECT_TRUE(row.find("-1.5") != std::string::npos);
}

TEST_F(SchemaFeTabletSchedulesScannerTest, test_zero_values) {
    SchemaFeTabletSchedulesScanner scanner;

    std::vector<TTabletSchedule> infos;
    auto& s = infos.emplace_back();
    s.__set_table_id(0);
    s.__set_partition_id(0);
    s.__set_tablet_id(0);
    s.__set_type("");
    s.__set_priority("");
    s.__set_state("");
    s.__set_tablet_status("");
    s.__set_create_time(0);
    s.__set_schedule_time(0);
    s.__set_finish_time(0);
    s.__set_clone_src(0);
    s.__set_clone_dest(0);
    s.__set_clone_bytes(0);
    s.__set_clone_duration(0);
    s.__set_error_msg("");

    EXPECT_OK(scanner.init(&_params, &_pool));
    EXPECT_OK(scanner.TEST_start(_state.get(), infos));

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_FALSE(eos);
    EXPECT_EQ(1, chunk->num_rows());

    auto row = chunk->debug_row(0);
    EXPECT_TRUE(row.find("0") != std::string::npos);
    EXPECT_TRUE(row.find("''") != std::string::npos);   // empty strings
    EXPECT_TRUE(row.find("NULL") != std::string::npos); // null timestamps
}

TEST_F(SchemaFeTabletSchedulesScannerTest, test_multiple_records) {
    SchemaFeTabletSchedulesScanner scanner;

    std::vector<TTabletSchedule> infos;

    // Add 10 records with different values
    for (int i = 0; i < 10; ++i) {
        auto& s = infos.emplace_back();
        s.__set_table_id(i + 1);
        s.__set_partition_id(i + 10);
        s.__set_tablet_id(i + 100);
        s.__set_type("TYPE_" + std::to_string(i));
        s.__set_priority("PRIORITY_" + std::to_string(i));
        s.__set_state("STATE_" + std::to_string(i));
        s.__set_tablet_status("STATUS_" + std::to_string(i));
        s.__set_create_time(1749711000 + i);
        s.__set_schedule_time(1749711001 + i);
        s.__set_finish_time(1749711002 + i);
        s.__set_clone_src(10001 + i);
        s.__set_clone_dest(10002 + i);
        s.__set_clone_bytes(1000 + i);
        s.__set_clone_duration(1.0 + i * 0.1);
        s.__set_error_msg("error_" + std::to_string(i));
    }

    EXPECT_OK(scanner.init(&_params, &_pool));
    EXPECT_OK(scanner.TEST_start(_state.get(), infos));

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;
    int count = 0;

    while (!eos) {
        EXPECT_OK(scanner.get_next(&chunk, &eos));
        if (!eos) {
            EXPECT_EQ(1, chunk->num_rows());
            count++;
        }
        chunk->reset();
    }

    EXPECT_EQ(10, count);
}

TEST_F(SchemaFeTabletSchedulesScannerTest, test_null_pointer_parameters) {
    SchemaFeTabletSchedulesScanner scanner;

    std::vector<TTabletSchedule> infos;
    auto& s = infos.emplace_back();
    s.__set_table_id(1);
    s.__set_partition_id(2);
    s.__set_tablet_id(3);
    s.__set_type("REPAIR");
    s.__set_priority("HIGH");
    s.__set_state("FINISHED");
    s.__set_tablet_status("HEALTHY");
    s.__set_create_time(1749711000);
    s.__set_schedule_time(1749711001);
    s.__set_finish_time(1749711002);
    s.__set_clone_src(10001);
    s.__set_clone_dest(10002);
    s.__set_clone_bytes(1000);
    s.__set_clone_duration(1.5);
    s.__set_error_msg("test");

    EXPECT_OK(scanner.init(&_params, &_pool));
    EXPECT_OK(scanner.TEST_start(_state.get(), infos));

    // Test with null chunk pointer
    bool eos = false;
    EXPECT_FALSE(scanner.get_next(nullptr, &eos).ok());

    // Test with null eos pointer
    auto chunk = create_chunk(scanner.get_slot_descs());
    EXPECT_FALSE(scanner.get_next(&chunk, nullptr).ok());
}

TEST_F(SchemaFeTabletSchedulesScannerTest, test_uninitialized_scanner) {
    SchemaFeTabletSchedulesScanner scanner;

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    // Should fail because scanner is not initialized
    EXPECT_FALSE(scanner.get_next(&chunk, &eos).ok());
}

TEST_F(SchemaFeTabletSchedulesScannerTest, test_different_schedule_types) {
    SchemaFeTabletSchedulesScanner scanner;

    std::vector<TTabletSchedule> infos;

    // Test different schedule types
    std::vector<std::string> types = {"REPAIR", "BALANCE", "CLONE", "MIGRATE", "COMPACTION"};
    std::vector<std::string> priorities = {"HIGH", "NORMAL", "LOW"};
    std::vector<std::string> states = {"PENDING", "RUNNING", "FINISHED", "FAILED", "CANCELLED"};
    std::vector<std::string> statuses = {"HEALTHY", "UNHEALTHY", "DISK_MIGRATION", "REPLICA_MISSING"};

    for (size_t i = 0; i < types.size(); ++i) {
        auto& s = infos.emplace_back();
        s.__set_table_id(i + 1);
        s.__set_partition_id(i + 10);
        s.__set_tablet_id(i + 100);
        s.__set_type(types[i]);
        s.__set_priority(priorities[i % priorities.size()]);
        s.__set_state(states[i % states.size()]);
        s.__set_tablet_status(statuses[i % statuses.size()]);
        s.__set_create_time(1749711000 + i);
        s.__set_schedule_time(1749711001 + i);
        s.__set_finish_time(1749711002 + i);
        s.__set_clone_src(10001 + i);
        s.__set_clone_dest(10002 + i);
        s.__set_clone_bytes(1000 + i);
        s.__set_clone_duration(1.0 + i * 0.1);
        s.__set_error_msg("test_" + std::to_string(i));
    }

    EXPECT_OK(scanner.init(&_params, &_pool));
    EXPECT_OK(scanner.TEST_start(_state.get(), infos));

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;
    int count = 0;

    while (!eos) {
        EXPECT_OK(scanner.get_next(&chunk, &eos));
        if (!eos) {
            EXPECT_EQ(1, chunk->num_rows());
            auto row = chunk->debug_row(0);
            EXPECT_TRUE(row.find(types[count]) != std::string::npos);
            count++;
        }
        chunk->reset();
    }

    EXPECT_EQ(5, count);
}

TEST_F(SchemaFeTabletSchedulesScannerTest, test_edge_case_timestamps) {
    SchemaFeTabletSchedulesScanner scanner;

    std::vector<TTabletSchedule> infos;

    // Test edge case timestamps
    std::vector<double> timestamps = {
            0.0,               // zero
            -1.0,              // negative
            1.0,               // small positive
            1749711000.0,      // normal timestamp
            1749711000.999999, // high precision
            1749711000.000001, // very small fraction
            DBL_MAX,           // maximum double
            DBL_MIN,           // minimum positive double
            -DBL_MAX           // minimum double
    };

    for (size_t i = 0; i < timestamps.size(); ++i) {
        auto& s = infos.emplace_back();
        s.__set_table_id(i + 1);
        s.__set_partition_id(i + 10);
        s.__set_tablet_id(i + 100);
        s.__set_type("REPAIR");
        s.__set_priority("HIGH");
        s.__set_state("FINISHED");
        s.__set_tablet_status("HEALTHY");
        s.__set_create_time(timestamps[i]);
        s.__set_schedule_time(timestamps[i]);
        s.__set_finish_time(timestamps[i]);
        s.__set_clone_src(10001);
        s.__set_clone_dest(10002);
        s.__set_clone_bytes(1000);
        s.__set_clone_duration(1.5);
        s.__set_error_msg("timestamp_test_" + std::to_string(i));
    }

    EXPECT_OK(scanner.init(&_params, &_pool));
    EXPECT_OK(scanner.TEST_start(_state.get(), infos));

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;
    int count = 0;

    while (!eos) {
        EXPECT_OK(scanner.get_next(&chunk, &eos));
        if (!eos) {
            EXPECT_EQ(1, chunk->num_rows());
            count++;
        }
        chunk->reset();
    }

    EXPECT_EQ(9, count);
}

TEST_F(SchemaFeTabletSchedulesScannerTest, test_special_characters_in_strings) {
    SchemaFeTabletSchedulesScanner scanner;

    std::vector<TTabletSchedule> infos;
    auto& s = infos.emplace_back();
    s.__set_table_id(1);
    s.__set_partition_id(2);
    s.__set_tablet_id(3);
    s.__set_type("REPAIR_WITH_SPECIAL_CHARS_!@#$%^&*()");
    s.__set_priority("HIGH_PRIORITY_WITH_SPECIAL_CHARS_[]{}|");
    s.__set_state("FINISHED_STATE_WITH_SPECIAL_CHARS_\\\"'");
    s.__set_tablet_status("HEALTHY_STATUS_WITH_SPECIAL_CHARS_<>?");
    s.__set_create_time(1749711000);
    s.__set_schedule_time(1749711001);
    s.__set_finish_time(1749711002);
    s.__set_clone_src(10001);
    s.__set_clone_dest(10002);
    s.__set_clone_bytes(1000);
    s.__set_clone_duration(1.5);
    s.__set_error_msg("Error message with special characters: !@#$%^&*()_+-=[]{}|;':\",./<>?\\");

    EXPECT_OK(scanner.init(&_params, &_pool));
    EXPECT_OK(scanner.TEST_start(_state.get(), infos));

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_FALSE(eos);
    EXPECT_EQ(1, chunk->num_rows());

    auto row = chunk->debug_row(0);
    EXPECT_TRUE(row.find("REPAIR_WITH_SPECIAL_CHARS_!@#$%^&*()") != std::string::npos);
    EXPECT_TRUE(row.find("HIGH_PRIORITY_WITH_SPECIAL_CHARS_[]{}|") != std::string::npos);
    EXPECT_TRUE(row.find("FINISHED_STATE_WITH_SPECIAL_CHARS_\\\"'") != std::string::npos);
    EXPECT_TRUE(row.find("HEALTHY_STATUS_WITH_SPECIAL_CHARS_<>?") != std::string::npos);
    EXPECT_TRUE(row.find("Error message with special characters") != std::string::npos);
}

TEST_F(SchemaFeTabletSchedulesScannerTest, test_unicode_characters) {
    SchemaFeTabletSchedulesScanner scanner;

    std::vector<TTabletSchedule> infos;
    auto& s = infos.emplace_back();
    s.__set_table_id(1);
    s.__set_partition_id(2);
    s.__set_tablet_id(3);
    s.__set_type("REPAIR_中文");
    s.__set_priority("HIGH_优先级_中文");
    s.__set_state("FINISHED_状态_中文");
    s.__set_tablet_status("HEALTHY_健康_中文");
    s.__set_create_time(1749711000);
    s.__set_schedule_time(1749711001);
    s.__set_finish_time(1749711002);
    s.__set_clone_src(10001);
    s.__set_clone_dest(10002);
    s.__set_clone_bytes(1000);
    s.__set_clone_duration(1.5);
    s.__set_error_msg("错误信息包含中文和特殊字符：!@#$%^&*()_+-=[]{}|;':\",./<>?\\");

    EXPECT_OK(scanner.init(&_params, &_pool));
    EXPECT_OK(scanner.TEST_start(_state.get(), infos));

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_FALSE(eos);
    EXPECT_EQ(1, chunk->num_rows());

    auto row = chunk->debug_row(0);
    EXPECT_TRUE(row.find("REPAIR_中文") != std::string::npos);
    EXPECT_TRUE(row.find("HIGH_优先级_中文") != std::string::npos);
    EXPECT_TRUE(row.find("FINISHED_状态_中文") != std::string::npos);
    EXPECT_TRUE(row.find("HEALTHY_健康_中文") != std::string::npos);
    EXPECT_TRUE(row.find("错误信息包含中文") != std::string::npos);
}

} // namespace starrocks