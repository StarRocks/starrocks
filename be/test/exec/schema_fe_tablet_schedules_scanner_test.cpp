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

#include "exec/schema_scanner/schema_fe_tablet_schedules_scanner.h"

#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"

namespace starrocks {

class SchemaFeTabletSchedulesScannerTest : public ::testing::Test {
private:
    ChunkPtr create_chunk(const std::vector<SlotDescriptor*> slot_descs) {
        ChunkPtr chunk = std::make_shared<Chunk>();
        for (const auto* slot_desc : slot_descs) {
            MutableColumnPtr column = ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());
            chunk->append_column(std::move(column), slot_desc->id());
        }
        return chunk;
    }
};

TEST_F(SchemaFeTabletSchedulesScannerTest, test_normal) {
    SchemaFeTabletSchedulesScanner scanner;
    SchemaScannerParam params;
    std::string ip = "127.0.0.1";
    params.ip = &ip;
    params.port = 9020;
    ObjectPool pool;
    RuntimeState state(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);

    // prepare tablet schedule data
    std::vector<TTabletSchedule> infos;
    // normal
    auto& s1 = infos.emplace_back();
    s1.__set_tablet_id(10);
    s1.__set_table_id(1);
    s1.__set_partition_id(2);
    s1.__set_type("REPAIR");
    s1.__set_state("FINISHED");
    s1.__set_schedule_reason("DISK_MIGRATION");
    s1.__set_medium("HDD");
    s1.__set_priority("HIGH");
    s1.__set_orig_priority("NORMAL");
    s1.__set_last_priority_adjust_time(1749711001);
    s1.__set_visible_version(100);
    s1.__set_committed_version(100);
    s1.__set_src_be_id(10001);
    s1.__set_src_path("123456789");
    s1.__set_dest_be_id(10002);
    s1.__set_dest_path("123456780");
    s1.__set_timeout(1000);
    s1.__set_create_time(1749711000);
    s1.__set_schedule_time(1749711001);
    s1.__set_finish_time(1749711002);
    s1.__set_clone_bytes(1);
    s1.__set_clone_duration(1);
    s1.__set_clone_rate(1);
    s1.__set_failed_schedule_count(1);
    s1.__set_failed_running_count(0);
    s1.__set_error_msg("success");
    // xxx_time is null
    auto& s2 = infos.emplace_back();
    s2.__set_tablet_id(11);
    s2.__set_table_id(1);
    s2.__set_partition_id(2);
    s2.__set_type("BALANCE");
    s2.__set_state("PENDING");
    s2.__set_schedule_reason("DISK_MIGRATION");
    s2.__set_medium("SSD");
    s2.__set_priority("NORMAL");
    s2.__set_orig_priority("NORMAL");
    s2.__set_last_priority_adjust_time(0);
    s2.__set_visible_version(100);
    s2.__set_committed_version(100);
    s2.__set_src_be_id(10001);
    s2.__set_src_path("123456789");
    s2.__set_dest_be_id(10002);
    s2.__set_dest_path("123456780");
    s2.__set_timeout(1000);
    s2.__set_create_time(0);
    s2.__set_schedule_time(0);
    s2.__set_finish_time(-0.001);
    s2.__set_clone_bytes(0);
    s2.__set_clone_duration(0);
    s2.__set_clone_rate(0);
    s2.__set_failed_schedule_count(0);
    s2.__set_failed_running_count(0);
    s2.__set_error_msg("");

    // init and start scanner
    EXPECT_OK(scanner.init(&params, &pool));
    EXPECT_OK(scanner.TEST_start(&state, infos));

    // check get_next
    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_FALSE(eos);
    EXPECT_EQ(1, chunk->num_rows());
    EXPECT_EQ(
            "[10, 1, 2, 'REPAIR', 'FINISHED', 'DISK_MIGRATION', 'HDD', 'HIGH', 'NORMAL', 2025-06-12 14:50:01, 100, "
            "100, 10001, '123456789', 10002, '123456780', 1000, 2025-06-12 14:50:00, 2025-06-12 14:50:01, "
            "2025-06-12 14:50:02, 1, 1, 1, 1, 0, 'success']",
            chunk->debug_row(0));

    chunk->reset();
    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_FALSE(eos);
    EXPECT_EQ(1, chunk->num_rows());
    EXPECT_EQ(
            "[11, 1, 2, 'BALANCE', 'PENDING', 'DISK_MIGRATION', 'SSD', 'NORMAL', 'NORMAL', NULL, 100, 100, 10001, "
            "'123456789', 10002, '123456780', 1000, NULL, NULL, NULL, 0, 0, 0, 0, 0, '']",
            chunk->debug_row(0));

    chunk->reset();
    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_TRUE(eos);
}

} // namespace starrocks
