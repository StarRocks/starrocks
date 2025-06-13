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
    EXPECT_OK(scanner.init(&params, &pool));
    EXPECT_OK(scanner.TEST_start(&state, infos));

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

} // namespace starrocks
