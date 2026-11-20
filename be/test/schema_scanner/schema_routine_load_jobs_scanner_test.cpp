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

#include "schema_scanner/schema_routine_load_jobs_scanner.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "runtime/runtime_state.h"

namespace starrocks {

// The FE leaves out the properties of a routine load job for a user who may not read the rows the job loads, and
// they must read NULL. The scanner's start() goes through a thrift RPC to FE; the test bypasses that by populating
// _result directly, which BE unit tests may do as they are compiled with -fno-access-control.
class SchemaRoutineLoadJobsScannerTest : public ::testing::Test {
protected:
    void SetUp() override {
        _params.ip = &_ip;
        _params.port = 9020;
        _state = std::make_unique<RuntimeState>(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
    }

    ChunkPtr create_chunk(const std::vector<SlotDescriptor*>& slot_descs) {
        ChunkPtr chunk = std::make_shared<Chunk>();
        for (const auto* slot_desc : slot_descs) {
            MutableColumnPtr column = ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());
            chunk->append_column(std::move(column), slot_desc->id());
        }
        return chunk;
    }

    // A job with the fields the scanner reads unconditionally, and without its properties.
    TRoutineLoadJobInfo make_job_info(int64_t id) {
        TRoutineLoadJobInfo info;
        info.__set_id(id);
        info.__set_name("job_" + std::to_string(id));
        info.__set_db_name("test_db");
        info.__set_table_name("test_tbl");
        info.__set_state("RUNNING");
        info.__set_data_source_type("KAFKA");
        info.__set_current_task_num(1);
        info.__set_statistic("{}");
        info.__set_progress("{}");
        info.__set_latest_source_position("{}");
        info.__set_offset_lag("{}");
        info.__set_timestamp_progress("{}");
        return info;
    }

    std::string string_at(const ChunkPtr& chunk, int slot_id, size_t row) {
        auto* nullable = down_cast<NullableColumn*>(chunk->get_column_raw_ptr_by_slot_id(slot_id));
        EXPECT_FALSE(nullable->is_null(row));
        return nullable->data_column_raw_ptr()->get(row).get_slice().to_string();
    }

    bool is_null_at(const ChunkPtr& chunk, int slot_id, size_t row) {
        return down_cast<NullableColumn*>(chunk->get_column_raw_ptr_by_slot_id(slot_id))->is_null(row);
    }

    SchemaScannerParam _params;
    std::string _ip = "127.0.0.1";
    ObjectPool _pool;
    std::unique_ptr<RuntimeState> _state;

    // Slot ids of the property columns (the case labels in SchemaRoutineLoadJobsScanner::fill_chunk).
    static constexpr int JOB_PROPERTIES = 11;
    static constexpr int DATA_SOURCE_PROPERTIES = 12;
    static constexpr int CUSTOM_PROPERTIES = 13;
};

TEST_F(SchemaRoutineLoadJobsScannerTest, unset_properties_render_null) {
    SchemaRoutineLoadJobsScanner scanner;
    EXPECT_OK(scanner.init(&_params, &_pool));
    TRoutineLoadJobInfo full = make_job_info(1);
    full.__set_job_properties("{\"max_error_number\":\"0\"}");
    full.__set_data_source_properties("{\"topic\":\"t\"}");
    full.__set_custom_properties("{}");
    scanner._result.loads = {full, make_job_info(2)};

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = true;
    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_FALSE(eos);
    ASSERT_EQ(2, chunk->num_rows());

    EXPECT_EQ("{\"max_error_number\":\"0\"}", string_at(chunk, JOB_PROPERTIES, 0));
    EXPECT_EQ("{\"topic\":\"t\"}", string_at(chunk, DATA_SOURCE_PROPERTIES, 0));
    EXPECT_EQ("{}", string_at(chunk, CUSTOM_PROPERTIES, 0));
    EXPECT_TRUE(is_null_at(chunk, JOB_PROPERTIES, 1));
    EXPECT_TRUE(is_null_at(chunk, DATA_SOURCE_PROPERTIES, 1));
    EXPECT_TRUE(is_null_at(chunk, CUSTOM_PROPERTIES, 1));

    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_TRUE(eos);
}

} // namespace starrocks
