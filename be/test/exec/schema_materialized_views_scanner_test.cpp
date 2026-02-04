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

#include "exec/schema_scanner/schema_materialized_views_scanner.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "column/column_helper.h"
#include "runtime/runtime_state.h"

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

TEST_F(SchemaMaterializedViewsScannerTest, test_scanner_initialization) {
    SchemaMaterializedViewsScanner scanner;

    // Test that scanner can be created and initialized
    EXPECT_OK(scanner.init(&_params, &_pool));

    // Test that scanner has the correct number of columns
    auto slot_descs = scanner.get_slot_descs();
    EXPECT_EQ(27, slot_descs.size());

    // Test column names and types
    EXPECT_EQ("MATERIALIZED_VIEW_ID", slot_descs[0]->col_name());
    EXPECT_EQ("TABLE_SCHEMA", slot_descs[1]->col_name());
    EXPECT_EQ("TABLE_NAME", slot_descs[2]->col_name());
    EXPECT_EQ("REFRESH_TYPE", slot_descs[3]->col_name());
    EXPECT_EQ("IS_ACTIVE", slot_descs[4]->col_name());
    EXPECT_EQ("INACTIVE_REASON", slot_descs[5]->col_name());
    EXPECT_EQ("PARTITION_TYPE", slot_descs[6]->col_name());
    EXPECT_EQ("TASK_ID", slot_descs[7]->col_name());
    EXPECT_EQ("TASK_NAME", slot_descs[8]->col_name());
    EXPECT_EQ("LAST_REFRESH_START_TIME", slot_descs[9]->col_name());
    EXPECT_EQ("LAST_REFRESH_FINISHED_TIME", slot_descs[10]->col_name());
    EXPECT_EQ("LAST_REFRESH_DURATION", slot_descs[11]->col_name());
    EXPECT_EQ("LAST_REFRESH_STATE", slot_descs[12]->col_name());
    EXPECT_EQ("LAST_REFRESH_FORCE_REFRESH", slot_descs[13]->col_name());
    EXPECT_EQ("LAST_REFRESH_START_PARTITION", slot_descs[14]->col_name());
    EXPECT_EQ("LAST_REFRESH_END_PARTITION", slot_descs[15]->col_name());
    EXPECT_EQ("LAST_REFRESH_BASE_REFRESH_PARTITIONS", slot_descs[16]->col_name());
    EXPECT_EQ("LAST_REFRESH_MV_REFRESH_PARTITIONS", slot_descs[17]->col_name());
    EXPECT_EQ("LAST_REFRESH_ERROR_CODE", slot_descs[18]->col_name());
    EXPECT_EQ("LAST_REFRESH_ERROR_MESSAGE", slot_descs[19]->col_name());
    EXPECT_EQ("TABLE_ROWS", slot_descs[20]->col_name());
    EXPECT_EQ("MATERIALIZED_VIEW_DEFINITION", slot_descs[21]->col_name());
    EXPECT_EQ("EXTRA_MESSAGE", slot_descs[22]->col_name());
    EXPECT_EQ("QUERY_REWRITE_STATUS", slot_descs[23]->col_name());
    EXPECT_EQ("CREATOR", slot_descs[24]->col_name());
    EXPECT_EQ("LAST_REFRESH_PROCESS_TIME", slot_descs[25]->col_name());
    EXPECT_EQ("LAST_REFRESH_JOB_ID", slot_descs[26]->col_name());
}

TEST_F(SchemaMaterializedViewsScannerTest, test_uninitialized_scanner) {
    SchemaMaterializedViewsScanner scanner;

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    // Should fail because scanner is not initialized
    EXPECT_FALSE(scanner.get_next(&chunk, &eos).ok());
}

TEST_F(SchemaMaterializedViewsScannerTest, test_null_pointer_parameters) {
    SchemaMaterializedViewsScanner scanner;

    EXPECT_OK(scanner.init(&_params, &_pool));

    // Test with null chunk pointer
    bool eos = false;
    EXPECT_FALSE(scanner.get_next(nullptr, &eos).ok());

    // Test with null eos pointer
    auto chunk = create_chunk(scanner.get_slot_descs());
    EXPECT_FALSE(scanner.get_next(&chunk, nullptr).ok());
}

TEST_F(SchemaMaterializedViewsScannerTest, test_empty_database_list) {
    SchemaMaterializedViewsScanner scanner;

    EXPECT_OK(scanner.init(&_params, &_pool));

    // Mock empty database result
    scanner._db_result.dbs.clear();
    scanner._db_index = 0;
    scanner._table_index = 0;
    scanner._mv_results.materialized_views.clear();

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_TRUE(eos);
    EXPECT_EQ(0, chunk->num_rows());
}

TEST_F(SchemaMaterializedViewsScannerTest, test_empty_materialized_views) {
    SchemaMaterializedViewsScanner scanner;

    EXPECT_OK(scanner.init(&_params, &_pool));

    // Mock database with no materialized views
    scanner._db_result.dbs = {"test_db"};
    scanner._db_index = 1;
    scanner._table_index = 0;
    scanner._mv_results.materialized_views.clear();

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_TRUE(eos);
    EXPECT_EQ(0, chunk->num_rows());
}

TEST_F(SchemaMaterializedViewsScannerTest, test_single_materialized_view) {
    SchemaMaterializedViewsScanner scanner;

    EXPECT_OK(scanner.init(&_params, &_pool));

    // Mock database and materialized view data
    scanner._db_result.dbs = {"test_db"};
    scanner._db_index = 1;
    scanner._table_index = 0;

    TMaterializedViewStatus mv;
    mv.__set_id("1001");
    mv.__set_database_name("test_db");
    mv.__set_name("test_mv");
    mv.__set_refresh_type("ASYNC");
    mv.__set_is_active("true");
    mv.__set_inactive_reason("");
    mv.__set_partition_type("RANGE");
    mv.__set_task_id("2001");
    mv.__set_task_name("test_task");
    mv.__set_last_refresh_start_time("2025-01-01 10:00:00");
    mv.__set_last_refresh_finished_time("2025-01-01 10:05:00");
    mv.__set_last_refresh_duration("300.5");
    mv.__set_last_refresh_state("SUCCESS");
    mv.__set_last_refresh_force_refresh("false");
    mv.__set_last_refresh_start_partition("p20250101");
    mv.__set_last_refresh_end_partition("p20250101");
    mv.__set_last_refresh_base_refresh_partitions("p20250101");
    mv.__set_last_refresh_mv_refresh_partitions("p20250101");
    mv.__set_last_refresh_error_code("");
    mv.__set_last_refresh_error_message("");
    mv.__set_rows("1000");
    mv.__set_text("SELECT * FROM base_table");
    mv.__set_extra_message("");
    mv.__set_query_rewrite_status("ENABLED");
    mv.__set_creator("admin");
    mv.__set_last_refresh_process_time("2025-01-01 10:04:30");
    mv.__set_last_refresh_job_id("job_001");

    scanner._mv_results.materialized_views = {mv};

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_FALSE(eos);
    EXPECT_EQ(1, chunk->num_rows());

    auto row = chunk->debug_row(0);
    EXPECT_TRUE(row.find("1001") != std::string::npos);                     // MATERIALIZED_VIEW_ID
    EXPECT_TRUE(row.find("test_db") != std::string::npos);                  // TABLE_SCHEMA
    EXPECT_TRUE(row.find("test_mv") != std::string::npos);                  // TABLE_NAME
    EXPECT_TRUE(row.find("ASYNC") != std::string::npos);                    // REFRESH_TYPE
    EXPECT_TRUE(row.find("true") != std::string::npos);                     // IS_ACTIVE
    EXPECT_TRUE(row.find("RANGE") != std::string::npos);                    // PARTITION_TYPE
    EXPECT_TRUE(row.find("2001") != std::string::npos);                     // TASK_ID
    EXPECT_TRUE(row.find("test_task") != std::string::npos);                // TASK_NAME
    EXPECT_TRUE(row.find("2025-01-01 10:00:00") != std::string::npos);      // LAST_REFRESH_START_TIME
    EXPECT_TRUE(row.find("2025-01-01 10:05:00") != std::string::npos);      // LAST_REFRESH_FINISHED_TIME
    EXPECT_TRUE(row.find("300.5") != std::string::npos);                    // LAST_REFRESH_DURATION
    EXPECT_TRUE(row.find("SUCCESS") != std::string::npos);                  // LAST_REFRESH_STATE
    EXPECT_TRUE(row.find("false") != std::string::npos);                    // LAST_REFRESH_FORCE_REFRESH
    EXPECT_TRUE(row.find("p20250101") != std::string::npos);                // partition info
    EXPECT_TRUE(row.find("1000") != std::string::npos);                     // TABLE_ROWS
    EXPECT_TRUE(row.find("SELECT * FROM base_table") != std::string::npos); // MATERIALIZED_VIEW_DEFINITION
    EXPECT_TRUE(row.find("ENABLED") != std::string::npos);                  // QUERY_REWRITE_STATUS
    EXPECT_TRUE(row.find("admin") != std::string::npos);                    // CREATOR
    EXPECT_TRUE(row.find("2025-01-01 10:04:30") != std::string::npos);      // LAST_REFRESH_PROCESS_TIME
    EXPECT_TRUE(row.find("job_001") != std::string::npos);                  // LAST_REFRESH_JOB_ID

    chunk->reset();
    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_TRUE(eos);
}

TEST_F(SchemaMaterializedViewsScannerTest, test_multiple_materialized_views) {
    SchemaMaterializedViewsScanner scanner;

    EXPECT_OK(scanner.init(&_params, &_pool));

    // Mock database and multiple materialized views
    scanner._db_result.dbs = {"test_db"};
    scanner._db_index = 1;
    scanner._table_index = 0;

    std::vector<TMaterializedViewStatus> mvs;

    for (int i = 1; i <= 3; ++i) {
        TMaterializedViewStatus mv;
        mv.__set_id(std::to_string(1000 + i));
        mv.__set_database_name("test_db");
        mv.__set_name("test_mv_" + std::to_string(i));
        mv.__set_refresh_type(i % 2 == 0 ? "SYNC" : "ASYNC");
        mv.__set_is_active(i % 2 == 0 ? "true" : "false");
        mv.__set_inactive_reason(i % 2 == 0 ? "" : "Base table dropped");
        mv.__set_partition_type(i % 3 == 0 ? "RANGE" : "LIST");
        mv.__set_task_id(std::to_string(2000 + i));
        mv.__set_task_name("task_" + std::to_string(i));
        mv.__set_last_refresh_start_time("2025-01-01 10:00:00");
        mv.__set_last_refresh_finished_time("2025-01-01 10:05:00");
        mv.__set_last_refresh_duration(std::to_string(300 + i));
        mv.__set_last_refresh_state("SUCCESS");
        mv.__set_last_refresh_force_refresh("false");
        mv.__set_last_refresh_start_partition("p20250101");
        mv.__set_last_refresh_end_partition("p20250101");
        mv.__set_last_refresh_base_refresh_partitions("p20250101");
        mv.__set_last_refresh_mv_refresh_partitions("p20250101");
        mv.__set_last_refresh_error_code("");
        mv.__set_last_refresh_error_message("");
        mv.__set_rows(std::to_string(1000 * i));
        mv.__set_text("SELECT * FROM base_table_" + std::to_string(i));
        mv.__set_extra_message("Extra info " + std::to_string(i));
        mv.__set_query_rewrite_status("ENABLED");
        mv.__set_creator("user_" + std::to_string(i));
        mv.__set_last_refresh_process_time("2025-01-01 10:04:30");
        mv.__set_last_refresh_job_id("job_" + std::to_string(i));

        mvs.push_back(mv);
    }

    scanner._mv_results.materialized_views = mvs;

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;
    int count = 0;

    while (!eos) {
        EXPECT_OK(scanner.get_next(&chunk, &eos));
        if (!eos) {
            EXPECT_EQ(1, chunk->num_rows());
            auto row = chunk->debug_row(0);
            EXPECT_TRUE(row.find("test_mv_" + std::to_string(count + 1)) != std::string::npos);
            count++;
        }
        chunk->reset();
    }

    EXPECT_EQ(3, count);
}

TEST_F(SchemaMaterializedViewsScannerTest, test_null_values) {
    SchemaMaterializedViewsScanner scanner;

    EXPECT_OK(scanner.init(&_params, &_pool));

    // Mock database and materialized view with null values
    scanner._db_result.dbs = {"test_db"};
    scanner._db_index = 1;
    scanner._table_index = 0;

    TMaterializedViewStatus mv;
    // Only set a few fields, leave others as null
    mv.__set_id("1001");
    mv.__set_database_name("test_db");
    mv.__set_name("test_mv");
    // All other fields are not set, so they should be null

    scanner._mv_results.materialized_views = {mv};

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_FALSE(eos);
    EXPECT_EQ(1, chunk->num_rows());

    auto row = chunk->debug_row(0);
    EXPECT_TRUE(row.find("1001") != std::string::npos);    // MATERIALIZED_VIEW_ID
    EXPECT_TRUE(row.find("test_db") != std::string::npos); // TABLE_SCHEMA
    EXPECT_TRUE(row.find("test_mv") != std::string::npos); // TABLE_NAME
    EXPECT_TRUE(row.find("NULL") != std::string::npos);    // Should have null values
}

TEST_F(SchemaMaterializedViewsScannerTest, test_invalid_numeric_values) {
    SchemaMaterializedViewsScanner scanner;

    EXPECT_OK(scanner.init(&_params, &_pool));

    // Mock database and materialized view with invalid numeric values
    scanner._db_result.dbs = {"test_db"};
    scanner._db_index = 1;
    scanner._table_index = 0;

    TMaterializedViewStatus mv;
    mv.__set_id("invalid_id"); // Invalid numeric string
    mv.__set_database_name("test_db");
    mv.__set_name("test_mv");
    mv.__set_task_id("invalid_task_id");                // Invalid numeric string
    mv.__set_last_refresh_duration("invalid_duration"); // Invalid numeric string
    mv.__set_rows("invalid_rows");                      // Invalid numeric string

    scanner._mv_results.materialized_views = {mv};

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_FALSE(eos);
    EXPECT_EQ(1, chunk->num_rows());

    auto row = chunk->debug_row(0);
    EXPECT_TRUE(row.find("test_db") != std::string::npos); // TABLE_SCHEMA
    EXPECT_TRUE(row.find("test_mv") != std::string::npos); // TABLE_NAME
    // Invalid numeric values should be handled gracefully (likely as null)
    EXPECT_TRUE(row.find("NULL") != std::string::npos);
}
} // namespace starrocks