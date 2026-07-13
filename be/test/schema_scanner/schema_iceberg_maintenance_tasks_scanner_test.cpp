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

#include "schema_scanner/schema_iceberg_maintenance_tasks_scanner.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "column/column_helper.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class SchemaIcebergMaintenanceTasksScannerTest : public ::testing::Test {
private:
    ChunkPtr create_chunk(const std::vector<SlotDescriptor*> slot_descs) {
        ChunkPtr chunk = std::make_shared<Chunk>();
        for (const auto* slot_desc : slot_descs) {
            MutableColumnPtr column = ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());
            chunk->append_column(std::move(column), slot_desc->id());
        }
        return chunk;
    }

    void init_scanner(SchemaIcebergMaintenanceTasksScanner& scanner) {
        EXPECT_OK(scanner.init(&_params, &_pool));
        scanner._runtime_state = _state.get();
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

TEST_F(SchemaIcebergMaintenanceTasksScannerTest, test_scanner_initialization) {
    SchemaIcebergMaintenanceTasksScanner scanner;
    init_scanner(scanner);

    auto slot_descs = scanner.get_slot_descs();
    EXPECT_EQ(13, slot_descs.size());

    EXPECT_EQ("TASK_ID", slot_descs[0]->col_name());
    EXPECT_EQ("CATALOG_NAME", slot_descs[1]->col_name());
    EXPECT_EQ("DATABASE_NAME", slot_descs[2]->col_name());
    EXPECT_EQ("TABLE_NAME", slot_descs[3]->col_name());
    EXPECT_EQ("ACTION", slot_descs[4]->col_name());
    EXPECT_EQ("TRIGGER_REASON", slot_descs[5]->col_name());
    EXPECT_EQ("STMT", slot_descs[6]->col_name());
    EXPECT_EQ("START_TIME", slot_descs[7]->col_name());
    EXPECT_EQ("END_TIME", slot_descs[8]->col_name());
    EXPECT_EQ("DURATION_MS", slot_descs[9]->col_name());
    EXPECT_EQ("STATUS", slot_descs[10]->col_name());
    EXPECT_EQ("FAILURE_REASON", slot_descs[11]->col_name());
    EXPECT_EQ("DETAILS", slot_descs[12]->col_name());
}

TEST_F(SchemaIcebergMaintenanceTasksScannerTest, test_uninitialized_scanner) {
    SchemaIcebergMaintenanceTasksScanner scanner;

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    EXPECT_FALSE(scanner.get_next(&chunk, &eos).ok());
}

TEST_F(SchemaIcebergMaintenanceTasksScannerTest, test_null_pointer_parameters) {
    SchemaIcebergMaintenanceTasksScanner scanner;
    init_scanner(scanner);

    bool eos = false;
    EXPECT_FALSE(scanner.get_next(nullptr, &eos).ok());

    auto chunk = create_chunk(scanner.get_slot_descs());
    EXPECT_FALSE(scanner.get_next(&chunk, nullptr).ok());
}

TEST_F(SchemaIcebergMaintenanceTasksScannerTest, test_empty_task_list) {
    SchemaIcebergMaintenanceTasksScanner scanner;
    init_scanner(scanner);

    scanner._task_result.tasks.clear();
    scanner._task_index = 0;

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_TRUE(eos);
    EXPECT_EQ(0, chunk->num_rows());
}

TEST_F(SchemaIcebergMaintenanceTasksScannerTest, test_single_task) {
    SchemaIcebergMaintenanceTasksScanner scanner;
    init_scanner(scanner);

    TIcebergMaintenanceTaskInfo task;
    task.__set_task_id("task_001");
    task.__set_catalog_name("iceberg_catalog");
    task.__set_database_name("test_db");
    task.__set_table_name("test_table");
    task.__set_action("expire_snapshots");
    task.__set_trigger_reason("schedule");
    task.__set_start_time(1640995200); // 2022-01-01 00:00:00
    task.__set_end_time(1640995500);   // 2022-01-01 00:05:00
    task.__set_duration_ms(300000);
    task.__set_status("success");
    task.__set_details("{\"snapshot_count_input\":10,\"snapshot_count_output\":3}");

    scanner._task_result.tasks = {task};
    scanner._task_index = 0;

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_FALSE(eos);
    EXPECT_EQ(1, chunk->num_rows());

    auto row = chunk->debug_row(0);
    EXPECT_TRUE(row.find("task_001") != std::string::npos);             // TASK_ID
    EXPECT_TRUE(row.find("iceberg_catalog") != std::string::npos);      // CATALOG_NAME
    EXPECT_TRUE(row.find("test_db") != std::string::npos);              // DATABASE_NAME
    EXPECT_TRUE(row.find("test_table") != std::string::npos);           // TABLE_NAME
    EXPECT_TRUE(row.find("expire_snapshots") != std::string::npos);     // ACTION
    EXPECT_TRUE(row.find("schedule") != std::string::npos);             // TRIGGER_REASON
    EXPECT_TRUE(row.find("2022-01-01") != std::string::npos);           // START_TIME / END_TIME
    EXPECT_TRUE(row.find("300000") != std::string::npos);               // DURATION_MS
    EXPECT_TRUE(row.find("success") != std::string::npos);              // STATUS
    EXPECT_TRUE(row.find("snapshot_count_input") != std::string::npos); // DETAILS

    // next call reaches the end
    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_TRUE(eos);
}

TEST_F(SchemaIcebergMaintenanceTasksScannerTest, test_task_with_null_optional_fields) {
    SchemaIcebergMaintenanceTasksScanner scanner;
    init_scanner(scanner);

    // only the four identity columns are set; every optional column is left unset
    TIcebergMaintenanceTaskInfo task;
    task.__set_task_id("task_null");
    task.__set_catalog_name("iceberg_catalog");
    task.__set_database_name("test_db");
    task.__set_table_name("test_table");

    scanner._task_result.tasks = {task};
    scanner._task_index = 0;

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;
    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_FALSE(eos);
    EXPECT_EQ(1, chunk->num_rows());

    // identity columns are always populated
    EXPECT_FALSE(chunk->get_column_by_slot_id(1)->is_null(0)); // TASK_ID
    EXPECT_EQ("task_null", chunk->get_column_by_slot_id(1)->get(0).get_slice().to_string());
    EXPECT_FALSE(chunk->get_column_by_slot_id(4)->is_null(0)); // TABLE_NAME

    // every unset optional column materializes as NULL
    EXPECT_TRUE(chunk->get_column_by_slot_id(5)->is_null(0));  // ACTION
    EXPECT_TRUE(chunk->get_column_by_slot_id(6)->is_null(0));  // TRIGGER_REASON
    EXPECT_TRUE(chunk->get_column_by_slot_id(7)->is_null(0));  // STMT
    EXPECT_TRUE(chunk->get_column_by_slot_id(8)->is_null(0));  // START_TIME
    EXPECT_TRUE(chunk->get_column_by_slot_id(9)->is_null(0));  // END_TIME
    EXPECT_TRUE(chunk->get_column_by_slot_id(10)->is_null(0)); // DURATION_MS
    EXPECT_TRUE(chunk->get_column_by_slot_id(11)->is_null(0)); // STATUS
    EXPECT_TRUE(chunk->get_column_by_slot_id(12)->is_null(0)); // FAILURE_REASON
    EXPECT_TRUE(chunk->get_column_by_slot_id(13)->is_null(0)); // DETAILS

    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_TRUE(eos);
}

TEST_F(SchemaIcebergMaintenanceTasksScannerTest, test_multiple_tasks) {
    SchemaIcebergMaintenanceTasksScanner scanner;
    init_scanner(scanner);

    TIcebergMaintenanceTaskInfo t1;
    t1.__set_task_id("task_001");
    t1.__set_catalog_name("cat1");
    t1.__set_database_name("db1");
    t1.__set_table_name("tbl1");
    t1.__set_action("expire_snapshots");
    t1.__set_status("success");
    // t1 leaves failure_reason unset

    TIcebergMaintenanceTaskInfo t2;
    t2.__set_task_id("task_002");
    t2.__set_catalog_name("cat2");
    t2.__set_database_name("db2");
    t2.__set_table_name("tbl2");
    t2.__set_action("rewrite_manifests");
    t2.__set_status("failed");
    t2.__set_failure_reason("boom");

    scanner._task_result.tasks = {t1, t2};
    scanner._task_index = 0;

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    // one row is appended per get_next call, accumulating in the same chunk
    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_FALSE(eos);
    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_FALSE(eos);
    EXPECT_EQ(2, chunk->num_rows());

    // row order is preserved
    EXPECT_EQ("task_001", chunk->get_column_by_slot_id(1)->get(0).get_slice().to_string());
    EXPECT_EQ("cat1", chunk->get_column_by_slot_id(2)->get(0).get_slice().to_string());
    EXPECT_EQ("success", chunk->get_column_by_slot_id(11)->get(0).get_slice().to_string());
    EXPECT_TRUE(chunk->get_column_by_slot_id(12)->is_null(0)); // t1 failure_reason unset -> NULL

    EXPECT_EQ("task_002", chunk->get_column_by_slot_id(1)->get(1).get_slice().to_string());
    EXPECT_EQ("rewrite_manifests", chunk->get_column_by_slot_id(5)->get(1).get_slice().to_string());
    EXPECT_EQ("failed", chunk->get_column_by_slot_id(11)->get(1).get_slice().to_string());
    EXPECT_FALSE(chunk->get_column_by_slot_id(12)->is_null(1)); // t2 failure_reason set
    EXPECT_EQ("boom", chunk->get_column_by_slot_id(12)->get(1).get_slice().to_string());

    // exhausted after both tasks
    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_TRUE(eos);
}

} // namespace starrocks
