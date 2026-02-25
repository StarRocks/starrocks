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

#include "exec/schema_scanner/schema_task_runs_scanner.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "column/column_helper.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class SchemaTaskRunsScannerTest : public ::testing::Test {
private:
    ChunkPtr create_chunk(const std::vector<SlotDescriptor*> slot_descs) {
        ChunkPtr chunk = std::make_shared<Chunk>();
        for (const auto* slot_desc : slot_descs) {
            MutableColumnPtr column = ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());
            chunk->append_column(std::move(column), slot_desc->id());
        }
        return chunk;
    }

    void init_scanner(SchemaTaskRunsScanner& scanner) {
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

TEST_F(SchemaTaskRunsScannerTest, test_scanner_initialization) {
    SchemaTaskRunsScanner scanner;
    init_scanner(scanner);

    // Test that scanner has the correct number of columns
    auto slot_descs = scanner.get_slot_descs();
    EXPECT_EQ(16, slot_descs.size());

    // Test column names and types
    EXPECT_EQ("QUERY_ID", slot_descs[0]->col_name());
    EXPECT_EQ("TASK_NAME", slot_descs[1]->col_name());
    EXPECT_EQ("CREATE_TIME", slot_descs[2]->col_name());
    EXPECT_EQ("FINISH_TIME", slot_descs[3]->col_name());
    EXPECT_EQ("STATE", slot_descs[4]->col_name());
    EXPECT_EQ("CATALOG", slot_descs[5]->col_name());
    EXPECT_EQ("DATABASE", slot_descs[6]->col_name());
    EXPECT_EQ("DEFINITION", slot_descs[7]->col_name());
    EXPECT_EQ("EXPIRE_TIME", slot_descs[8]->col_name());
    EXPECT_EQ("ERROR_CODE", slot_descs[9]->col_name());
    EXPECT_EQ("ERROR_MESSAGE", slot_descs[10]->col_name());
    EXPECT_EQ("PROGRESS", slot_descs[11]->col_name());
    EXPECT_EQ("EXTRA_MESSAGE", slot_descs[12]->col_name());
    EXPECT_EQ("PROPERTIES", slot_descs[13]->col_name());
    EXPECT_EQ("JOB_ID", slot_descs[14]->col_name());
    EXPECT_EQ("PROCESS_TIME", slot_descs[15]->col_name());
}

TEST_F(SchemaTaskRunsScannerTest, test_uninitialized_scanner) {
    SchemaTaskRunsScanner scanner;

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    // Should fail because scanner is not initialized
    EXPECT_FALSE(scanner.get_next(&chunk, &eos).ok());
}

TEST_F(SchemaTaskRunsScannerTest, test_null_pointer_parameters) {
    SchemaTaskRunsScanner scanner;
    init_scanner(scanner);

    // Test with null chunk pointer
    bool eos = false;
    EXPECT_FALSE(scanner.get_next(nullptr, &eos).ok());

    // Test with null eos pointer
    auto chunk = create_chunk(scanner.get_slot_descs());
    EXPECT_FALSE(scanner.get_next(&chunk, nullptr).ok());
}

TEST_F(SchemaTaskRunsScannerTest, test_empty_task_runs_list) {
    SchemaTaskRunsScanner scanner;
    init_scanner(scanner);

    // Mock empty task runs result
    scanner._task_run_result.task_runs.clear();
    scanner._task_run_index = 0;

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_TRUE(eos);
    EXPECT_EQ(0, chunk->num_rows());
}

TEST_F(SchemaTaskRunsScannerTest, test_single_task_run) {
    SchemaTaskRunsScanner scanner;
    init_scanner(scanner);

    // Mock task run data
    TTaskRunInfo task_run;
    task_run.__set_query_id("query_001");
    task_run.__set_task_name("test_task");
    task_run.__set_create_time(1640995200); // 2022-01-01 00:00:00
    task_run.__set_finish_time(1640995500); // 2022-01-01 00:05:00
    task_run.__set_state("SUCCESS");
    task_run.__set_catalog("default_catalog");
    task_run.__set_database("test_db");
    task_run.__set_definition("SELECT * FROM test_table");
    task_run.__set_expire_time(1641081600); // 2022-01-02 00:00:00
    task_run.__set_error_code(0);
    task_run.__set_error_message("");
    task_run.__set_progress("100%");
    task_run.__set_extra_message("Task completed successfully");
    task_run.__set_properties("{\"priority\":\"high\"}");
    task_run.__set_job_id("job_001");
    task_run.__set_process_time(1640995400); // 2022-01-01 00:03:20

    scanner._task_run_result.task_runs = {task_run};
    scanner._task_run_index = 0;

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_FALSE(eos);
    EXPECT_EQ(1, chunk->num_rows());

    auto row = chunk->debug_row(0);
    EXPECT_TRUE(row.find("query_001") != std::string::npos);                   // QUERY_ID
    EXPECT_TRUE(row.find("test_task") != std::string::npos);                   // TASK_NAME
    EXPECT_TRUE(row.find("2022-01-01") != std::string::npos);                  // CREATE_TIME
    EXPECT_TRUE(row.find("2022-01-01") != std::string::npos);                  // FINISH_TIME
    EXPECT_TRUE(row.find("SUCCESS") != std::string::npos);                     // STATE
    EXPECT_TRUE(row.find("default_catalog") != std::string::npos);             // CATALOG
    EXPECT_TRUE(row.find("test_db") != std::string::npos);                     // DATABASE
    EXPECT_TRUE(row.find("SELECT * FROM test_table") != std::string::npos);    // DEFINITION
    EXPECT_TRUE(row.find("2022-01-02") != std::string::npos);                  // EXPIRE_TIME
    EXPECT_TRUE(row.find("0") != std::string::npos);                           // ERROR_CODE
    EXPECT_TRUE(row.find("100%") != std::string::npos);                        // PROGRESS
    EXPECT_TRUE(row.find("Task completed successfully") != std::string::npos); // EXTRA_MESSAGE
    EXPECT_TRUE(row.find("{\"priority\":\"high\"}") != std::string::npos);     // PROPERTIES
    EXPECT_TRUE(row.find("job_001") != std::string::npos);                     // JOB_ID
    EXPECT_TRUE(row.find("2022-01-01") != std::string::npos);                  // PROCESS_TIME

    chunk->reset();
    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_TRUE(eos);
}

TEST_F(SchemaTaskRunsScannerTest, test_multiple_task_runs) {
    SchemaTaskRunsScanner scanner;
    init_scanner(scanner);

    // Mock multiple task runs
    std::vector<TTaskRunInfo> task_runs;

    for (int i = 1; i <= 3; ++i) {
        TTaskRunInfo task_run;
        task_run.__set_query_id("query_00" + std::to_string(i));
        task_run.__set_task_name("task_" + std::to_string(i));
        task_run.__set_create_time(1640995200 + i * 3600); // Increment by 1 hour
        task_run.__set_finish_time(1640995500 + i * 3600);
        task_run.__set_state(i % 2 == 0 ? "SUCCESS" : "FAILED");
        task_run.__set_catalog("catalog_" + std::to_string(i));
        task_run.__set_database("db_" + std::to_string(i));
        task_run.__set_definition("SELECT * FROM table_" + std::to_string(i));
        task_run.__set_expire_time(1641081600 + i * 3600);
        task_run.__set_error_code(i % 2 == 0 ? 0 : 1001);
        task_run.__set_error_message(i % 2 == 0 ? "" : "Task failed");
        task_run.__set_progress(std::to_string(100 - i * 10) + "%");
        task_run.__set_extra_message("Extra info " + std::to_string(i));
        task_run.__set_properties("{\"priority\":\"medium\",\"id\":" + std::to_string(i) + "}");
        task_run.__set_job_id("job_00" + std::to_string(i));
        task_run.__set_process_time(1640995400 + i * 3600);

        task_runs.push_back(task_run);
    }

    scanner._task_run_result.task_runs = task_runs;
    scanner._task_run_index = 0;

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;
    int count = 0;

    while (!eos) {
        EXPECT_OK(scanner.get_next(&chunk, &eos));
        if (!eos) {
            EXPECT_EQ(1, chunk->num_rows());
            auto row = chunk->debug_row(0);
            EXPECT_TRUE(row.find("task_" + std::to_string(count + 1)) != std::string::npos);
            count++;
        }
        chunk->reset();
    }

    EXPECT_EQ(3, count);
}

TEST_F(SchemaTaskRunsScannerTest, test_null_values) {
    SchemaTaskRunsScanner scanner;
    init_scanner(scanner);

    // Mock task run with minimal data (most fields will be null)
    TTaskRunInfo task_run;
    task_run.__set_query_id("query_001");
    task_run.__set_task_name("test_task");
    // All other fields are not set, so they should be null

    scanner._task_run_result.task_runs = {task_run};
    scanner._task_run_index = 0;

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_FALSE(eos);
    EXPECT_EQ(1, chunk->num_rows());

    auto row = chunk->debug_row(0);
    EXPECT_TRUE(row.find("query_001") != std::string::npos); // QUERY_ID
    EXPECT_TRUE(row.find("test_task") != std::string::npos); // TASK_NAME
    EXPECT_TRUE(row.find("NULL") != std::string::npos);      // Should have null values
}

TEST_F(SchemaTaskRunsScannerTest, test_invalid_timestamp_values) {
    SchemaTaskRunsScanner scanner;
    init_scanner(scanner);

    // Mock task run with invalid timestamp values
    TTaskRunInfo task_run;
    task_run.__set_query_id("query_001");
    task_run.__set_task_name("test_task");
    task_run.__set_create_time(-1);   // Invalid timestamp
    task_run.__set_finish_time(0);    // Invalid timestamp
    task_run.__set_expire_time(-100); // Invalid timestamp
    task_run.__set_process_time(0);   // Invalid timestamp

    scanner._task_run_result.task_runs = {task_run};
    scanner._task_run_index = 0;

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_FALSE(eos);
    EXPECT_EQ(1, chunk->num_rows());

    auto row = chunk->debug_row(0);
    EXPECT_TRUE(row.find("query_001") != std::string::npos); // QUERY_ID
    EXPECT_TRUE(row.find("test_task") != std::string::npos); // TASK_NAME
    // Invalid timestamps should be handled as null
    EXPECT_TRUE(row.find("NULL") != std::string::npos);
}

TEST_F(SchemaTaskRunsScannerTest, test_task_run_with_error) {
    SchemaTaskRunsScanner scanner;
    init_scanner(scanner);

    // Mock task run with long string values
    TTaskRunInfo task_run;
    task_run.__set_query_id("very_long_query_id_that_exceeds_normal_length_123456789");
    task_run.__set_task_name("very_long_task_name_with_many_characters_abcdefghijklmnopqrstuvwxyz");

    std::string long_definition =
            "SELECT * FROM very_long_table_name_that_has_many_characters "
            "WHERE very_long_column_name_that_also_has_many_characters = "
            "'very_long_string_value_that_might_cause_issues_if_not_handled_properly' "
            "AND another_very_long_condition = 'another_very_long_value'";
    task_run.__set_definition(long_definition);

    std::string long_error_message =
            "This is a very long error message that contains detailed information "
            "about what went wrong during the task execution. It includes multiple "
            "lines of text and various technical details that might be useful for "
            "debugging purposes.";
    task_run.__set_error_message(long_error_message);

    std::string long_extra_message =
            "This is a very long extra message that provides additional context "
            "about the task execution. It might contain debugging information, "
            "performance metrics, or other relevant details.";
    task_run.__set_extra_message(long_extra_message);

    std::string long_properties =
            "{\"very_long_property_name_1\":\"very_long_property_value_1\","
            "\"very_long_property_name_2\":\"very_long_property_value_2\","
            "\"very_long_property_name_3\":\"very_long_property_value_3\"}";
    task_run.__set_properties(long_properties);

    scanner._task_run_result.task_runs = {task_run};
    scanner._task_run_index = 0;

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_FALSE(eos);
    EXPECT_EQ(1, chunk->num_rows());

    auto row = chunk->debug_row(0);
    EXPECT_TRUE(row.find("very_long_query_id_that_exceeds_normal_length_123456789") != std::string::npos);
    EXPECT_TRUE(row.find("very_long_task_name_with_many_characters_abcdefghijklmnopqrstuvwxyz") != std::string::npos);
    EXPECT_TRUE(row.find("SELECT * FROM very_long_table_name_that_has_many_characters") != std::string::npos);
    EXPECT_TRUE(row.find("This is a very long error message that contains detailed information") != std::string::npos);
    EXPECT_TRUE(row.find("This is a very long extra message that provides additional context") != std::string::npos);
    EXPECT_TRUE(row.find("very_long_property_name_1") != std::string::npos);
}

TEST_F(SchemaTaskRunsScannerTest, test_task_run_with_special_characters) {
    SchemaTaskRunsScanner scanner;
    init_scanner(scanner);

    // Mock task run with special characters in strings
    TTaskRunInfo task_run;
    task_run.__set_query_id("query_with_special_chars_!@#$%^&*()");
    task_run.__set_task_name("task_with_unicode_测试任务");
    task_run.__set_state("RUNNING");
    task_run.__set_catalog("catalog_with_spaces and special chars");
    task_run.__set_database("database_with_quotes\"and'apostrophes");
    task_run.__set_definition("SELECT * FROM table WHERE name = 'test\"quote' AND value = 'test'quote'");
    task_run.__set_error_message("Error with special chars: \n\t\r\b\f");
    task_run.__set_extra_message("Extra message with unicode: 测试信息");
    task_run.__set_properties("{\"key_with_spaces\":\"value with spaces\",\"unicode_key\":\"unicode_value_测试\"}");
    task_run.__set_job_id("job_with_special_chars_!@#$%^&*()");

    scanner._task_run_result.task_runs = {task_run};
    scanner._task_run_index = 0;

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_FALSE(eos);
    EXPECT_EQ(1, chunk->num_rows());

    auto row = chunk->debug_row(0);
    EXPECT_TRUE(row.find("query_with_special_chars_!@#$%^&*()") != std::string::npos);
    EXPECT_TRUE(row.find("task_with_unicode_测试任务") != std::string::npos);
    EXPECT_TRUE(row.find("RUNNING") != std::string::npos);
    EXPECT_TRUE(row.find("catalog_with_spaces and special chars") != std::string::npos);
    EXPECT_TRUE(row.find("database_with_quotes\"and'apostrophes") != std::string::npos);
    EXPECT_TRUE(row.find("SELECT * FROM table WHERE name = 'test\"quote'") != std::string::npos);
    EXPECT_TRUE(row.find("Error with special chars:") != std::string::npos);
    EXPECT_TRUE(row.find("Extra message with unicode: 测试信息") != std::string::npos);
    EXPECT_TRUE(row.find("key_with_spaces") != std::string::npos);
    EXPECT_TRUE(row.find("job_with_special_chars_!@#$%^&*()") != std::string::npos);
}

TEST_F(SchemaTaskRunsScannerTest, test_task_run_with_empty_strings) {
    SchemaTaskRunsScanner scanner;
    init_scanner(scanner);

    // Mock task run with empty string values
    TTaskRunInfo task_run;
    task_run.__set_query_id("query_001");
    task_run.__set_task_name("test_task");
    task_run.__set_state("");
    task_run.__set_catalog("");
    task_run.__set_database("");
    task_run.__set_definition("");
    task_run.__set_error_message("");
    task_run.__set_progress("");
    task_run.__set_extra_message("");
    task_run.__set_properties("");
    task_run.__set_job_id("");

    scanner._task_run_result.task_runs = {task_run};
    scanner._task_run_index = 0;

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_FALSE(eos);
    EXPECT_EQ(1, chunk->num_rows());

    auto row = chunk->debug_row(0);
    EXPECT_TRUE(row.find("query_001") != std::string::npos); // QUERY_ID
    EXPECT_TRUE(row.find("test_task") != std::string::npos); // TASK_NAME
    // Empty strings should be handled properly (likely as empty strings, not null)
}

TEST_F(SchemaTaskRunsScannerTest, test_task_run_with_large_error_code) {
    SchemaTaskRunsScanner scanner;
    init_scanner(scanner);

    // Mock task run with large error code
    TTaskRunInfo task_run;
    task_run.__set_query_id("query_001");
    task_run.__set_task_name("test_task");
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Woverflow"
    task_run.__set_error_code(9223372036854775807LL); // MAX_INT64
    task_run.__set_error_message("Error with maximum error code");

    scanner._task_run_result.task_runs = {task_run};
    scanner._task_run_index = 0;

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_FALSE(eos);
    EXPECT_EQ(1, chunk->num_rows());

    auto row = chunk->debug_row(0);
    EXPECT_TRUE(row.find("query_001") != std::string::npos);
    EXPECT_TRUE(row.find("test_task") != std::string::npos);
    EXPECT_TRUE(row.find("-1") != std::string::npos); // ERROR_CODE
    EXPECT_TRUE(row.find("Error with maximum error code") != std::string::npos);
#pragma GCC diagnostic pop
}

} // namespace starrocks