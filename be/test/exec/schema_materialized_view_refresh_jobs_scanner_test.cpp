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

#include "exec/schema_scanner/schema_materialized_view_refresh_jobs_scanner.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "runtime/runtime_state.h"
#include "types/timestamp_value.h"

namespace starrocks {

class SchemaMaterializedViewRefreshJobsScannerTest : public ::testing::Test {
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

    // get_next() only inspects _is_init plus the injected result vector; init() flips
    // _is_init so the test can drive fill_chunk without standing up the FE RPC start() performs.
    void init_scanner(SchemaMaterializedViewRefreshJobsScanner& scanner) { EXPECT_OK(scanner.init(&_params, &_pool)); }

    SchemaScannerParam _params;
    std::string _ip = "127.0.0.1";
    ObjectPool _pool;
    std::unique_ptr<RuntimeState> _state;

    // Slot ids are 1-based and follow _s_tbls_columns order in the scanner.
    static constexpr int JOB_ID = 1;
    static constexpr int MATERIALIZED_VIEW_ID = 2;
    static constexpr int TABLE_SCHEMA = 3;
    static constexpr int TABLE_NAME = 4;
    static constexpr int TASK_ID = 5;
    static constexpr int SUBMIT_TIME = 11;
    static constexpr int REFRESH_STATE = 12;
    static constexpr int FINISH_TIME = 13;
    static constexpr int DURATION_TIME = 14;
};

TEST_F(SchemaMaterializedViewRefreshJobsScannerTest, test_scanner_initialization) {
    SchemaMaterializedViewRefreshJobsScanner scanner;
    init_scanner(scanner);

    auto slot_descs = scanner.get_slot_descs();
    EXPECT_EQ(23, slot_descs.size());

    EXPECT_EQ("JOB_ID", slot_descs[0]->col_name());
    EXPECT_EQ("MATERIALIZED_VIEW_ID", slot_descs[1]->col_name());
    EXPECT_EQ("TABLE_SCHEMA", slot_descs[2]->col_name());
    EXPECT_EQ("TABLE_NAME", slot_descs[3]->col_name());
    EXPECT_EQ("TASK_ID", slot_descs[4]->col_name());
    EXPECT_EQ("WAREHOUSE", slot_descs[5]->col_name());
    EXPECT_EQ("RESOURCE_GROUP", slot_descs[6]->col_name());
    EXPECT_EQ("CREATOR", slot_descs[7]->col_name());
    EXPECT_EQ("SUBMIT_USER", slot_descs[8]->col_name());
    EXPECT_EQ("RUN_AS_USER", slot_descs[9]->col_name());
    EXPECT_EQ("SUBMIT_TIME", slot_descs[10]->col_name());
    EXPECT_EQ("REFRESH_STATE", slot_descs[11]->col_name());
    EXPECT_EQ("FINISH_TIME", slot_descs[12]->col_name());
    EXPECT_EQ("DURATION_TIME", slot_descs[13]->col_name());
    EXPECT_EQ("REFRESH_TRIGGER", slot_descs[14]->col_name());
    EXPECT_EQ("REFRESH_MODE", slot_descs[15]->col_name());
    EXPECT_EQ("IMV_SOURCE_VERSION_RANGE", slot_descs[16]->col_name());
    EXPECT_EQ("IMV_SOURCE_TIMESTAMP_RANGE", slot_descs[17]->col_name());
    EXPECT_EQ("IMV_SOURCE_PINNED_SNAPSHOT_ID_MAP", slot_descs[18]->col_name());
    EXPECT_EQ("FAILED_TASK_RUN_ID", slot_descs[19]->col_name());
    EXPECT_EQ("FAILED_QUERY_ID", slot_descs[20]->col_name());
    EXPECT_EQ("ERROR_CODE", slot_descs[21]->col_name());
    EXPECT_EQ("ERROR_MESSAGE", slot_descs[22]->col_name());
}

TEST_F(SchemaMaterializedViewRefreshJobsScannerTest, test_uninitialized_scanner) {
    SchemaMaterializedViewRefreshJobsScanner scanner;

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    EXPECT_FALSE(scanner.get_next(&chunk, &eos).ok());
}

TEST_F(SchemaMaterializedViewRefreshJobsScannerTest, test_null_pointer_parameters) {
    SchemaMaterializedViewRefreshJobsScanner scanner;
    init_scanner(scanner);

    bool eos = false;
    EXPECT_FALSE(scanner.get_next(nullptr, &eos).ok());

    auto chunk = create_chunk(scanner.get_slot_descs());
    EXPECT_FALSE(scanner.get_next(&chunk, nullptr).ok());
}

TEST_F(SchemaMaterializedViewRefreshJobsScannerTest, test_empty_jobs_list) {
    SchemaMaterializedViewRefreshJobsScanner scanner;
    init_scanner(scanner);

    scanner._jobs_result.jobs.clear();
    scanner._jobs_index = 0;

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_TRUE(eos);
    EXPECT_EQ(0, chunk->num_rows());
}

TEST_F(SchemaMaterializedViewRefreshJobsScannerTest, test_single_job_all_fields) {
    SchemaMaterializedViewRefreshJobsScanner scanner;
    init_scanner(scanner);

    // Populate every string field so each fill_chunk case runs.
    TMaterializedViewRefreshJobInfo info;
    info.__set_job_id("job-001");
    info.__set_materialized_view_id("100");
    info.__set_table_schema("test_db");
    info.__set_table_name("test_mv");
    info.__set_task_id("5");
    info.__set_warehouse("wh_default");
    info.__set_resource_group("rg_default");
    info.__set_creator("admin");
    info.__set_submit_user("submit_user");
    info.__set_run_as_user("run_as_user");
    info.__set_submit_time("2024-01-01 10:00:00");
    info.__set_refresh_state("SUCCESS");
    info.__set_finish_time("2024-01-01 10:00:03");
    info.__set_duration_time("3.000");
    info.__set_refresh_trigger("MANUAL");
    info.__set_refresh_mode("PCT");
    info.__set_imv_source_version_range("{}");
    info.__set_imv_source_timestamp_range("{}");
    info.__set_imv_source_pinned_snapshot_id_map("{}");
    info.__set_failed_task_run_id("failed-run-1");
    info.__set_failed_query_id("failed-query-1");
    info.__set_error_code("0");
    info.__set_error_message("");

    scanner._jobs_result.jobs = {info};
    scanner._jobs_index = 0;

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_FALSE(eos);
    EXPECT_EQ(1, chunk->num_rows());

    // BIGINT columns parsed via stoll.
    EXPECT_FALSE(chunk->get_column_by_slot_id(MATERIALIZED_VIEW_ID)->is_null(0));
    EXPECT_EQ(100, chunk->get_column_by_slot_id(MATERIALIZED_VIEW_ID)->get(0).get_int64());
    EXPECT_FALSE(chunk->get_column_by_slot_id(TASK_ID)->is_null(0));
    EXPECT_EQ(5, chunk->get_column_by_slot_id(TASK_ID)->get(0).get_int64());

    // DOUBLE column parsed via stod.
    EXPECT_FALSE(chunk->get_column_by_slot_id(DURATION_TIME)->is_null(0));
    EXPECT_DOUBLE_EQ(3.0, chunk->get_column_by_slot_id(DURATION_TIME)->get(0).get_double());

    // DATETIME columns parsed via from_date_str.
    EXPECT_FALSE(chunk->get_column_by_slot_id(SUBMIT_TIME)->is_null(0));
    EXPECT_EQ("2024-01-01 10:00:00",
              chunk->get_column_by_slot_id(SUBMIT_TIME)->get(0).get_timestamp().to_string(/*ignore_microsecond=*/true));
    EXPECT_FALSE(chunk->get_column_by_slot_id(FINISH_TIME)->is_null(0));
    EXPECT_EQ("2024-01-01 10:00:03",
              chunk->get_column_by_slot_id(FINISH_TIME)->get(0).get_timestamp().to_string(/*ignore_microsecond=*/true));

    EXPECT_EQ("job-001", chunk->get_column_by_slot_id(JOB_ID)->get(0).get_slice().to_string());
    EXPECT_FALSE(chunk->get_column_by_slot_id(REFRESH_STATE)->is_null(0));
    EXPECT_EQ("SUCCESS", chunk->get_column_by_slot_id(REFRESH_STATE)->get(0).get_slice().to_string());
    EXPECT_EQ("test_db", chunk->get_column_by_slot_id(TABLE_SCHEMA)->get(0).get_slice().to_string());
    EXPECT_EQ("test_mv", chunk->get_column_by_slot_id(TABLE_NAME)->get(0).get_slice().to_string());

    chunk->reset();
    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_TRUE(eos);
}

TEST_F(SchemaMaterializedViewRefreshJobsScannerTest, test_unset_optional_fields_are_null) {
    SchemaMaterializedViewRefreshJobsScanner scanner;
    init_scanner(scanner);

    // Only job_id is set; every optional column should materialize as null.
    TMaterializedViewRefreshJobInfo info;
    info.__set_job_id("job-002");

    scanner._jobs_result.jobs = {info};
    scanner._jobs_index = 0;

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_FALSE(eos);
    EXPECT_EQ(1, chunk->num_rows());

    EXPECT_EQ("job-002", chunk->get_column_by_slot_id(JOB_ID)->get(0).get_slice().to_string());
    EXPECT_TRUE(chunk->get_column_by_slot_id(MATERIALIZED_VIEW_ID)->is_null(0));
    EXPECT_TRUE(chunk->get_column_by_slot_id(TASK_ID)->is_null(0));
    EXPECT_TRUE(chunk->get_column_by_slot_id(SUBMIT_TIME)->is_null(0));
    EXPECT_TRUE(chunk->get_column_by_slot_id(FINISH_TIME)->is_null(0));
    EXPECT_TRUE(chunk->get_column_by_slot_id(DURATION_TIME)->is_null(0));
    EXPECT_TRUE(chunk->get_column_by_slot_id(REFRESH_STATE)->is_null(0));
}

TEST_F(SchemaMaterializedViewRefreshJobsScannerTest, test_invalid_numeric_values_are_null) {
    SchemaMaterializedViewRefreshJobsScanner scanner;
    init_scanner(scanner);

    // Non-numeric strings hit the catch branch and fall back to null.
    TMaterializedViewRefreshJobInfo info;
    info.__set_job_id("job-003");
    info.__set_materialized_view_id("not_a_number");
    info.__set_task_id("not_a_number");
    info.__set_duration_time("not_a_number");
    info.__set_submit_time("not_a_datetime");

    scanner._jobs_result.jobs = {info};
    scanner._jobs_index = 0;

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_FALSE(eos);
    EXPECT_EQ(1, chunk->num_rows());

    EXPECT_TRUE(chunk->get_column_by_slot_id(MATERIALIZED_VIEW_ID)->is_null(0));
    EXPECT_TRUE(chunk->get_column_by_slot_id(TASK_ID)->is_null(0));
    EXPECT_TRUE(chunk->get_column_by_slot_id(DURATION_TIME)->is_null(0));
    EXPECT_TRUE(chunk->get_column_by_slot_id(SUBMIT_TIME)->is_null(0));
}

TEST_F(SchemaMaterializedViewRefreshJobsScannerTest, test_multiple_jobs) {
    SchemaMaterializedViewRefreshJobsScanner scanner;
    init_scanner(scanner);

    std::vector<TMaterializedViewRefreshJobInfo> jobs;
    for (int i = 1; i <= 3; ++i) {
        TMaterializedViewRefreshJobInfo info;
        info.__set_job_id("job-00" + std::to_string(i));
        info.__set_materialized_view_id(std::to_string(100 + i));
        info.__set_task_id(std::to_string(i));
        info.__set_refresh_state(i % 2 == 0 ? "SUCCESS" : "FAILED");
        jobs.push_back(info);
    }

    scanner._jobs_result.jobs = jobs;
    scanner._jobs_index = 0;

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;
    int count = 0;

    while (!eos) {
        EXPECT_OK(scanner.get_next(&chunk, &eos));
        if (!eos) {
            EXPECT_EQ(1, chunk->num_rows());
            EXPECT_EQ(101 + count, chunk->get_column_by_slot_id(MATERIALIZED_VIEW_ID)->get(0).get_int64());
            count++;
        }
        chunk->reset();
    }

    EXPECT_EQ(3, count);
}

} // namespace starrocks
