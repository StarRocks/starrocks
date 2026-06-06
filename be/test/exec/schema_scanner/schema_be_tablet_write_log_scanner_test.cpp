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

#include "exec/schema_scanner/schema_be_tablet_write_log_scanner.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "column/column_helper.h"
#include "runtime/runtime_state.h"
#include "storage/lake/tablet_write_log_manager.h"

namespace starrocks {

class SchemaBeTabletWriteLogScannerTest : public ::testing::Test {
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

TEST_F(SchemaBeTabletWriteLogScannerTest, test_normal) {
    SchemaBeTabletWriteLogScanner scanner;
    SchemaScannerParam params;
    std::string ip = "127.0.0.1";
    params.ip = &ip;
    params.port = 9020;
    ObjectPool pool;
    RuntimeState state(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
    state.init_instance_mem_tracker();

    // prepare data
    auto mgr = lake::TabletWriteLogManager::instance();
    // Clear logs
    mgr->cleanup_old_logs(std::numeric_limits<int64_t>::max());

    // Add logs (with SST stats)
    mgr->add_load_log(1001, 100, 30, 10, 20, 100, 1000, 100, 2000, 5, "label1", 1686000000000, 1686000010000, 2, 8192);
    mgr->add_compaction_log(1001, 200, 31, 11, 21, 200, 2000, 150, 1500, 10, 5, 80, "base", 1686000020000,
                            1686000030000, 3, 12288, 1, 4096);

    // init and start scanner
    EXPECT_OK(scanner.init(&params, &pool));
    EXPECT_OK(scanner.start(&state));

    // check get_next
    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;

    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_FALSE(eos);
    EXPECT_EQ(2, chunk->num_rows());

    // Verify first row (LOAD)
    auto row0 = chunk->debug_row(0);
    EXPECT_TRUE(row0.find("1001") != std::string::npos);   // be_id
    EXPECT_TRUE(row0.find("LOAD") != std::string::npos);   // log_type
    EXPECT_TRUE(row0.find("label1") != std::string::npos); // label

    // Verify second row (COMPACTION)
    auto row1 = chunk->debug_row(1);
    EXPECT_TRUE(row1.find("1001") != std::string::npos);       // be_id
    EXPECT_TRUE(row1.find("COMPACTION") != std::string::npos); // log_type
    EXPECT_TRUE(row1.find("base") != std::string::npos);       // compaction_type

    chunk->reset();
    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_TRUE(eos);
}

TEST_F(SchemaBeTabletWriteLogScannerTest, test_sst_columns_with_values) {
    SchemaBeTabletWriteLogScanner scanner;
    SchemaScannerParam params;
    std::string ip = "127.0.0.1";
    params.ip = &ip;
    params.port = 9020;
    ObjectPool pool;
    RuntimeState state(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
    state.init_instance_mem_tracker();

    auto mgr = lake::TabletWriteLogManager::instance();
    mgr->cleanup_old_logs(std::numeric_limits<int64_t>::max());

    // Load with SST output stats
    mgr->add_load_log(1001, 100, 30, 10, 20, 100, 1000, 100, 2000, 5, "label1", 1686000000000, 1686000010000, 2, 8192);
    // Compaction with both SST input and output stats
    mgr->add_compaction_log(1001, 200, 31, 11, 21, 200, 2000, 150, 1500, 10, 5, 80, "base", 1686000020000,
                            1686000030000, 3, 12288, 1, 4096);

    EXPECT_OK(scanner.init(&params, &pool));
    EXPECT_OK(scanner.start(&state));

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;
    EXPECT_OK(scanner.get_next(&chunk, &eos));
    ASSERT_EQ(2, chunk->num_rows());

    // Column indices for SST stats: SST_INPUT_FILES=17, SST_INPUT_BYTES=18, SST_OUTPUT_FILES=19, SST_OUTPUT_BYTES=20
    // LOAD row (index 0): sst_input_files=0 (NULL), sst_output_files=2 (non-NULL)
    auto sst_input_files_col = chunk->get_column_by_index(17);
    auto sst_input_bytes_col = chunk->get_column_by_index(18);
    auto sst_output_files_col = chunk->get_column_by_index(19);
    auto sst_output_bytes_col = chunk->get_column_by_index(20);

    // LOAD row: no SST input (NULL), has SST output
    EXPECT_TRUE(sst_input_files_col->is_null(0));
    EXPECT_TRUE(sst_input_bytes_col->is_null(0));
    EXPECT_FALSE(sst_output_files_col->is_null(0));
    EXPECT_FALSE(sst_output_bytes_col->is_null(0));
    EXPECT_EQ(2, sst_output_files_col->get(0).get_int32());
    EXPECT_EQ(8192, sst_output_bytes_col->get(0).get_int64());

    // COMPACTION row: has both SST input and output
    EXPECT_FALSE(sst_input_files_col->is_null(1));
    EXPECT_FALSE(sst_input_bytes_col->is_null(1));
    EXPECT_FALSE(sst_output_files_col->is_null(1));
    EXPECT_FALSE(sst_output_bytes_col->is_null(1));
    EXPECT_EQ(3, sst_input_files_col->get(1).get_int32());
    EXPECT_EQ(12288, sst_input_bytes_col->get(1).get_int64());
    EXPECT_EQ(1, sst_output_files_col->get(1).get_int32());
    EXPECT_EQ(4096, sst_output_bytes_col->get(1).get_int64());
}

TEST_F(SchemaBeTabletWriteLogScannerTest, test_sst_columns_null_when_zero) {
    SchemaBeTabletWriteLogScanner scanner;
    SchemaScannerParam params;
    std::string ip = "127.0.0.1";
    params.ip = &ip;
    params.port = 9020;
    ObjectPool pool;
    RuntimeState state(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
    state.init_instance_mem_tracker();

    auto mgr = lake::TabletWriteLogManager::instance();
    mgr->cleanup_old_logs(std::numeric_limits<int64_t>::max());

    // Load without SST stats (non-PK table)
    mgr->add_load_log(1001, 100, 30, 10, 20, 100, 1000, 100, 2000, 5, "label_no_sst", 1686000000000, 1686000010000);

    EXPECT_OK(scanner.init(&params, &pool));
    EXPECT_OK(scanner.start(&state));

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;
    EXPECT_OK(scanner.get_next(&chunk, &eos));
    ASSERT_EQ(1, chunk->num_rows());

    // All SST columns should be NULL when values are 0
    EXPECT_TRUE(chunk->get_column_by_index(17)->is_null(0)); // SST_INPUT_FILES
    EXPECT_TRUE(chunk->get_column_by_index(18)->is_null(0)); // SST_INPUT_BYTES
    EXPECT_TRUE(chunk->get_column_by_index(19)->is_null(0)); // SST_OUTPUT_FILES
    EXPECT_TRUE(chunk->get_column_by_index(20)->is_null(0)); // SST_OUTPUT_BYTES
}

TEST_F(SchemaBeTabletWriteLogScannerTest, test_publish_log_type) {
    SchemaBeTabletWriteLogScanner scanner;
    SchemaScannerParam params;
    std::string ip = "127.0.0.1";
    params.ip = &ip;
    params.port = 9020;
    ObjectPool pool;
    RuntimeState state(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
    state.init_instance_mem_tracker();

    auto mgr = lake::TabletWriteLogManager::instance();
    mgr->cleanup_old_logs(std::numeric_limits<int64_t>::max());

    mgr->add_publish_log(1001, 300, 30, 10, 20, 1686000000000, 1686000010000, 3, 12288);

    EXPECT_OK(scanner.init(&params, &pool));
    EXPECT_OK(scanner.start(&state));

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;
    EXPECT_OK(scanner.get_next(&chunk, &eos));
    ASSERT_EQ(1, chunk->num_rows());

    auto row0 = chunk->debug_row(0);
    EXPECT_TRUE(row0.find("PUBLISH") != std::string::npos);

    // PUBLISH: input_rows/output_rows = 0, label = NULL, compaction fields = NULL
    // input_segments (col 12) = NULL (not compaction)
    EXPECT_TRUE(chunk->get_column_by_index(12)->is_null(0));
    // label (col 14) = NULL (not load)
    EXPECT_TRUE(chunk->get_column_by_index(14)->is_null(0));
    // compaction_score (col 15) = NULL
    EXPECT_TRUE(chunk->get_column_by_index(15)->is_null(0));
    // compaction_type (col 16) = NULL
    EXPECT_TRUE(chunk->get_column_by_index(16)->is_null(0));

    // SST output should have values
    EXPECT_FALSE(chunk->get_column_by_index(19)->is_null(0)); // SST_OUTPUT_FILES
    EXPECT_FALSE(chunk->get_column_by_index(20)->is_null(0)); // SST_OUTPUT_BYTES
    EXPECT_EQ(3, chunk->get_column_by_index(19)->get(0).get_int32());
    EXPECT_EQ(12288, chunk->get_column_by_index(20)->get(0).get_int64());
    // SST input = NULL (no input for publish)
    EXPECT_TRUE(chunk->get_column_by_index(17)->is_null(0)); // SST_INPUT_FILES
    EXPECT_TRUE(chunk->get_column_by_index(18)->is_null(0)); // SST_INPUT_BYTES
}

TEST_F(SchemaBeTabletWriteLogScannerTest, test_mixed_log_types) {
    SchemaBeTabletWriteLogScanner scanner;
    SchemaScannerParam params;
    std::string ip = "127.0.0.1";
    params.ip = &ip;
    params.port = 9020;
    ObjectPool pool;
    RuntimeState state(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
    state.init_instance_mem_tracker();

    auto mgr = lake::TabletWriteLogManager::instance();
    mgr->cleanup_old_logs(std::numeric_limits<int64_t>::max());

    // All three log types
    mgr->add_load_log(1001, 100, 30, 10, 20, 100, 1000, 100, 2000, 5, "label1", 1686000000000, 1686000010000, 2, 8192);
    mgr->add_compaction_log(1001, 200, 31, 11, 21, 200, 2000, 150, 1500, 10, 5, 80, "base", 1686000020000,
                            1686000030000, 3, 12288, 1, 4096);
    mgr->add_publish_log(1001, 300, 32, 12, 22, 1686000040000, 1686000050000, 4, 16384);

    EXPECT_OK(scanner.init(&params, &pool));
    EXPECT_OK(scanner.start(&state));

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;
    EXPECT_OK(scanner.get_next(&chunk, &eos));
    ASSERT_EQ(3, chunk->num_rows());

    // Verify log_type column (index 7) for each row
    auto row0 = chunk->debug_row(0);
    auto row1 = chunk->debug_row(1);
    auto row2 = chunk->debug_row(2);
    EXPECT_TRUE(row0.find("LOAD") != std::string::npos);
    EXPECT_TRUE(row1.find("COMPACTION") != std::string::npos);
    EXPECT_TRUE(row2.find("PUBLISH") != std::string::npos);

    // Verify nullable columns across all three types
    // input_segments: only non-NULL for COMPACTION (row 1)
    EXPECT_TRUE(chunk->get_column_by_index(12)->is_null(0));
    EXPECT_FALSE(chunk->get_column_by_index(12)->is_null(1));
    EXPECT_TRUE(chunk->get_column_by_index(12)->is_null(2));

    // label: only non-NULL for LOAD (row 0)
    EXPECT_FALSE(chunk->get_column_by_index(14)->is_null(0));
    EXPECT_TRUE(chunk->get_column_by_index(14)->is_null(1));
    EXPECT_TRUE(chunk->get_column_by_index(14)->is_null(2));

    // compaction_score: only non-NULL for COMPACTION (row 1)
    EXPECT_TRUE(chunk->get_column_by_index(15)->is_null(0));
    EXPECT_FALSE(chunk->get_column_by_index(15)->is_null(1));
    EXPECT_TRUE(chunk->get_column_by_index(15)->is_null(2));
}

TEST_F(SchemaBeTabletWriteLogScannerTest, test_compaction_without_sst_all_null) {
    SchemaBeTabletWriteLogScanner scanner;
    SchemaScannerParam params;
    std::string ip = "127.0.0.1";
    params.ip = &ip;
    params.port = 9020;
    ObjectPool pool;
    RuntimeState state(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
    state.init_instance_mem_tracker();

    auto mgr = lake::TabletWriteLogManager::instance();
    mgr->cleanup_old_logs(std::numeric_limits<int64_t>::max());

    // Compaction without SST stats (non-PK table compaction)
    mgr->add_compaction_log(1001, 200, 31, 11, 21, 200, 2000, 150, 1500, 10, 5, 80, "cumulative", 1686000020000,
                            1686000030000);

    EXPECT_OK(scanner.init(&params, &pool));
    EXPECT_OK(scanner.start(&state));

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;
    EXPECT_OK(scanner.get_next(&chunk, &eos));
    ASSERT_EQ(1, chunk->num_rows());

    // Compaction-specific columns should be non-NULL
    EXPECT_FALSE(chunk->get_column_by_index(12)->is_null(0)); // INPUT_SEGMENTS
    EXPECT_FALSE(chunk->get_column_by_index(15)->is_null(0)); // COMPACTION_SCORE
    EXPECT_FALSE(chunk->get_column_by_index(16)->is_null(0)); // COMPACTION_TYPE
    // LABEL should be NULL for compaction
    EXPECT_TRUE(chunk->get_column_by_index(14)->is_null(0));

    // All SST columns should be NULL when values are 0
    EXPECT_TRUE(chunk->get_column_by_index(17)->is_null(0)); // SST_INPUT_FILES
    EXPECT_TRUE(chunk->get_column_by_index(18)->is_null(0)); // SST_INPUT_BYTES
    EXPECT_TRUE(chunk->get_column_by_index(19)->is_null(0)); // SST_OUTPUT_FILES
    EXPECT_TRUE(chunk->get_column_by_index(20)->is_null(0)); // SST_OUTPUT_BYTES
}

TEST_F(SchemaBeTabletWriteLogScannerTest, test_sst_column_values_exact) {
    SchemaBeTabletWriteLogScanner scanner;
    SchemaScannerParam params;
    std::string ip = "127.0.0.1";
    params.ip = &ip;
    params.port = 9020;
    ObjectPool pool;
    RuntimeState state(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
    state.init_instance_mem_tracker();

    auto mgr = lake::TabletWriteLogManager::instance();
    mgr->cleanup_old_logs(std::numeric_limits<int64_t>::max());

    // Compaction with specific SST values to verify exact column data
    mgr->add_compaction_log(1001, 200, 31, 11, 21, 200, 2000, 150, 1500, 10, 5, 80, "base", 1686000020000,
                            1686000030000, 7, 71680, 3, 30720);

    EXPECT_OK(scanner.init(&params, &pool));
    EXPECT_OK(scanner.start(&state));

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;
    EXPECT_OK(scanner.get_next(&chunk, &eos));
    ASSERT_EQ(1, chunk->num_rows());

    // Verify exact SST values
    EXPECT_EQ(7, chunk->get_column_by_index(17)->get(0).get_int32());     // SST_INPUT_FILES
    EXPECT_EQ(71680, chunk->get_column_by_index(18)->get(0).get_int64()); // SST_INPUT_BYTES
    EXPECT_EQ(3, chunk->get_column_by_index(19)->get(0).get_int32());     // SST_OUTPUT_FILES
    EXPECT_EQ(30720, chunk->get_column_by_index(20)->get(0).get_int64()); // SST_OUTPUT_BYTES

    // Also verify table_id and partition_id columns
    EXPECT_EQ(11, chunk->get_column_by_index(5)->get(0).get_int64()); // TABLE_ID
    EXPECT_EQ(21, chunk->get_column_by_index(6)->get(0).get_int64()); // PARTITION_ID
}

TEST_F(SchemaBeTabletWriteLogScannerTest, test_load_with_sst_output_only) {
    SchemaBeTabletWriteLogScanner scanner;
    SchemaScannerParam params;
    std::string ip = "127.0.0.1";
    params.ip = &ip;
    params.port = 9020;
    ObjectPool pool;
    RuntimeState state(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
    state.init_instance_mem_tracker();

    auto mgr = lake::TabletWriteLogManager::instance();
    mgr->cleanup_old_logs(std::numeric_limits<int64_t>::max());

    // Load with SST output (eager PK index build) but no input
    mgr->add_load_log(1001, 100, 30, 10, 20, 100, 1000, 100, 2000, 5, "eager_pk_load", 1686000000000, 1686000010000, 4,
                      32768);

    EXPECT_OK(scanner.init(&params, &pool));
    EXPECT_OK(scanner.start(&state));

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;
    EXPECT_OK(scanner.get_next(&chunk, &eos));
    ASSERT_EQ(1, chunk->num_rows());

    // SST input = NULL (load never has SST input)
    EXPECT_TRUE(chunk->get_column_by_index(17)->is_null(0));
    EXPECT_TRUE(chunk->get_column_by_index(18)->is_null(0));
    // SST output should have values
    EXPECT_FALSE(chunk->get_column_by_index(19)->is_null(0));
    EXPECT_FALSE(chunk->get_column_by_index(20)->is_null(0));
    EXPECT_EQ(4, chunk->get_column_by_index(19)->get(0).get_int32());
    EXPECT_EQ(32768, chunk->get_column_by_index(20)->get(0).get_int64());

    // LABEL should be non-NULL for LOAD
    EXPECT_FALSE(chunk->get_column_by_index(14)->is_null(0));
    // INPUT_SEGMENTS should be NULL for LOAD
    EXPECT_TRUE(chunk->get_column_by_index(12)->is_null(0));
}

TEST_F(SchemaBeTabletWriteLogScannerTest, test_empty_logs) {
    SchemaBeTabletWriteLogScanner scanner;
    SchemaScannerParam params;
    std::string ip = "127.0.0.1";
    params.ip = &ip;
    params.port = 9020;
    ObjectPool pool;
    RuntimeState state(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
    state.init_instance_mem_tracker();

    auto mgr = lake::TabletWriteLogManager::instance();
    mgr->cleanup_old_logs(std::numeric_limits<int64_t>::max());

    EXPECT_OK(scanner.init(&params, &pool));
    EXPECT_OK(scanner.start(&state));

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;
    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_TRUE(eos);
    EXPECT_EQ(0, chunk->num_rows());
}

TEST_F(SchemaBeTabletWriteLogScannerTest, test_get_next_overflow) {
    SchemaBeTabletWriteLogScanner scanner;
    SchemaScannerParam params;
    std::string ip = "127.0.0.1";
    params.ip = &ip;
    params.port = 9020;
    ObjectPool pool;
    RuntimeState state(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
    state.init_instance_mem_tracker();

    // Set chunk size smaller than log count to test multiple batches
    // Note: Can't easily change chunk size on RuntimeState directly as it defaults to 4096
    // So we add enough logs or just rely on standard behavior

    auto mgr = lake::TabletWriteLogManager::instance();
    mgr->cleanup_old_logs(std::numeric_limits<int64_t>::max());

    for (int i = 0; i < 5000; ++i) {
        mgr->add_load_log(1, i, 1, 1, 1, 1, 1, 1, 1, 1, "l", 0, 0);
    }

    EXPECT_OK(scanner.init(&params, &pool));
    EXPECT_OK(scanner.start(&state));

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;
    int total_rows = 0;

    while (!eos) {
        chunk->reset();
        EXPECT_OK(scanner.get_next(&chunk, &eos));
        total_rows += chunk->num_rows();
    }

    EXPECT_EQ(5000, total_rows);
}

} // namespace starrocks
