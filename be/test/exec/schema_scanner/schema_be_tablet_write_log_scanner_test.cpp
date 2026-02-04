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

    // Add logs
    mgr->add_load_log(1001, 100, 30, 10, 20, 100, 1000, 100, 2000, 5, "label1", 1686000000000, 1686000010000);
    mgr->add_compaction_log(1001, 200, 31, 11, 21, 200, 2000, 150, 1500, 10, 5, 80, "base", 1686000020000,
                            1686000030000);

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
