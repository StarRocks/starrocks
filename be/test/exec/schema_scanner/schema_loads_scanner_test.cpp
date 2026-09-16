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

#include "exec/schema_scanner/schema_loads_scanner.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "runtime/descriptors.h"
#include "testutil/assert.h"

namespace starrocks {

// getLoads answers in pages, and the cursor for the page after the current one lives in
// _next_job_id_offset (0 = end). start() goes through a thrift RPC to FE; these tests
// bypass it by populating _result directly (BE unit tests compile with
// -fno-access-control) and pin the two halves that need no FE.
//
// 1. The terminating branch of the page-pulling loop in get_next(), which is also the
//    whole story for an FE too old to paginate: it leaves next_job_id_offset unset,
//    _fetch_page decodes that to 0, and the single response it sent must be served in
//    full and then end the scan.
// 2. The row-at-a-time contract. SchemaChunkSource counts one row per get_next() call,
//    so a scanner that drained its whole buffer in one call would make the caller
//    accumulate the entire result set into one chunk.
class SchemaLoadsScannerTest : public ::testing::Test {
protected:
    void SetUp() override {
        _params.ip = &_ip;
        _params.port = 9020;
    }

    void init_scanner(SchemaLoadsScanner& scanner) { EXPECT_OK(scanner.init(&_params, &_pool)); }

    ChunkPtr create_chunk(const std::vector<SlotDescriptor*>& slot_descs) {
        ChunkPtr chunk = std::make_shared<Chunk>();
        for (const auto* slot_desc : slot_descs) {
            MutableColumnPtr column = ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());
            chunk->append_column(std::move(column), slot_desc->id());
        }
        return chunk;
    }

    // Minimal TLoadInfo: the non-nullable string columns populated, and the two JSON
    // columns given a valid document so fill_chunk parses them instead of logging a
    // warning and falling back to NULL.
    static TLoadInfo make_min_load_info(int64_t job_id) {
        TLoadInfo info;
        info.__set_job_id(job_id);
        info.__set_label("test_label_" + std::to_string(job_id));
        info.__set_db("test_db");
        info.__set_table("test_tbl");
        info.__set_user("test_user");
        info.__set_state("FINISHED");
        info.__set_progress("100%");
        info.__set_type("BROKER");
        info.__set_priority("NORMAL");
        info.__set_runtime_details("{}");
        info.__set_properties("{}");
        return info;
    }

    SchemaScannerParam _params;
    std::string _ip = "127.0.0.1";
    ObjectPool _pool;
};

TEST_F(SchemaLoadsScannerTest, single_page_response_yields_one_row_per_call_then_eos) {
    SchemaLoadsScanner scanner;
    init_scanner(scanner);

    scanner._result.loads = {make_min_load_info(1), make_min_load_info(2), make_min_load_info(3)};
    scanner._cur_idx = 0;
    scanner._next_job_id_offset = 0;

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = true;
    for (int expected_rows = 1; expected_rows <= 3; ++expected_rows) {
        EXPECT_OK(scanner.get_next(&chunk, &eos));
        EXPECT_FALSE(eos) << "row " << expected_rows;
        EXPECT_EQ(static_cast<size_t>(expected_rows), chunk->num_rows())
                << "each get_next must append exactly one row, not drain the buffer";
    }

    // Page drained with no cursor to follow: end the scan instead of attempting
    // another fetch.
    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_TRUE(eos);
    EXPECT_EQ(3UL, chunk->num_rows()) << "eos must not append anything";
}

// An empty result with no cursor ends immediately and yields no chunk rows. Guards the
// page-pulling loop in get_next against spinning on, or handing the pipeline, an empty
// page.
TEST_F(SchemaLoadsScannerTest, empty_page_without_cursor_reaches_eos_immediately) {
    SchemaLoadsScanner scanner;
    init_scanner(scanner);

    scanner._result.loads.clear();
    scanner._cur_idx = 0;
    scanner._next_job_id_offset = 0;

    auto chunk = create_chunk(scanner.get_slot_descs());
    bool eos = false;
    EXPECT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_TRUE(eos);
    EXPECT_EQ(0UL, chunk->num_rows());
}

} // namespace starrocks
