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

#include "exec/schema_scanner/schema_table_bookmark_summary_scanner.h"

#include <gtest/gtest.h>

#include <vector>

#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/object_pool.h"
#include "exec/schema_scanner.h"
#include "types/datum.h"

namespace starrocks {

// Friend of SchemaTableBookmarkSummaryScanner so the test can inject the rows that
// FE RPC would normally supply, then exercise the public get_next() path.
class SchemaTableBookmarkSummaryScannerTest : public ::testing::Test {
protected:
    void set_rows(SchemaTableBookmarkSummaryScanner& scanner, std::vector<TTableBookmarkSummaryInfo> rows) {
        scanner._rows = std::move(rows);
        scanner._row_idx = 0;
    }
};

// OLDEST_REFERENCE (slot 9) and NEWEST_REFERENCE (slot 10) are STRUCT<id, time, ttl_ms>.
// The ttl_ms field carries the raw reference ttl in milliseconds; a thrift ttl left unset
// by an older FE falls back to -1.
TEST_F(SchemaTableBookmarkSummaryScannerTest, fillReferenceSummaryTtlMs) {
    SchemaTableBookmarkSummaryScanner scanner;
    SchemaScannerParam params;
    std::string ip = "127.0.0.1";
    params.ip = &ip;
    params.port = 9020;
    ObjectPool pool;
    ASSERT_OK(scanner.init(&params, &pool));

    TBookmarkReferenceSummary oldest;
    oldest.__set_id("ref-oldest");
    oldest.__set_time(1700000000000);
    oldest.__set_ttl(3000);

    TBookmarkReferenceSummary newest;
    newest.__set_id("ref-newest");
    newest.__set_time(1700000001000);
    // ttl left unset -> default -1

    TTableBookmarkSummaryInfo info;
    info.__set_oldest_reference(oldest);
    info.__set_newest_reference(newest);
    set_rows(scanner, {info});

    // Request only the two reference-summary struct columns (slots 9 and 10).
    ChunkPtr chunk = std::make_shared<Chunk>();
    for (auto* slot : scanner.get_slot_descs()) {
        if (slot->id() == 9 || slot->id() == 10) {
            chunk->append_column(ColumnHelper::create_column(slot->type(), slot->is_nullable()), slot->id());
        }
    }
    ASSERT_EQ(2, chunk->num_columns());

    bool eos = false;
    ASSERT_OK(scanner.get_next(&chunk, &eos));
    ASSERT_FALSE(eos);

    // ttl_ms is the third struct field; oldest carries the set value, newest falls back to -1.
    DatumStruct oldest_struct = chunk->get_column_by_slot_id(9)->get(0).get_struct();
    ASSERT_EQ(3, oldest_struct.size());
    EXPECT_EQ(3000, oldest_struct[2].get_int64());

    DatumStruct newest_struct = chunk->get_column_by_slot_id(10)->get(0).get_struct();
    ASSERT_EQ(3, newest_struct.size());
    EXPECT_EQ(-1, newest_struct[2].get_int64());

    ASSERT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_TRUE(eos);
}

} // namespace starrocks
