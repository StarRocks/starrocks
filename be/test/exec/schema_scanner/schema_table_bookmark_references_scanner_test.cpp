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

#include "exec/schema_scanner/schema_table_bookmark_references_scanner.h"

#include <gtest/gtest.h>

#include <vector>

#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/object_pool.h"
#include "exec/schema_scanner.h"

namespace starrocks {

// Friend of SchemaTableBookmarkReferencesScanner so the test can inject the rows that
// FE RPC would normally supply, then exercise the public get_next() path.
class SchemaTableBookmarkReferencesScannerTest : public ::testing::Test {
protected:
    void set_rows(SchemaTableBookmarkReferencesScanner& scanner, std::vector<TTableBookmarkReferenceInfo> rows) {
        scanner._rows = std::move(rows);
        scanner._row_idx = 0;
    }
};

// TTL_MS (slot 6) carries the raw per-reference ttl in milliseconds. A thrift ttl left
// unset by an older FE falls back to -1 for version-skew safety.
TEST_F(SchemaTableBookmarkReferencesScannerTest, fillTtlMsColumn) {
    SchemaTableBookmarkReferencesScanner scanner;
    SchemaScannerParam params;
    std::string ip = "127.0.0.1";
    params.ip = &ip;
    params.port = 9020;
    ObjectPool pool;
    ASSERT_OK(scanner.init(&params, &pool));

    TTableBookmarkReferenceInfo with_ttl;
    with_ttl.__set_ttl(5000);
    TTableBookmarkReferenceInfo without_ttl; // ttl left unset -> default -1
    set_rows(scanner, {with_ttl, without_ttl});

    // Request only TTL_MS (slot 6) so get_next exercises just that case.
    ChunkPtr chunk = std::make_shared<Chunk>();
    for (auto* slot : scanner.get_slot_descs()) {
        if (slot->id() == 6) {
            chunk->append_column(ColumnHelper::create_column(slot->type(), slot->is_nullable()), slot->id());
        }
    }
    ASSERT_EQ(1, chunk->num_columns());

    bool eos = false;
    ASSERT_OK(scanner.get_next(&chunk, &eos));
    ASSERT_FALSE(eos);
    ASSERT_OK(scanner.get_next(&chunk, &eos));
    ASSERT_FALSE(eos);

    auto column = chunk->get_column_by_slot_id(6);
    ASSERT_EQ(2, column->size());
    EXPECT_EQ(5000, column->get(0).get_int64());
    EXPECT_EQ(-1, column->get(1).get_int64());

    // Third call drains the rows and reports end-of-stream.
    ASSERT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_TRUE(eos);
}

} // namespace starrocks
