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

#include "schema_scanner/schema_table_bookmark_references_scanner.h"

#include <gtest/gtest.h>

#include <vector>

#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "common/object_pool.h"
#include "exec/schema_scanner.h"
#include "types/timestamp_value.h"

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

// LAST_RENEW_TIME (slot 7) is the one column on this table that is genuinely nullable, and the
// only one whose value is derived rather than passed through. FE sends epoch-millis; DATETIME
// stores seconds. Dropping the /1000 renders the year as roughly 55,000 and nothing else in the
// tree notices -- from_unixtime range-checks nothing, and the SQL test only asserts IS NOT NULL.
TEST_F(SchemaTableBookmarkReferencesScannerTest, fillLastRenewTimeColumn) {
    SchemaTableBookmarkReferencesScanner scanner;
    SchemaScannerParam params;
    std::string ip = "127.0.0.1";
    params.ip = &ip;
    params.port = 9020;
    ObjectPool pool;
    ASSERT_OK(scanner.init(&params, &pool));

    TTableBookmarkReferenceInfo renewed;
    renewed.__set_last_renew_time(1700000000123);
    TTableBookmarkReferenceInfo never_renewed; // field unset, as an older FE sends it
    TTableBookmarkReferenceInfo zero_renewed;  // set but non-positive: still "never"
    zero_renewed.__set_last_renew_time(0);
    set_rows(scanner, {renewed, never_renewed, zero_renewed});

    // Request only LAST_RENEW_TIME (slot 7); start() is skipped, so _ctz is the default
    // cctz::time_zone, which is UTC.
    ChunkPtr chunk = std::make_shared<Chunk>();
    for (auto* slot : scanner.get_slot_descs()) {
        if (slot->id() == 7) {
            chunk->append_column(ColumnHelper::create_column(slot->type(), slot->is_nullable()), slot->id());
        }
    }
    ASSERT_EQ(1, chunk->num_columns());

    bool eos = false;
    for (int i = 0; i < 3; i++) {
        ASSERT_OK(scanner.get_next(&chunk, &eos));
        ASSERT_FALSE(eos);
    }

    auto column = chunk->get_column_by_slot_id(7);
    ASSERT_EQ(3, column->size());
    EXPECT_FALSE(column->is_null(0));
    EXPECT_EQ("2023-11-14 22:13:20", column->get(0).get_timestamp().to_string(/*ignore_microsecond=*/true));
    EXPECT_TRUE(column->is_null(1));
    EXPECT_TRUE(column->is_null(2));

    ASSERT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_TRUE(eos);
}

// The sub-second remainder is dropped, never rounded up: an operator comparing
// LAST_RENEW_TIME against a lease is then conservative by up to 999ms, never optimistic.
TEST_F(SchemaTableBookmarkReferencesScannerTest, lastRenewTimeTruncatesSubSecondMillis) {
    SchemaTableBookmarkReferencesScanner scanner;
    SchemaScannerParam params;
    std::string ip = "127.0.0.1";
    params.ip = &ip;
    params.port = 9020;
    ObjectPool pool;
    ASSERT_OK(scanner.init(&params, &pool));

    TTableBookmarkReferenceInfo info;
    info.__set_last_renew_time(1700000000999);
    set_rows(scanner, {info});

    ChunkPtr chunk = std::make_shared<Chunk>();
    for (auto* slot : scanner.get_slot_descs()) {
        if (slot->id() == 7) {
            chunk->append_column(ColumnHelper::create_column(slot->type(), slot->is_nullable()), slot->id());
        }
    }

    bool eos = false;
    ASSERT_OK(scanner.get_next(&chunk, &eos));
    ASSERT_FALSE(eos);

    auto column = chunk->get_column_by_slot_id(7);
    EXPECT_EQ("2023-11-14 22:13:20", column->get(0).get_timestamp().to_string(/*ignore_microsecond=*/true));
}

// CREATE_TIME (slot 5) shares LAST_RENEW_TIME's millis-to-seconds conversion and had no
// coverage either. Only the positive path is exercised: the column is declared NOT NULL, so
// a non-positive create_time takes a null-fill branch its own declaration does not allow.
TEST_F(SchemaTableBookmarkReferencesScannerTest, fillCreateTimeColumn) {
    SchemaTableBookmarkReferencesScanner scanner;
    SchemaScannerParam params;
    std::string ip = "127.0.0.1";
    params.ip = &ip;
    params.port = 9020;
    ObjectPool pool;
    ASSERT_OK(scanner.init(&params, &pool));

    TTableBookmarkReferenceInfo info;
    info.__set_create_time(1747202363000);
    set_rows(scanner, {info});

    ChunkPtr chunk = std::make_shared<Chunk>();
    for (auto* slot : scanner.get_slot_descs()) {
        if (slot->id() == 5) {
            chunk->append_column(ColumnHelper::create_column(slot->type(), slot->is_nullable()), slot->id());
        }
    }

    bool eos = false;
    ASSERT_OK(scanner.get_next(&chunk, &eos));
    ASSERT_FALSE(eos);

    auto column = chunk->get_column_by_slot_id(5);
    EXPECT_EQ("2025-05-14 05:59:23", column->get(0).get_timestamp().to_string(/*ignore_microsecond=*/true));
}

TEST_F(SchemaTableBookmarkReferencesScannerTest, fillExpireTimeColumn) {
    SchemaTableBookmarkReferencesScanner scanner;
    SchemaScannerParam params;
    std::string ip = "127.0.0.1";
    params.ip = &ip;
    params.port = 9020;
    ObjectPool pool;
    ASSERT_OK(scanner.init(&params, &pool));

    TTableBookmarkReferenceInfo with_expire;
    with_expire.__set_expire_time(1700000000123);
    TTableBookmarkReferenceInfo without_expire;
    TTableBookmarkReferenceInfo zero_expire;
    zero_expire.__set_expire_time(0);
    set_rows(scanner, {with_expire, without_expire, zero_expire});

    ChunkPtr chunk = std::make_shared<Chunk>();
    for (auto* slot : scanner.get_slot_descs()) {
        if (slot->id() == 8) {
            chunk->append_column(ColumnHelper::create_column(slot->type(), slot->is_nullable()), slot->id());
        }
    }
    ASSERT_EQ(1, chunk->num_columns());

    bool eos = false;
    for (int i = 0; i < 3; i++) {
        ASSERT_OK(scanner.get_next(&chunk, &eos));
        ASSERT_FALSE(eos);
    }

    auto column = chunk->get_column_by_slot_id(8);
    ASSERT_EQ(3, column->size());
    EXPECT_FALSE(column->is_null(0));
    EXPECT_EQ("2023-11-14 22:13:20", column->get(0).get_timestamp().to_string(/*ignore_microsecond=*/true));
    EXPECT_TRUE(column->is_null(1));
    EXPECT_TRUE(column->is_null(2));

    ASSERT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_TRUE(eos);
}

TEST_F(SchemaTableBookmarkReferencesScannerTest, fillRenewCountColumn) {
    SchemaTableBookmarkReferencesScanner scanner;
    SchemaScannerParam params;
    std::string ip = "127.0.0.1";
    params.ip = &ip;
    params.port = 9020;
    ObjectPool pool;
    ASSERT_OK(scanner.init(&params, &pool));

    TTableBookmarkReferenceInfo with_count;
    with_count.__set_renew_count(7);
    TTableBookmarkReferenceInfo without_count; // unset -> 0
    set_rows(scanner, {with_count, without_count});

    ChunkPtr chunk = std::make_shared<Chunk>();
    for (auto* slot : scanner.get_slot_descs()) {
        if (slot->id() == 9) {
            chunk->append_column(ColumnHelper::create_column(slot->type(), slot->is_nullable()), slot->id());
        }
    }
    ASSERT_EQ(1, chunk->num_columns());

    bool eos = false;
    ASSERT_OK(scanner.get_next(&chunk, &eos));
    ASSERT_FALSE(eos);
    ASSERT_OK(scanner.get_next(&chunk, &eos));
    ASSERT_FALSE(eos);

    auto column = chunk->get_column_by_slot_id(9);
    ASSERT_EQ(2, column->size());
    EXPECT_EQ(7, column->get(0).get_int64());
    EXPECT_EQ(0, column->get(1).get_int64());

    ASSERT_OK(scanner.get_next(&chunk, &eos));
    EXPECT_TRUE(eos);
}

} // namespace starrocks
