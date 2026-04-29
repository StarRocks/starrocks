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

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "column/column_helper.h"
#include "exec/schema_scanner/schema_charsets_scanner.h"
#include "exec/schema_scanner/schema_collations_scanner.h"
#include "exec/schema_scanner/schema_column_filler.h"
#include "exec/schema_scanner/schema_dummy_scanner.h"
#include "exec/schema_scanner/schema_events_scanner.h"
#include "exec/schema_scanner/schema_statistics_scanner.h"
#include "exec/schema_scanner/schema_triggers_scanner.h"
#include "runtime/descriptors.h"
#include "types/logical_type.h"

namespace starrocks {

namespace {

ChunkPtr create_chunk(const std::vector<SlotDescriptor*>& slot_descs) {
    ChunkPtr chunk = std::make_shared<Chunk>();
    for (const auto* slot_desc : slot_descs) {
        auto column = ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());
        chunk->append_column(std::move(column), slot_desc->id());
    }
    return chunk;
}

template <typename Scanner>
std::vector<SlotDescriptor*> init_scanner(Scanner* scanner, ObjectPool* pool) {
    SchemaScannerParam param;
    auto st = scanner->init(&param, pool);
    EXPECT_TRUE(st.ok()) << st.to_string();
    return scanner->get_slot_descs();
}

} // namespace

TEST(ExecSchemaScannersTest, DummyScannerAlwaysReturnsEos) {
    SchemaDummyScanner scanner;
    SchemaScannerParam param;
    ObjectPool pool;
    ASSERT_TRUE(scanner.init(&param, &pool).ok());

    bool eos = false;
    auto st = scanner.get_next(nullptr, &eos);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_TRUE(eos);
}

TEST(ExecSchemaScannersTest, CharsetsScannerReturnsStaticCharsetRow) {
    SchemaCharsetsScanner scanner;
    ObjectPool pool;
    auto slots = init_scanner(&scanner, &pool);
    ASSERT_EQ(4, slots.size());
    EXPECT_EQ("CHARACTER_SET_NAME", slots[0]->col_name());
    EXPECT_EQ("DEFAULT_COLLATE_NAME", slots[1]->col_name());
    EXPECT_EQ("DESCRIPTION", slots[2]->col_name());
    EXPECT_EQ("MAXLEN", slots[3]->col_name());

    auto chunk = create_chunk(slots);
    bool eos = false;
    auto st = scanner.get_next(&chunk, &eos);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_FALSE(eos);
    ASSERT_EQ(1, chunk->num_rows());
    EXPECT_EQ("['utf8', 'utf8_general_ci', 'UTF-8 Unicode', 3]", chunk->debug_row(0));

    chunk->reset();
    st = scanner.get_next(&chunk, &eos);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_TRUE(eos);
}

TEST(ExecSchemaScannersTest, CollationsScannerReturnsStaticCollationRow) {
    SchemaCollationsScanner scanner;
    ObjectPool pool;
    auto slots = init_scanner(&scanner, &pool);
    ASSERT_EQ(6, slots.size());
    EXPECT_EQ("COLLATION_NAME", slots[0]->col_name());
    EXPECT_EQ("CHARACTER_SET_NAME", slots[1]->col_name());
    EXPECT_EQ("ID", slots[2]->col_name());
    EXPECT_EQ("IS_DEFAULT", slots[3]->col_name());
    EXPECT_EQ("IS_COMPILED", slots[4]->col_name());
    EXPECT_EQ("SORTLEN", slots[5]->col_name());

    auto chunk = create_chunk(slots);
    bool eos = false;
    auto st = scanner.get_next(&chunk, &eos);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_FALSE(eos);
    ASSERT_EQ(1, chunk->num_rows());
    EXPECT_EQ("['utf8_general_ci', 'utf8', 33, 'Yes', 'Yes', 1]", chunk->debug_row(0));

    chunk->reset();
    st = scanner.get_next(&chunk, &eos);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_TRUE(eos);
}

TEST(ExecSchemaScannersTest, DescriptorOnlyScannersExposeStableSchemas) {
    ObjectPool pool;

    SchemaEventsScanner events;
    auto event_slots = init_scanner(&events, &pool);
    ASSERT_EQ(24, event_slots.size());
    EXPECT_EQ("EVENT_CATALOG", event_slots.front()->col_name());
    EXPECT_EQ("DATABASE_COLLATION", event_slots.back()->col_name());

    SchemaStatisticsScanner statistics;
    auto statistics_slots = init_scanner(&statistics, &pool);
    ASSERT_EQ(17, statistics_slots.size());
    EXPECT_EQ("TABLE_CATALOG", statistics_slots.front()->col_name());
    EXPECT_TRUE(statistics_slots.front()->is_nullable());
    EXPECT_EQ("EXPRESSION", statistics_slots.back()->col_name());

    SchemaTriggersScanner triggers;
    auto trigger_slots = init_scanner(&triggers, &pool);
    ASSERT_EQ(22, trigger_slots.size());
    EXPECT_EQ("TRIGGER_CATALOG", trigger_slots.front()->col_name());
    EXPECT_EQ("DATABASE_COLLATION", trigger_slots.back()->col_name());
}

TEST(ExecSchemaScannersTest, ColumnFillerHandlesNullableAndNonNullableColumns) {
    auto nullable_bigint = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_BIGINT), true);
    int64_t bigint_value = 42;
    fill_column_with_slot<TYPE_BIGINT>(nullable_bigint.get(), &bigint_value);
    ASSERT_EQ(1, nullable_bigint->size());
    EXPECT_EQ("42", nullable_bigint->debug_item(0));

    auto varchar = ColumnHelper::create_column(TypeDescriptor::create_varchar_type(sizeof(Slice)), false);
    std::string text = "schema";
    Slice text_slice(text);
    fill_column_with_slot<TYPE_VARCHAR>(varchar.get(), &text_slice);
    ASSERT_EQ(1, varchar->size());
    EXPECT_EQ("'schema'", varchar->debug_item(0));
}

} // namespace starrocks
