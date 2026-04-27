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

#include "base/string/slice.h"
#include "column/chunk.h"
#include "exec/schema_scanner.h"
#include "gen_cpp/Types_types.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {

class TestSchemaScanner : public SchemaScanner {
public:
    TestSchemaScanner(ColumnDesc* columns, int column_num) : SchemaScanner(columns, column_num) {}

    const SchemaScannerState& scanner_state() const { return _ss_state; }
};

TEST(SchemaScannerCoreTest, InitRejectsInvalidParameters) {
    SchemaScanner scanner(nullptr, 0);
    SchemaScannerParam param;
    ObjectPool pool;

    ASSERT_FALSE(scanner.init(nullptr, &pool).ok());
    ASSERT_FALSE(scanner.init(&param, nullptr).ok());
}

TEST(SchemaScannerCoreTest, InitCreatesSlotDescriptors) {
    SchemaScanner::ColumnDesc columns[] = {
            {"ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
            {"NAME", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
    };
    TestSchemaScanner scanner(columns, 2);
    SchemaScannerParam param;
    ObjectPool pool;

    auto st = scanner.init(&param, &pool);
    ASSERT_TRUE(st.ok()) << st.to_string();

    const auto& slots = scanner.get_slot_descs();
    ASSERT_EQ(2, slots.size());
    EXPECT_EQ(1, slots[0]->id());
    EXPECT_EQ("ID", slots[0]->col_name());
    EXPECT_EQ(TYPE_BIGINT, slots[0]->type().type);
    EXPECT_FALSE(slots[0]->is_nullable());
    EXPECT_EQ(2, slots[1]->id());
    EXPECT_EQ("NAME", slots[1]->col_name());
    EXPECT_EQ(TYPE_VARCHAR, slots[1]->type().type);
    EXPECT_TRUE(slots[1]->is_nullable());
}

TEST(SchemaScannerCoreTest, StartAndDefaultGetNextRequireInit) {
    SchemaScanner scanner(nullptr, 0);
    RuntimeState state;

    ASSERT_FALSE(scanner.start(&state).ok());

    SchemaScannerParam param;
    ObjectPool pool;
    auto st = scanner.init(&param, &pool);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_TRUE(scanner.start(&state).ok());

    bool eos = false;
    ChunkPtr chunk = std::make_shared<Chunk>();
    st = scanner.get_next(&chunk, &eos);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_TRUE(eos);

    eos = false;
    EXPECT_FALSE(scanner.get_next(nullptr, &eos).ok());
    EXPECT_FALSE(scanner.get_next(&chunk, nullptr).ok());
}

TEST(SchemaScannerCoreTest, InitSchemaScannerStateCopiesFrontendEndpoint) {
    TestSchemaScanner scanner(nullptr, 0);
    SchemaScannerParam param;
    std::string ip = "127.0.0.1";
    param.ip = &ip;
    param.port = 9020;
    ObjectPool pool;
    ASSERT_TRUE(scanner.init(&param, &pool).ok());

    TQueryOptions query_options;
    query_options.__set_query_timeout(13);
    RuntimeState state(TUniqueId(), query_options, TQueryGlobals(), nullptr);

    auto st = scanner.init_schema_scanner_state(&state);
    ASSERT_TRUE(st.ok()) << st.to_string();

    EXPECT_EQ("127.0.0.1", scanner.scanner_state().ip);
    EXPECT_EQ(9020, scanner.scanner_state().port);
    EXPECT_EQ(13 * 1000, scanner.scanner_state().timeout_ms);
    EXPECT_EQ(&param, scanner.scanner_state().param);
}

TEST(SchemaScannerCoreTest, InitSchemaScannerStateRejectsMissingEndpoint) {
    SchemaScanner scanner(nullptr, 0);
    SchemaScannerParam param;
    ObjectPool pool;
    ASSERT_TRUE(scanner.init(&param, &pool).ok());

    RuntimeState state;
    ASSERT_FALSE(scanner.init_schema_scanner_state(&state).ok());
}

} // namespace starrocks
