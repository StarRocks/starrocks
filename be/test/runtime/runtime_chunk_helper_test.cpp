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

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "column/chunk.h"
#include "column/field.h"
#include "column/schema.h"
#include "common/config_exec_fwd.h"
#include "common/object_pool.h"
#include "gtest/gtest.h"
#include "runtime/chunk_helper.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {

class RuntimeChunkHelperDescriptorTest : public ::testing::Test {
protected:
    LogicalType _primitive_type[9] = {LogicalType::TYPE_TINYINT, LogicalType::TYPE_SMALLINT, LogicalType::TYPE_INT,
                                      LogicalType::TYPE_BIGINT,  LogicalType::TYPE_LARGEINT, LogicalType::TYPE_FLOAT,
                                      LogicalType::TYPE_DOUBLE,  LogicalType::TYPE_VARCHAR,  LogicalType::TYPE_CHAR};

    TSlotDescriptor _create_slot_desc(LogicalType type, const std::string& col_name, int col_pos);
    TupleDescriptor* _create_tuple_desc();

    RuntimeState _runtime_state;
    ObjectPool _pool;
};

TSlotDescriptor RuntimeChunkHelperDescriptorTest::_create_slot_desc(LogicalType type, const std::string& col_name,
                                                                    int col_pos) {
    TSlotDescriptorBuilder builder;

    if (type == LogicalType::TYPE_VARCHAR || type == LogicalType::TYPE_CHAR) {
        return builder.string_type(1024).column_name(col_name).column_pos(col_pos).nullable(false).build();
    } else {
        return builder.type(type).column_name(col_name).column_pos(col_pos).nullable(false).build();
    }
}

TupleDescriptor* RuntimeChunkHelperDescriptorTest::_create_tuple_desc() {
    TDescriptorTableBuilder table_builder;
    TTupleDescriptorBuilder tuple_builder;

    for (size_t i = 0; i < 9; i++) {
        tuple_builder.add_slot(_create_slot_desc(_primitive_type[i], "c" + std::to_string(i), 0));
    }

    tuple_builder.build(&table_builder);

    std::vector<TTupleId> row_tuples = std::vector<TTupleId>{0};
    DescriptorTbl* tbl = nullptr;
    CHECK(DescriptorTbl::create(&_runtime_state, &_pool, table_builder.desc_tbl(), &tbl, config::vector_chunk_size)
                  .ok());

    auto* row_desc = _pool.add(new RowDescriptor(*tbl, row_tuples));
    auto* tuple_desc = row_desc->tuple_descriptors()[0];

    return tuple_desc;
}

TEST(RuntimeChunkHelperTest, checked_schema_construction) {
    Fields fields;
    fields.emplace_back(std::make_shared<Field>(0, "c0", TYPE_INT, false));
    fields.emplace_back(std::make_shared<Field>(1, "c1", TYPE_VARCHAR, true));
    Schema schema(std::move(fields), DUP_KEYS, {});

    auto chunk_or = RuntimeChunkHelper::new_chunk_checked(schema, 8);
    ASSERT_TRUE(chunk_or.ok()) << chunk_or.status();
    ASSERT_EQ(2, chunk_or.value()->num_columns());

    auto mutable_chunk_or = RuntimeChunkHelper::new_mutable_chunk_checked(schema, 8);
    ASSERT_TRUE(mutable_chunk_or.ok()) << mutable_chunk_or.status();
    ASSERT_EQ(2, mutable_chunk_or.value()->num_columns());

    auto pooled_chunk_or = RuntimeChunkHelper::new_chunk_pooled_checked(schema, 8);
    ASSERT_TRUE(pooled_chunk_or.ok()) << pooled_chunk_or.status();
    std::unique_ptr<Chunk> pooled_chunk(pooled_chunk_or.value());
    ASSERT_EQ(2, pooled_chunk->num_columns());
}

TEST_F(RuntimeChunkHelperDescriptorTest, new_chunk_with_tuple) {
    auto* tuple_desc = _create_tuple_desc();

    auto chunk = RuntimeChunkHelper::new_chunk(*tuple_desc, 1024);

    // check
    ASSERT_EQ(chunk->num_columns(), 9);
    ASSERT_EQ(chunk->get_column_by_slot_id(0)->get_name(), "integral-1");
    ASSERT_EQ(chunk->get_column_by_slot_id(1)->get_name(), "integral-2");
    ASSERT_EQ(chunk->get_column_by_slot_id(2)->get_name(), "integral-4");
    ASSERT_EQ(chunk->get_column_by_slot_id(3)->get_name(), "integral-8");
    ASSERT_EQ(chunk->get_column_by_slot_id(4)->get_name(), "int128");
    ASSERT_EQ(chunk->get_column_by_slot_id(5)->get_name(), "float-4");
    ASSERT_EQ(chunk->get_column_by_slot_id(6)->get_name(), "float-8");
    ASSERT_EQ(chunk->get_column_by_slot_id(7)->get_name(), "binary");
    ASSERT_EQ(chunk->get_column_by_slot_id(8)->get_name(), "binary");
}

TEST_F(RuntimeChunkHelperDescriptorTest, ReorderChunk) {
    auto* tuple_desc = _create_tuple_desc();

    auto reversed_slots = tuple_desc->slots();
    std::reverse(reversed_slots.begin(), reversed_slots.end());
    auto chunk = RuntimeChunkHelper::new_chunk(reversed_slots, 1024);

    // check
    ASSERT_EQ(chunk->num_columns(), 9);
    ASSERT_EQ(chunk->columns()[8]->get_name(), "integral-1");
    ASSERT_EQ(chunk->columns()[7]->get_name(), "integral-2");
    ASSERT_EQ(chunk->columns()[6]->get_name(), "integral-4");
    ASSERT_EQ(chunk->columns()[5]->get_name(), "integral-8");
    ASSERT_EQ(chunk->columns()[4]->get_name(), "int128");
    ASSERT_EQ(chunk->columns()[3]->get_name(), "float-4");
    ASSERT_EQ(chunk->columns()[2]->get_name(), "float-8");
    ASSERT_EQ(chunk->columns()[1]->get_name(), "binary");
    ASSERT_EQ(chunk->columns()[0]->get_name(), "binary");

    RuntimeChunkHelper::reorder_chunk(*tuple_desc, chunk.get());
    // check
    ASSERT_EQ(chunk->num_columns(), 9);
    ASSERT_EQ(chunk->columns()[0]->get_name(), "integral-1");
    ASSERT_EQ(chunk->columns()[1]->get_name(), "integral-2");
    ASSERT_EQ(chunk->columns()[2]->get_name(), "integral-4");
    ASSERT_EQ(chunk->columns()[3]->get_name(), "integral-8");
    ASSERT_EQ(chunk->columns()[4]->get_name(), "int128");
    ASSERT_EQ(chunk->columns()[5]->get_name(), "float-4");
    ASSERT_EQ(chunk->columns()[6]->get_name(), "float-8");
    ASSERT_EQ(chunk->columns()[7]->get_name(), "binary");
    ASSERT_EQ(chunk->columns()[8]->get_name(), "binary");
}

} // namespace starrocks
