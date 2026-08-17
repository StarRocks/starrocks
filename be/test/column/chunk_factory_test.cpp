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

#include "column/chunk_factory.h"

#include <memory>
#include <utility>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/field.h"
#include "column/schema.h"
#include "gtest/gtest.h"
#include "types/type_info.h"

namespace starrocks {

TEST(ChunkFactoryTest, column_from_field_type) {
    auto column = ChunkFactory::column_from_field_type(TYPE_INT, false);
    ASSERT_EQ("integral-4", column->get_name());
    ASSERT_FALSE(column->is_nullable());

    auto nullable_column = ChunkFactory::column_from_field_type(TYPE_INT, true);
    ASSERT_TRUE(nullable_column->is_nullable());
}

TEST(ChunkFactoryTest, column_from_field) {
    Field decimal_field(0, "decimal_col", TYPE_DECIMAL64, 10, 2, true);
    auto decimal_column = ChunkFactory::column_from_field(decimal_field);
    ASSERT_TRUE(decimal_column->is_nullable());

    Field array_field(1, "array_col", get_array_type_info(get_type_info(TYPE_INT)), true);
    array_field.add_sub_field(Field(1, "item", TYPE_INT, true));
    auto array_column = ChunkFactory::column_from_field(array_field);
    ASSERT_TRUE(array_column->is_nullable());
    ASSERT_TRUE(ColumnHelper::get_data_column(array_column.get())->is_array());

    Field map_field(2, "map_col", get_map_type_info(get_type_info(TYPE_INT), get_type_info(TYPE_VARCHAR)), true);
    map_field.add_sub_field(Field(2, "key", TYPE_INT, false));
    map_field.add_sub_field(Field(2, "value", TYPE_VARCHAR, true));
    auto map_column = ChunkFactory::column_from_field(map_field);
    ASSERT_TRUE(map_column->is_nullable());
    ASSERT_TRUE(ColumnHelper::get_data_column(map_column.get())->is_map());

    Field struct_field(3, "struct_col", get_struct_type_info({get_type_info(TYPE_INT), get_type_info(TYPE_VARCHAR)}),
                       true);
    struct_field.add_sub_field(Field(3, "field1", TYPE_INT, false));
    struct_field.add_sub_field(Field(3, "field2", TYPE_VARCHAR, true));
    auto struct_column = ChunkFactory::column_from_field(struct_field);
    ASSERT_TRUE(struct_column->is_nullable());
    ASSERT_TRUE(ColumnHelper::get_data_column(struct_column.get())->is_struct());
}

TEST(ChunkFactoryTest, new_chunks_from_schema) {
    Fields fields;
    fields.emplace_back(std::make_shared<Field>(0, "c0", TYPE_INT, false));
    fields.emplace_back(std::make_shared<Field>(1, "c1", TYPE_VARCHAR, true));
    Schema schema(std::move(fields), DUP_KEYS, {});

    auto chunk = ChunkFactory::new_chunk(schema, 8);
    ASSERT_EQ(2, chunk->num_columns());
    ASSERT_EQ(0, chunk->num_rows());
    ASSERT_EQ("integral-4", chunk->get_column_by_index(0)->get_name());
    ASSERT_TRUE(chunk->get_column_by_index(1)->is_nullable());

    auto mutable_chunk = ChunkFactory::new_mutable_chunk(schema, 8);
    ASSERT_EQ(2, mutable_chunk->num_columns());
    ASSERT_EQ(0, mutable_chunk->num_rows());

    std::unique_ptr<Chunk> pooled_chunk(ChunkFactory::new_chunk_pooled(schema, 8));
    ASSERT_EQ(2, pooled_chunk->num_columns());
    ASSERT_EQ(0, pooled_chunk->num_rows());
}

} // namespace starrocks
