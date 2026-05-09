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

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/chunk_builder.h"
#include "column/column_helper.h"
#include "column/decimalv3_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/schema.h"
#include "column/struct_column.h"

namespace starrocks {

class ChunkBuilderCoreTest : public ::testing::Test {
protected:
    FieldPtr make_field(ColumnId id, std::string_view name, LogicalType type, bool nullable) {
        return std::make_shared<Field>(id, name, type, nullable);
    }

    Schema make_schema() {
        Fields fields;
        fields.emplace_back(make_field(3, "c0", TYPE_INT, false));
        fields.emplace_back(make_field(9, "c1", TYPE_VARCHAR, true));
        return Schema(std::move(fields));
    }
};

TEST_F(ChunkBuilderCoreTest, ColumnFromFieldType) {
    auto int_column = ChunkBuilder::column_from_field_type(TYPE_INT, false);
    ASSERT_EQ("integral-4", int_column->get_name());
    ASSERT_FALSE(int_column->is_nullable());

    auto nullable_varchar = ChunkBuilder::column_from_field_type(TYPE_VARCHAR, true);
    ASSERT_TRUE(nullable_varchar->is_nullable());
    ASSERT_EQ("binary", ColumnHelper::get_data_column(nullable_varchar.get())->get_name());
}

TEST_F(ChunkBuilderCoreTest, ColumnFromField) {
    Field decimal_field(1, "decimal_col", TYPE_DECIMAL32, 9, 2, true);
    auto decimal_column = ChunkBuilder::column_from_field(decimal_field);
    ASSERT_TRUE(decimal_column->is_nullable());
    auto* decimal_data = down_cast<Decimal32Column*>(ColumnHelper::get_data_column(decimal_column.get()));
    ASSERT_EQ(9, decimal_data->precision());
    ASSERT_EQ(2, decimal_data->scale());

    Field array_field(2, "array_col", get_array_type_info(get_type_info(TYPE_INT)), true);
    array_field.add_sub_field(Field(3, "item", TYPE_INT, true));
    auto array_column = ChunkBuilder::column_from_field(array_field);
    ASSERT_TRUE(array_column->is_nullable());
    ASSERT_TRUE(ColumnHelper::get_data_column(array_column.get())->is_array());

    Field map_field(4, "map_col", get_map_type_info(get_type_info(TYPE_VARCHAR), get_type_info(TYPE_INT)), false);
    map_field.add_sub_field(Field(5, "key", TYPE_VARCHAR, true));
    map_field.add_sub_field(Field(6, "value", TYPE_INT, true));
    auto map_column = ChunkBuilder::column_from_field(map_field);
    ASSERT_FALSE(map_column->is_nullable());
    ASSERT_TRUE(map_column->is_map());

    Field struct_field(7, "struct_col", get_struct_type_info({get_type_info(TYPE_INT)}), true);
    struct_field.add_sub_field(Field(8, "member", TYPE_INT, false));
    auto struct_column = ChunkBuilder::column_from_field(struct_field);
    ASSERT_TRUE(struct_column->is_nullable());
    ASSERT_TRUE(ColumnHelper::get_data_column(struct_column.get())->is_struct());
}

TEST_F(ChunkBuilderCoreTest, NewChunkFromSchema) {
    auto schema = make_schema();
    auto chunk = ChunkBuilder::new_chunk(schema, 16);

    ASSERT_EQ(2, chunk->num_columns());
    ASSERT_EQ(3, chunk->schema()->field(0)->id());
    ASSERT_EQ(9, chunk->schema()->field(1)->id());
    ASSERT_EQ("integral-4", chunk->get_column_by_id(3)->get_name());
    ASSERT_TRUE(chunk->get_column_by_id(9)->is_nullable());
}

TEST_F(ChunkBuilderCoreTest, NewMutableChunkFromSchema) {
    auto schema = make_schema();
    auto chunk = ChunkBuilder::new_mutable_chunk(schema, 16);

    ASSERT_EQ(2, chunk->num_columns());
    ASSERT_EQ(2, chunk->schema()->num_fields());
    ASSERT_EQ("integral-4", chunk->get_column_by_index(0)->get_name());
    ASSERT_TRUE(chunk->get_column_by_index(1)->is_nullable());
}

TEST_F(ChunkBuilderCoreTest, NewChunkPooledFromSchema) {
    auto schema = make_schema();
    std::unique_ptr<Chunk> chunk(ChunkBuilder::new_chunk_pooled(schema, 16));

    ASSERT_EQ(2, chunk->num_columns());
    ASSERT_EQ(2, chunk->schema()->num_fields());
    ASSERT_EQ("integral-4", chunk->get_column_by_id(3)->get_name());
    ASSERT_TRUE(chunk->get_column_by_id(9)->is_nullable());
}

TEST_F(ChunkBuilderCoreTest, SchemaHelpers) {
    auto schema = std::make_shared<Schema>(make_schema());
    std::vector<int> keys = {1, 0};

    auto non_nullable_schema = ChunkBuilder::get_non_nullable_schema(schema, &keys);
    ASSERT_EQ(2, non_nullable_schema->num_fields());
    ASSERT_FALSE(non_nullable_schema->field(0)->is_nullable());
    ASSERT_FALSE(non_nullable_schema->field(1)->is_nullable());
    ASSERT_TRUE(non_nullable_schema->field(0)->is_key());
    ASSERT_FALSE(non_nullable_schema->field(1)->is_key());
    ASSERT_EQ(9, ChunkBuilder::max_column_id(*non_nullable_schema));
}

} // namespace starrocks
