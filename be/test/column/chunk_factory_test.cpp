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
#include <optional>
#include <utility>

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/field.h"
#include "column/geo_column.h"
#include "column/map_column.h"
#include "column/schema.h"
#include "gtest/gtest.h"
#include "gutil/casts.h"
#include "types/geo_type_info.h"
#include "types/type_descriptor.h"
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
    array_column->check_or_die();

    // The element sub-field is declared NOT NULL, but the elements column must still be nullable.
    Field array_field_with_not_null_element(1, "array_col2", get_array_type_info(get_type_info(TYPE_INT)), true);
    array_field_with_not_null_element.add_sub_field(Field(1, "item", TYPE_INT, false));
    auto array_column2 = ChunkFactory::column_from_field(array_field_with_not_null_element);
    auto* array_data2 = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(array_column2.get()));
    ASSERT_TRUE(array_data2->elements_column()->is_nullable());
    array_column2->check_or_die();

    // Map keys are always declared NOT NULL in the tablet schema, but the keys column must still be nullable.
    Field map_field(2, "map_col", get_map_type_info(get_type_info(TYPE_INT), get_type_info(TYPE_VARCHAR)), true);
    map_field.add_sub_field(Field(2, "key", TYPE_INT, true));
    map_field.add_sub_field(Field(2, "value", TYPE_VARCHAR, true));
    auto map_column = ChunkFactory::column_from_field(map_field);
    ASSERT_TRUE(map_column->is_nullable());
    ASSERT_TRUE(ColumnHelper::get_data_column(map_column.get())->is_map());
    auto* map_data = down_cast<MapColumn*>(ColumnHelper::get_data_column(map_column.get()));
    ASSERT_TRUE(map_data->keys_column()->is_nullable());
    ASSERT_TRUE(map_data->values_column()->is_nullable());
    map_column->check_or_die();

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

TEST(ChunkFactoryTest, materializes_typed_geo_fields) {
    const GeoTypeDescriptor descriptors[] = {
            {GEO_LOGICAL_TYPE_GEOGRAPHY, GEO_EDGE_ALGORITHM_SPHERICAL, "OGC:CRS84", std::nullopt},
            {GEO_LOGICAL_TYPE_GEOMETRY, GEO_EDGE_ALGORITHM_PLANAR, "EPSG:3857", 3857},
            {GEO_LOGICAL_TYPE_GEOMETRY, GEO_EDGE_ALGORITHM_PLANAR, "custom:LocalSurveyGrid-Moscow-2026", std::nullopt},
    };
    for (const auto& descriptor : descriptors) {
        SCOPED_TRACE(descriptor.crs);
        const auto type = descriptor.logical_type == GEO_LOGICAL_TYPE_GEOGRAPHY ? TYPE_GEOGRAPHY : TYPE_GEOMETRY;
        FieldPtr plain;
        {
            auto info = get_geo_type_info(TypeDescriptor::create_geo_type(type, descriptor));
            ASSERT_TRUE(info.ok()) << info.status();
            plain = std::make_shared<Field>(1, "geo", *info, false);
            EXPECT_EQ(*info, plain->type());
        }
        ASSERT_NE(nullptr, plain->type());
        EXPECT_EQ(type, plain->type()->type());
        plain->set_uid(42);
        auto copied = plain->copy();
        EXPECT_EQ(42, copied->uid());
        EXPECT_EQ(plain->type(), copied->type());
        auto renamed = copied->with_name("renamed_geo");
        EXPECT_EQ(plain->type(), renamed->type());
        EXPECT_EQ("geo", plain->name());
        EXPECT_EQ("renamed_geo", renamed->name());
        Field nullable(2, "nullable_geo", plain->type(), false);
        auto nullable_field = nullable.with_nullable(true);
        EXPECT_FALSE(nullable.is_nullable());
        EXPECT_TRUE(nullable_field->is_nullable());
        EXPECT_EQ(nullable.type(), nullable_field->type());
        nullable_field->set_uid(43);
        Fields fields{std::make_shared<Field>(0, "control", TYPE_INT, false), std::move(renamed),
                      std::move(nullable_field)};
        Schema schema(std::move(fields), DUP_KEYS, {});
        Schema projection(&schema, {2, 0, 1});
        EXPECT_EQ(43, projection.field(0)->uid());
        EXPECT_EQ(nullable.type(), projection.field(0)->type());
        EXPECT_EQ(plain->type(), projection.field(2)->type());

        const auto check = [&](const auto& chunk) {
            ASSERT_EQ(3, chunk->num_columns());
            ASSERT_EQ(0, chunk->num_rows());
            ASSERT_EQ("integral-4", chunk->get_column_by_index(1)->get_name());
            for (size_t index : {0, 2}) {
                const auto* column = chunk->get_column_by_index(index).get();
                ASSERT_EQ(index == 0, column->is_nullable());
                const auto* geo = dynamic_cast<const GeoColumn*>(ColumnHelper::get_data_column(column));
                ASSERT_NE(nullptr, geo);
                EXPECT_EQ(descriptor, geo->descriptor().type);
            }
        };
        check(ChunkFactory::new_chunk(projection, 8));
        check(ChunkFactory::new_mutable_chunk(projection, 8));
        check(std::unique_ptr<Chunk>(ChunkFactory::new_chunk_pooled(projection, 8)));
    }
}

TEST(ChunkFactoryTest, rejects_missing_or_mismatched_geo_descriptor) {
    const GeoTypeDescriptor geography{GEO_LOGICAL_TYPE_GEOGRAPHY, GEO_EDGE_ALGORITHM_SPHERICAL, "OGC:CRS84"};
    const GeoTypeDescriptor geometry{GEO_LOGICAL_TYPE_GEOMETRY, GEO_EDGE_ALGORITHM_PLANAR, "EPSG:3857", 3857};
    for (const auto type : {TYPE_GEOGRAPHY, TYPE_GEOMETRY}) {
        TypeDescriptor input(type);
        EXPECT_TRUE(get_geo_type_info(input).status().is_invalid_argument());
        input.geo_type = type == TYPE_GEOGRAPHY ? geometry : geography;
        EXPECT_TRUE(get_geo_type_info(input).status().is_invalid_argument());
    }
}

} // namespace starrocks
