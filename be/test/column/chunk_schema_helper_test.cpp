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

#include "column/chunk_schema_helper.h"

#include <memory>
#include <utility>
#include <vector>

#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/field.h"
#include "column/geo_column.h"
#include "column/schema.h"
#include "gtest/gtest.h"
#include "types/type_descriptor.h"

namespace starrocks {

TEST(ChunkSchemaHelperTest, schema_helpers) {
    Fields fields;
    fields.emplace_back(std::make_shared<Field>(3, "c0", TYPE_INT, true));
    fields.emplace_back(std::make_shared<Field>(9, "c1", TYPE_CHAR, true));
    fields.emplace_back(std::make_shared<Field>(5, "c2", TYPE_VARCHAR, false));
    auto schema = std::make_shared<Schema>(std::move(fields), DUP_KEYS, std::vector<ColumnId>{});

    ASSERT_EQ(9, ChunkSchemaHelper::max_column_id(*schema));
    ASSERT_EQ((std::vector<size_t>{1}), ChunkSchemaHelper::get_char_field_indexes(*schema));

    std::vector<int> keys{1, 0, 1};
    auto non_nullable_schema = ChunkSchemaHelper::get_non_nullable_schema(schema, &keys);
    ASSERT_EQ(3, non_nullable_schema->num_fields());
    ASSERT_FALSE(non_nullable_schema->field(0)->is_nullable());
    ASSERT_FALSE(non_nullable_schema->field(1)->is_nullable());
    ASSERT_TRUE(non_nullable_schema->field(0)->is_key());
    ASSERT_FALSE(non_nullable_schema->field(1)->is_key());
    ASSERT_TRUE(non_nullable_schema->field(2)->is_key());
}

TEST(ChunkSchemaHelperTest, createDummyChunk) {
    auto chunk = ChunkSchemaHelper::createDummyChunk();
    ASSERT_EQ(1, chunk->num_columns());
    ASSERT_EQ(1, chunk->num_rows());
    ASSERT_TRUE(chunk->is_slot_exist(0));
}

TEST(ChunkSchemaHelperTest, non_nullable_schema_preserves_geo_value) {
    const GeoTypeDescriptor descriptor{GEO_LOGICAL_TYPE_GEOMETRY, GEO_EDGE_ALGORITHM_PLANAR, "EPSG:3857", 3857};
    auto field = Field::create_geo(1, "g", TypeDescriptor::create_geo_type(TYPE_GEOMETRY, descriptor), true);
    ASSERT_TRUE(field.ok()) << field.status();
    Fields fields{std::make_shared<Field>(0, "c", TYPE_CHAR, false), field->copy()};
    auto schema = std::make_shared<Schema>(std::move(fields), DUP_KEYS, std::vector<ColumnId>{});
    EXPECT_EQ(TYPE_CHAR, schema->field(0)->type()->type());
    EXPECT_EQ(TYPE_CHAR, schema->field(0)->logical_type());
    EXPECT_EQ(nullptr, schema->field(1)->type());
    EXPECT_EQ(TYPE_GEOMETRY, schema->field(1)->logical_type());
    EXPECT_EQ((std::vector<size_t>{0}), ChunkSchemaHelper::get_char_field_indexes(*schema));

    const std::vector<int> keys{1, 0};
    for (const std::vector<int>* override_keys : {static_cast<const std::vector<int>*>(nullptr), &keys}) {
        auto non_nullable = ChunkSchemaHelper::get_non_nullable_schema(schema, override_keys);
        ASSERT_NE(nullptr, non_nullable);
        EXPECT_EQ(override_keys != nullptr, non_nullable->field(0)->is_key());
        EXPECT_FALSE(non_nullable->field(1)->is_key());
        EXPECT_FALSE(non_nullable->field(1)->is_nullable());
        ASSERT_NE(nullptr, non_nullable->field(1)->geo_descriptor());
        EXPECT_EQ(descriptor, *non_nullable->field(1)->geo_descriptor());
        auto chunk = ChunkFactory::new_chunk(*non_nullable, 8);
        const auto* geo = dynamic_cast<const GeoColumn*>(chunk->get_column_by_index(1).get());
        ASSERT_NE(nullptr, geo);
        EXPECT_EQ(descriptor, geo->descriptor().type);
    }
    EXPECT_TRUE(schema->field(1)->is_nullable());
    const std::vector<int> invalid_keys{1, 1};
    EXPECT_EQ(nullptr, ChunkSchemaHelper::get_non_nullable_schema(schema, &invalid_keys));
}

} // namespace starrocks
