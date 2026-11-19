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

#include "column/schema.h"

#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/geo_column.h"
#include "column/nullable_column.h"
#include "types/type_descriptor.h"

namespace starrocks {

TEST(SchemaGeoConversionTest, copies_value_field_only_for_same_geo_type) {
    const GeoTypeDescriptor descriptor{GEO_LOGICAL_TYPE_GEOMETRY, GEO_EDGE_ALGORITHM_PLANAR, "EPSG:3857", 3857};
    auto field = Field::create_geo(1, "g", TypeDescriptor::create_geo_type(TYPE_GEOMETRY, descriptor), true);
    ASSERT_TRUE(field.ok()) << field.status();
    field->set_uid(99);
    Schema schema(Fields{std::make_shared<Field>(0, "c", TYPE_CHAR, false), field->copy()}, DUP_KEYS, {});
    Schema dst;
    ASSERT_TRUE(schema.convert_to(&dst, {TYPE_CHAR, TYPE_GEOMETRY}).ok());
    const auto copied = dst.field(1);
    EXPECT_NE(schema.field(1), copied);
    EXPECT_EQ(1, copied->id());
    EXPECT_EQ("g", copied->name());
    EXPECT_EQ(99, copied->uid());
    EXPECT_TRUE(copied->is_nullable());
    ASSERT_NE(nullptr, copied->geo_descriptor());
    EXPECT_EQ(descriptor, *copied->geo_descriptor());

    auto chunk = ChunkFactory::new_chunk(dst, 8);
    const auto* nullable = dynamic_cast<const NullableColumn*>(chunk->get_column_by_index(1).get());
    ASSERT_NE(nullptr, nullable);
    const auto* geo = dynamic_cast<const GeoColumn*>(nullable->data_column().get());
    ASSERT_NE(nullptr, geo);
    EXPECT_EQ(descriptor, geo->descriptor().type);

    EXPECT_TRUE(schema.convert_to(&dst, {TYPE_VARCHAR, TYPE_GEOGRAPHY}).is_not_supported());
    EXPECT_TRUE(schema.convert_to(&dst, {TYPE_CHAR, TYPE_VARBINARY}).is_not_supported());
    EXPECT_TRUE(schema.convert_to(&dst, {TYPE_GEOMETRY, TYPE_GEOMETRY}).is_not_supported());
    EXPECT_EQ(copied, dst.field(1));
    EXPECT_EQ(TYPE_CHAR, dst.field(0)->logical_type());
    ASSERT_TRUE(schema.convert_to(&dst, {TYPE_VARCHAR, TYPE_GEOMETRY}).ok());
    EXPECT_EQ(TYPE_VARCHAR, dst.field(0)->type()->type());
}

TEST(SchemaGeoConversionTest, converted_schema_owns_name_index) {
    const GeoTypeDescriptor descriptor{GEO_LOGICAL_TYPE_GEOMETRY, GEO_EDGE_ALGORITHM_PLANAR, "custom:LocalGrid",
                                       std::nullopt};
    for (const bool reuse_dst : {false, true}) {
        SCOPED_TRACE(reuse_dst);
        Schema dst;
        if (reuse_dst) {
            dst.append(std::make_shared<Field>(0, "old", TYPE_INT, false));
            const Schema shared(dst);
            dst.append(std::make_shared<Field>(1, "old_append", TYPE_INT, false));
            EXPECT_EQ(nullptr, shared.get_field_by_name("old_append"));
        }
        std::weak_ptr<Field> source_geo;
        {
            auto field = Field::create_geo(1, "geo", TypeDescriptor::create_geo_type(TYPE_GEOMETRY, descriptor), true);
            ASSERT_TRUE(field.ok()) << field.status();
            Schema source(Fields{std::make_shared<Field>(0, "c", TYPE_CHAR, false), field->copy()}, DUP_KEYS, {});
            source_geo = source.field(1);
            if (reuse_dst) {
                const auto old = dst.field(0);
                const auto old_append = dst.field(1);
                ASSERT_TRUE(source.convert_to(&dst, {TYPE_VARCHAR, TYPE_GEOGRAPHY}).is_not_supported());
                ASSERT_EQ(2, dst.num_fields());
                EXPECT_EQ(old, dst.get_field_by_name("old"));
                EXPECT_EQ(old_append, dst.get_field_by_name("old_append"));
            }

            ASSERT_TRUE(source.convert_to(&dst, {TYPE_CHAR, TYPE_GEOMETRY}).ok());
            ASSERT_EQ(nullptr, dst.get_field_by_name("old"));
            ASSERT_EQ(nullptr, dst.get_field_by_name("old_append"));
            dst.append(std::make_shared<Field>(2, "dst_only", TYPE_INT, false));
            ASSERT_EQ(-1, source.get_field_index_by_name("dst_only"));
        }
        ASSERT_TRUE(source_geo.expired());
        EXPECT_EQ(dst.field(2), dst.get_field_by_name("dst_only"));
        const auto geo = dst.get_field_by_name("geo");
        ASSERT_EQ(dst.field(1), geo);
        EXPECT_TRUE(geo->is_nullable());
        ASSERT_NE(nullptr, geo->geo_descriptor());
        EXPECT_EQ(descriptor, *geo->geo_descriptor());
    }
}

class SchemaTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}

    Fields make_fields(size_t size) {
        Fields fields;
        for (size_t i = 0; i < size; i++) {
            fields.emplace_back(make_field(i));
        }
        return fields;
    }

    std::string make_string(size_t i) { return std::string("c").append(std::to_string(static_cast<int32_t>(i))); }

    FieldPtr make_field(size_t i) { return std::make_shared<Field>(i, make_string(i), get_type_info(TYPE_INT), false); }

    Schema* make_schema(size_t i) {
        Fields fields = make_fields(i);
        return new Schema(fields);
    }

    void check_field(const FieldPtr& field, size_t i) {
        ASSERT_EQ(make_string(i), field->name());
        ASSERT_EQ(i, field->id());
        ASSERT_FALSE(field->is_nullable());
    }

    void check_schema(Schema* schema, size_t size) {
        ASSERT_EQ(schema->num_fields(), size);
        for (size_t i = 0; i < size; i++) {
            FieldPtr field = schema->get_field_by_name(make_string(i));
            check_field(field, i);
        }
    }
};

TEST_F(SchemaTest, test_construct) {
    Fields fields1 = make_fields(2);
    auto* schema1 = new Schema(fields1);
    check_schema(schema1, 2);
    delete schema1;

    Fields fields2 = make_fields(2);
    auto* schema2 = new Schema(std::move(fields2));
    check_schema(schema2, 2);
    delete schema2;
}

TEST_F(SchemaTest, append) {
    Schema* schema = make_schema(2);
    schema->append(make_field(2));

    check_schema(schema, 3);
    delete schema;
}

TEST_F(SchemaTest, insert) {
    Schema* schema = make_schema(0);
    schema->append(make_field(0));
    schema->append(make_field(2));
    schema->insert(1, make_field(1));

    check_schema(schema, 3);
    delete schema;
}

TEST_F(SchemaTest, remove) {
    Schema* schema = make_schema(2);
    schema->remove(1);

    check_schema(schema, 1);
    delete schema;
}

TEST_F(SchemaTest, field) {
    Schema* schema = make_schema(2);
    const FieldPtr& ptr = schema->field(0);

    check_field(ptr, 0);
    delete schema;
}

TEST_F(SchemaTest, field_names) {
    Schema* schema = make_schema(2);
    std::vector<std::string> names = schema->field_names();

    std::vector<std::string> check_names;
    check_names.emplace_back("c0");
    check_names.emplace_back("c1");
    ASSERT_EQ(check_names, names);
    delete schema;
}

TEST_F(SchemaTest, get_field_by_name) {
    Schema* schema = make_schema(2);

    FieldPtr ptr1 = schema->get_field_by_name("c0");
    check_field(ptr1, 0);

    FieldPtr ptr2 = schema->get_field_by_name("c2");
    ASSERT_EQ(ptr2, nullptr);
    delete schema;
}

TEST_F(SchemaTest, get_field_index) {
    Schema* schema = make_schema(2);
    size_t idx1 = schema->get_field_index_by_name("c0");
    ASSERT_EQ(0, idx1);
    size_t idx2 = schema->get_field_index_by_name("c2");
    ASSERT_EQ(-1, idx2);
    delete schema;
}

} // namespace starrocks
