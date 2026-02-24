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

#include "storage/tablet_schema.h"

#include <gtest/gtest.h>

#include "storage/tablet_schema_helper.h"
#include "storage/tablet_schema_map.h"
#include "util/json_util.h"

namespace starrocks {
// NOLINTNEXTLINE
TEST(TabletSchemaTest, test_estimate_row_size) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_short_key_columns(1);

    auto c1 = schema_pb.add_column();
    c1->set_unique_id(1);
    c1->set_name("c1");
    c1->set_type("TINYINT");
    c1->set_is_key(true);

    auto c2 = schema_pb.add_column();
    c2->set_unique_id(2);
    c2->set_name("c2");
    c2->set_type("SMALLINT");
    c2->set_is_key(false);

    auto c3 = schema_pb.add_column();
    c3->set_unique_id(3);
    c3->set_name("c3");
    c3->set_type("DATE_V2");
    c3->set_is_key(false);

    auto c4 = schema_pb.add_column();
    c4->set_unique_id(4);
    c4->set_name("c4");
    c4->set_type("INT");
    c4->set_is_key(false);

    auto c5 = schema_pb.add_column();
    c5->set_unique_id(5);
    c5->set_name("c5");
    c5->set_type("BIGINT");
    c5->set_is_key(false);

    auto c6 = schema_pb.add_column();
    c6->set_unique_id(6);
    c6->set_name("c6");
    c6->set_type("LARGEINT");
    c6->set_is_key(false);

    auto c7 = schema_pb.add_column();
    c7->set_unique_id(7);
    c7->set_name("c7");
    c7->set_type("VARCHAR");
    c7->set_is_key(false);

    TabletSchema tablet_schema(schema_pb);
    size_t row_size = tablet_schema.estimate_row_size(100);
    ASSERT_EQ(row_size, 135);
}

TEST(TabletSchemaTest, test_schema_with_index) {
    // init tablet schema
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_short_key_columns(1);

    auto c1 = schema_pb.add_column();
    c1->set_unique_id(1);
    c1->set_name("f1");
    c1->set_type("VARCHAR");
    c1->set_is_key(true);

    auto c2 = schema_pb.add_column();
    c2->set_unique_id(2);
    c2->set_name("f2");
    c2->set_type("SMALLINT");
    c2->set_is_key(false);

    // init tablet index
    TabletIndexPB index_pb;

    std::map<std::string, std::string> common_map;
    std::map<std::string, std::string> index_map;
    std::map<std::string, std::string> search_map;
    std::map<std::string, std::string> extra_map;
    common_map.emplace("imp_type", "clucene");

    std::map<std::string, std::map<std::string, std::string>> properties_map;
    properties_map.emplace("common_properties", common_map);
    properties_map.emplace("index_properties", index_map);
    properties_map.emplace("search_properties", search_map);
    properties_map.emplace("extra_properties", extra_map);

    std::string json_properties = to_json(properties_map);
    index_pb.set_index_id(0);
    index_pb.set_index_name("test_index");
    index_pb.set_index_type(GIN);
    index_pb.add_col_unique_id(1);
    index_pb.set_index_properties(json_properties);

    auto* tablet_index = schema_pb.add_table_indices();
    *tablet_index = index_pb;

    TabletSchema tablet_schema(schema_pb);

    std::shared_ptr<TabletIndex> tablet_index_ptr;
    ASSERT_TRUE(tablet_schema.get_indexes_for_column(1, GIN, tablet_index_ptr).ok());
    ASSERT_TRUE(tablet_index_ptr.get() != nullptr);

    ASSERT_TRUE(tablet_schema.has_index(1, GIN));

    std::unordered_map<IndexType, TabletIndex> res;
    ASSERT_TRUE(tablet_schema.get_indexes_for_column(1, &res).ok());
    ASSERT_FALSE(res.empty());

    ASSERT_FALSE(tablet_schema.has_separate_sort_key());
}

TEST(TabletSchemaTest, test_is_support_checksum) {
    // struct<int, varchar>
    TabletColumn struct_column1 = create_struct(0, true);
    TabletColumn f11 = create_int_value(1, STORAGE_AGGREGATE_NONE, true);
    struct_column1.add_sub_column(f11);
    TabletColumn f12 = create_varchar_key(2, true);
    struct_column1.add_sub_column(f12);
    ASSERT_TRUE(struct_column1.is_support_checksum());

    // struct<int, bitmap>
    TabletColumn struct_column2 = create_struct(0, true);
    TabletColumn f21 = create_int_value(1, STORAGE_AGGREGATE_NONE, true);
    struct_column2.add_sub_column(f21);
    TabletColumn f22;
    f22.set_unique_id(2);
    f22.set_type(TYPE_OBJECT);
    struct_column2.add_sub_column(f22);
    ASSERT_FALSE(struct_column2.is_support_checksum());

    // map<int, int>
    TabletColumn map_column1 = create_map(0, true);
    TabletColumn key1 = create_int_value(1, STORAGE_AGGREGATE_NONE, true);
    map_column1.add_sub_column(key1);
    TabletColumn value1 = create_int_value(2, STORAGE_AGGREGATE_NONE, true);
    map_column1.add_sub_column(value1);
    ASSERT_TRUE(map_column1.is_support_checksum());

    // map<int, bitmap>
    TabletColumn map_column2 = create_map(0, true);
    TabletColumn key2 = create_int_value(1, STORAGE_AGGREGATE_NONE, true);
    map_column2.add_sub_column(key2);
    TabletColumn value2;
    value2.set_unique_id(2);
    value2.set_type(TYPE_OBJECT);
    map_column2.add_sub_column(value2);
    ASSERT_FALSE(map_column2.is_support_checksum());
}

TEST(TabletSchemaTest, test_primary_key_encoding_type_fallback) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(PRIMARY_KEYS);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_schema_version(1);

    auto c1 = schema_pb.add_column();
    c1->set_unique_id(1);
    c1->set_name("k1");
    c1->set_type("BIGINT");
    c1->set_is_key(true);

    TabletSchema tablet_schema(schema_pb);
    ASSERT_TRUE(tablet_schema.has_valid_primary_key_encoding_type());
    ASSERT_EQ(tablet_schema.primary_key_encoding_type(), PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1);
}

TEST(TabletSchemaTest, test_primary_key_encoding_type_round_trip) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(PRIMARY_KEYS);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_schema_version(1);
    schema_pb.set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);

    auto c1 = schema_pb.add_column();
    c1->set_unique_id(1);
    c1->set_name("k1");
    c1->set_type("BIGINT");
    c1->set_is_key(true);

    TabletSchema tablet_schema(schema_pb);
    ASSERT_EQ(tablet_schema.primary_key_encoding_type(), PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2);

    TabletSchemaPB persisted_schema_pb;
    tablet_schema.to_schema_pb(&persisted_schema_pb);
    ASSERT_TRUE(persisted_schema_pb.has_primary_key_encoding_type());
    ASSERT_EQ(persisted_schema_pb.primary_key_encoding_type(), PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);

    TabletSchema reloaded_schema(persisted_schema_pb);
    ASSERT_EQ(reloaded_schema.primary_key_encoding_type(), PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2);
}

TEST(TabletSchemaTest, test_primary_key_encoding_type_persist_none_for_non_pk_table) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_schema_version(1);

    auto c1 = schema_pb.add_column();
    c1->set_unique_id(1);
    c1->set_name("k1");
    c1->set_type("BIGINT");
    c1->set_is_key(true);

    TabletSchema tablet_schema(schema_pb);
    ASSERT_FALSE(tablet_schema.has_valid_primary_key_encoding_type());
    ASSERT_EQ(tablet_schema.primary_key_encoding_type(), PrimaryKeyEncodingType::PK_ENCODING_TYPE_NONE);

    TabletSchemaPB persisted_schema_pb;
    tablet_schema.to_schema_pb(&persisted_schema_pb);
    ASSERT_TRUE(persisted_schema_pb.has_primary_key_encoding_type());
    ASSERT_EQ(persisted_schema_pb.primary_key_encoding_type(), PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_NONE);
}

TEST(TabletSchemaTest, test_primary_key_encoding_type_or_error_for_non_pk_table) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_schema_version(1);

    auto c1 = schema_pb.add_column();
    c1->set_unique_id(1);
    c1->set_name("k1");
    c1->set_type("BIGINT");
    c1->set_is_key(true);

    TabletSchema tablet_schema(schema_pb);
    auto encoding_type = tablet_schema.primary_key_encoding_type_or_error();
    ASSERT_FALSE(encoding_type.ok());
    ASSERT_TRUE(encoding_type.status().is_internal_error());
}

TEST(TabletSchemaTest, test_primary_key_encoding_type_invalid_enum_for_pk_table) {
    // Build a valid TabletSchemaPB with a known encoding type first.
    TabletSchemaPB valid_pb;
    valid_pb.set_keys_type(PRIMARY_KEYS);
    valid_pb.set_num_short_key_columns(1);
    valid_pb.set_schema_version(1);
    valid_pb.set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V1);

    auto c1 = valid_pb.add_column();
    c1->set_unique_id(1);
    c1->set_name("k1");
    c1->set_type("BIGINT");
    c1->set_is_key(true);

    // Serialize, then tamper the encoding_type field to an unknown enum value.
    // In proto2, the deserializer moves unknown enum values to unknown fields,
    // so has_primary_key_encoding_type() returns false after parsing.
    std::string serialized;
    ASSERT_TRUE(valid_pb.SerializeToString(&serialized));

    // Locate the primary_key_encoding_type field (field 16, varint) and replace
    // its value with 9999. Field 16 tag = (16 << 3) | 0 = 0x80 0x01.
    // We rebuild the message with the tampered field to avoid byte-offset fragility.
    TabletSchemaPB tmp_pb;
    tmp_pb.set_keys_type(PRIMARY_KEYS);
    tmp_pb.set_num_short_key_columns(1);
    tmp_pb.set_schema_version(1);
    auto c2 = tmp_pb.add_column();
    c2->set_unique_id(1);
    c2->set_name("k1");
    c2->set_type("BIGINT");
    c2->set_is_key(true);
    // Do NOT set primary_key_encoding_type — serialize without it, then manually
    // append the field with an invalid varint value.
    std::string base;
    ASSERT_TRUE(tmp_pb.SerializeToString(&base));
    // Append field 16, wire type 0 (varint), value 9999 (0x270F).
    // Tag: (16 << 3) | 0 = 128 → varint 0x80 0x01
    // Value 9999: varint 0x8F 0x4E
    base.push_back(static_cast<char>(0x80));
    base.push_back(static_cast<char>(0x01));
    base.push_back(static_cast<char>(0x8F));
    base.push_back(static_cast<char>(0x4E));

    TabletSchemaPB schema_pb;
    ASSERT_TRUE(schema_pb.ParseFromString(base));
    // In proto2, the unknown enum value is not stored in the field.
    ASSERT_FALSE(schema_pb.has_primary_key_encoding_type());

    // For a PRIMARY_KEYS table without a recognized encoding type, TabletSchema
    // falls back to V1 (compatibility with older schemas that predate the field).
    TabletSchema tablet_schema(schema_pb);
    ASSERT_EQ(tablet_schema.primary_key_encoding_type(), PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1);
}

TEST(TabletSchemaTest, test_partial_schema_create_keeps_pk_encoding_type) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(PRIMARY_KEYS);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_schema_version(1);
    schema_pb.set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);

    auto c1 = schema_pb.add_column();
    c1->set_unique_id(1);
    c1->set_name("k1");
    c1->set_type("BIGINT");
    c1->set_is_key(true);

    auto c2 = schema_pb.add_column();
    c2->set_unique_id(2);
    c2->set_name("v1");
    c2->set_type("INT");
    c2->set_is_key(false);

    auto src_schema = TabletSchema::create(schema_pb);
    ASSERT_NE(src_schema, nullptr);
    auto partial_schema = TabletSchema::create(src_schema, {0, 1});
    ASSERT_NE(partial_schema, nullptr);
    ASSERT_EQ(partial_schema->primary_key_encoding_type(), PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2);

    TabletSchemaPB partial_pb;
    partial_schema->to_schema_pb(&partial_pb);
    ASSERT_TRUE(partial_pb.has_primary_key_encoding_type());
    ASSERT_EQ(partial_pb.primary_key_encoding_type(), PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
}

} // namespace starrocks
