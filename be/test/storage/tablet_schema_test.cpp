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

#include <utility>
#include <vector>

#include "base/json/json_util.h"
#include "base/testutil/assert.h"
#include "column/chunk_factory.h"
#include "column/column_helper.h"
#include "column/geo_column.h"
#include "column/schema.h"
#include "storage/tablet_schema_helper.h"
#include "storage/tablet_schema_map.h"

namespace starrocks {

TEST(TabletSchemaTest, materializes_geo_tablet_columns) {
    const GeoTypeDescriptor descriptors[] = {
            {GEO_LOGICAL_TYPE_GEOGRAPHY, GEO_EDGE_ALGORITHM_SPHERICAL, "OGC:CRS84", std::nullopt},
            {GEO_LOGICAL_TYPE_GEOMETRY, GEO_EDGE_ALGORITHM_PLANAR, "EPSG:3857", 3857},
            {GEO_LOGICAL_TYPE_GEOMETRY, GEO_EDGE_ALGORITHM_PLANAR, "custom:LocalSurveyGrid-Moscow-2026", std::nullopt},
    };
    for (const auto& expected : descriptors) {
        SCOPED_TRACE(expected.crs);
        const bool geography = expected.logical_type == GEO_LOGICAL_TYPE_GEOGRAPHY;
        const auto type = geography ? TYPE_GEOGRAPHY : TYPE_GEOMETRY;
        auto tablet_schema = std::make_shared<TabletSchema>();
        TabletColumn control(STORAGE_AGGREGATE_NONE, TYPE_INT, false, 10, sizeof(int32_t));
        control.set_name("control");
        control.set_is_key(true);
        ASSERT_OK(tablet_schema->append_column(control));
        {
            TabletColumn geo(STORAGE_AGGREGATE_NONE, type, !geography, 11, 0);
            geo.set_name("geo");
            auto descriptor = expected;
            geo.set_geo_descriptor(descriptor);
            ASSERT_OK(tablet_schema->append_column(geo));
        }

        // This overload copies the supplied TabletColumns directly.
        auto copy = TabletSchema::copy(*tablet_schema, tablet_schema->columns());
        ASSERT_TRUE(copy.ok()) << copy.status();
        tablet_schema.reset();
        const auto& schema = *copy.value()->schema();
        ASSERT_EQ(2, schema.num_fields());
        EXPECT_EQ(TYPE_INT, schema.field(0)->type()->type());
        EXPECT_TRUE(schema.field(0)->is_key());
        EXPECT_EQ(11, schema.field(1)->uid());
        EXPECT_EQ(type, schema.field(1)->type()->type());
        ASSERT_NE(nullptr, schema.field(1)->geo_descriptor());
        EXPECT_EQ(expected, *schema.field(1)->geo_descriptor());

        auto chunk = ChunkFactory::new_chunk(schema, 8);
        ASSERT_EQ(2, chunk->num_columns());
        EXPECT_EQ("integral-4", chunk->get_column_by_index(0)->get_name());
        const auto* column = chunk->get_column_by_index(1).get();
        ASSERT_EQ(!geography, column->is_nullable());
        const auto* geo = dynamic_cast<const GeoColumn*>(ColumnHelper::get_data_column(column));
        ASSERT_NE(nullptr, geo);
        EXPECT_EQ(expected, geo->descriptor().type);
    }
}

TEST(TabletSchemaTest, rejects_invalid_geo_tablet_columns_before_materialization) {
    TabletSchema schema;
    TabletColumn column(STORAGE_AGGREGATE_NONE, TYPE_GEOMETRY, true, 11, 0);
    column.set_name("shape");
    EXPECT_TRUE(schema.append_column(column).is_invalid_argument());
    column.set_geo_descriptor({GEO_LOGICAL_TYPE_GEOGRAPHY, GEO_EDGE_ALGORITHM_SPHERICAL, "OGC:CRS84"});
    EXPECT_TRUE(schema.append_column(column).is_invalid_argument());
    column.set_type(TYPE_INT);
    EXPECT_TRUE(schema.append_column(column).is_invalid_argument());
    column.set_type(TYPE_GEOGRAPHY);
    column.set_is_key(true);
    EXPECT_TRUE(schema.append_column(column).is_not_supported());

    column.set_is_key(false);
    TabletColumn array(STORAGE_AGGREGATE_NONE, TYPE_ARRAY, true, 12, 0);
    array.set_name("shapes");
    array.add_sub_column(column);
    EXPECT_TRUE(schema.append_column(array).is_not_supported());
    EXPECT_EQ(0, schema.num_columns());
}

TEST(TabletSchemaTest, compares_geo_descriptor_values) {
    const auto append_geo = [](TabletSchema& schema, const GeoTypeDescriptor& descriptor) {
        TabletColumn column(STORAGE_AGGREGATE_NONE, TYPE_GEOMETRY, true, 11, 0);
        column.set_name("shape");
        column.set_geo_descriptor(descriptor);
        return schema.append_column(std::move(column));
    };

    const GeoTypeDescriptor descriptor{GEO_LOGICAL_TYPE_GEOMETRY, GEO_EDGE_ALGORITHM_PLANAR, "EPSG:3857", 3857};
    TabletSchema original;
    TabletSchema same;
    ASSERT_OK(append_geo(original, descriptor));
    ASSERT_OK(append_geo(same, descriptor));
    ASSERT_NE(original.column(0).geo_descriptor(), same.column(0).geo_descriptor());
    EXPECT_EQ(original, same);

    auto no_srid = descriptor;
    no_srid.srid.reset();
    TabletSchema without_srid;
    ASSERT_OK(append_geo(without_srid, no_srid));
    EXPECT_NE(original, without_srid);

    auto other_crs = no_srid;
    other_crs.crs = "custom:LocalSurveyGrid";
    TabletSchema different_crs;
    ASSERT_OK(append_geo(different_crs, other_crs));
    EXPECT_NE(without_srid, different_crs);
}

TEST(TabletSchemaTest, rejects_geo_at_inherited_sort_key_uid) {
    TabletSchemaPB pb;
    pb.set_keys_type(DUP_KEYS);
    auto* key = pb.add_column();
    key->set_unique_id(11);
    key->set_name("sort_key");
    key->set_type("INT");
    key->set_is_key(true);
    pb.add_sort_key_unique_ids(key->unique_id());
    auto source = TabletSchema::create(pb);
    ASSERT_TRUE(source->column(0).is_sort_key());
    TabletSchemaPB before;
    source->to_schema_pb(&before);

    TabletColumn geo = source->column(0);
    geo.set_type(TYPE_GEOMETRY);
    geo.set_is_key(false);
    geo.set_is_sort_key(false);
    geo.set_geo_descriptor({GEO_LOGICAL_TYPE_GEOMETRY, GEO_EDGE_ALGORITHM_PLANAR, "EPSG:3857", 3857});
    const auto result = TabletSchema::copy(*source, std::vector<TabletColumn>{std::move(geo)});
    EXPECT_TRUE(result.status().is_not_supported()) << result.status();
    TabletSchemaPB after;
    source->to_schema_pb(&after);
    EXPECT_EQ(before.SerializeAsString(), after.SerializeAsString());
    EXPECT_TRUE(source->column(0).is_sort_key());
}

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

TEST(TabletSchemaTest, test_has_dropped_index) {
    // Verify TabletSchema::has_dropped_index correctly reflects entries
    // carried by TabletSchemaPB.dropped_table_indices (the tombstone list
    // populated by MetaFileBuilder::apply_drop_index). Empty input must
    // return false.
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_short_key_columns(1);

    auto* c1 = schema_pb.add_column();
    c1->set_unique_id(101);
    c1->set_name("k");
    c1->set_type("VARCHAR");
    c1->set_is_key(true);

    auto* c2 = schema_pb.add_column();
    c2->set_unique_id(202);
    c2->set_name("v");
    c2->set_type("INT");
    c2->set_is_key(false);

    // Two tombstones on the same column, different index types.
    auto* drop1 = schema_pb.add_dropped_table_indices();
    drop1->set_index_id(9001);
    drop1->set_index_type(NGRAMBF);
    drop1->add_col_unique_id(101);

    auto* drop2 = schema_pb.add_dropped_table_indices();
    drop2->set_index_id(9002);
    drop2->set_index_type(BITMAP);
    drop2->add_col_unique_id(202);

    TabletSchema schema(schema_pb);

    EXPECT_TRUE(schema.has_dropped_index(101, NGRAMBF));
    EXPECT_TRUE(schema.has_dropped_index(202, BITMAP));
    // Column has no such tombstoned index.
    EXPECT_FALSE(schema.has_dropped_index(101, BITMAP));
    EXPECT_FALSE(schema.has_dropped_index(202, NGRAMBF));
    // Unknown column uid.
    EXPECT_FALSE(schema.has_dropped_index(999, NGRAMBF));
}

TEST(TabletSchemaTest, test_has_dropped_index_empty) {
    // No dropped_table_indices in the proto — has_dropped_index must return
    // false for every probe without touching maps.
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_short_key_columns(1);
    auto* c = schema_pb.add_column();
    c->set_unique_id(1);
    c->set_name("c");
    c->set_type("INT");
    c->set_is_key(true);

    TabletSchema schema(schema_pb);
    EXPECT_FALSE(schema.has_dropped_index(1, NGRAMBF));
    EXPECT_FALSE(schema.has_dropped_index(1, BITMAP));
    EXPECT_FALSE(schema.has_dropped_index(1, GIN));
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

// Test that partial schema has consistent num_short_key_columns when sort key columns
// are not in the referenced columns (e.g. column-mode partial update on table with ORDER BY).
TEST(TabletSchemaTest, test_partial_schema_short_key_consistency_with_separate_sort_key) {
    // Build a PK table schema: pk=k0, ORDER BY(k3, k2), num_short_key_columns=2
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(PRIMARY_KEYS);
    schema_pb.set_num_short_key_columns(2);

    auto c0 = schema_pb.add_column();
    c0->set_unique_id(0);
    c0->set_name("k0");
    c0->set_type("INT");
    c0->set_is_key(true);
    c0->set_is_nullable(false);

    auto c1 = schema_pb.add_column();
    c1->set_unique_id(1);
    c1->set_name("k1");
    c1->set_type("INT");
    c1->set_is_key(false);

    auto c2 = schema_pb.add_column();
    c2->set_unique_id(2);
    c2->set_name("k2");
    c2->set_type("INT");
    c2->set_is_key(false);

    auto c3 = schema_pb.add_column();
    c3->set_unique_id(3);
    c3->set_name("k3");
    c3->set_type("INT");
    c3->set_is_key(false);

    auto c4 = schema_pb.add_column();
    c4->set_unique_id(4);
    c4->set_name("k4");
    c4->set_type("INT");
    c4->set_is_key(false);

    // Set sort key to (k3, k2) — separate from primary key (k0)
    schema_pb.add_sort_key_idxes(3);
    schema_pb.add_sort_key_idxes(2);

    auto src_schema = TabletSchema::create(schema_pb);
    ASSERT_NE(src_schema, nullptr);
    ASSERT_EQ(src_schema->num_short_key_columns(), 2);
    ASSERT_EQ(src_schema->sort_key_idxes().size(), 2);

    // Create partial schema with only [k0, k4] — no sort key columns included
    auto partial_schema = TabletSchema::create(src_schema, {0, 4});
    ASSERT_NE(partial_schema, nullptr);
    // num_short_key_columns must not exceed sort_key_idxes.size()
    ASSERT_LE(partial_schema->num_short_key_columns(), partial_schema->sort_key_idxes().size());

    // Create partial schema with [k0, k2] — one of two sort key columns included
    auto partial_schema2 = TabletSchema::create(src_schema, {0, 2});
    ASSERT_NE(partial_schema2, nullptr);
    ASSERT_LE(partial_schema2->num_short_key_columns(), partial_schema2->sort_key_idxes().size());

    // Create partial schema with all sort keys [k0, k2, k3] — both sort key columns included
    auto partial_schema3 = TabletSchema::create(src_schema, {0, 2, 3});
    ASSERT_NE(partial_schema3, nullptr);
    ASSERT_LE(partial_schema3->num_short_key_columns(), partial_schema3->sort_key_idxes().size());
}

} // namespace starrocks
