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

} // namespace starrocks
