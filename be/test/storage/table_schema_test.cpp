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

#include "storage/tablet_schema.h"
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
}
} // namespace starrocks
