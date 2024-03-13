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

#include "storage/tablet_index.h"

#include <gtest/gtest.h>

#include "storage/tablet_schema.h"
#include "util/json_util.h"

namespace starrocks {
TEST(TabletIndexTest, test_init_from_thrift) {
    TOlapTableIndex olap_table_index;
    std::vector<std::string> columns;
    columns.emplace_back("f1");
    std::map<std::string, std::string> common_map;
    common_map.emplace("imp_type", "clucene");

    olap_table_index.__set_index_id(0);
    olap_table_index.__set_index_name("test_index");
    olap_table_index.__set_columns(columns);
    olap_table_index.__set_common_properties(common_map);
    olap_table_index.__set_index_type(TIndexType::GIN);

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

    TabletSchema tablet_schema(schema_pb);

    TabletIndex index;
    index.init_from_thrift(olap_table_index, tablet_schema);

    ASSERT_EQ(index.index_type(), GIN);
    ASSERT_EQ(index.index_id(), 0);
    ASSERT_EQ(index.index_name(), "test_index");
    ASSERT_EQ(index.common_properties(), common_map);
    std::map<std::string, std::map<std::string, std::string>> result_map;
    ASSERT_TRUE(from_json(index.properties_to_json(), &result_map).ok());
    ASSERT_EQ(result_map.at("common_properties"), index.common_properties());
    ASSERT_TRUE(index.index_properties().empty());
    ASSERT_TRUE(index.search_properties().empty());
    ASSERT_TRUE(index.extra_properties().empty());
    ASSERT_TRUE(index.contains_column(1));
    ASSERT_FALSE(index.contains_column(0));
}

TEST(TabletIndexTest, test_init_from_pb) {
    TabletSchemaPB schema_pb;
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
    TabletIndex index;
    ASSERT_TRUE(index.init_from_pb(index_pb).ok());

    ASSERT_EQ(index.index_type(), GIN);
    ASSERT_EQ(index.index_id(), 0);
    ASSERT_EQ(index.index_name(), "test_index");
    ASSERT_EQ(index.common_properties(), common_map);
    std::map<std::string, std::map<std::string, std::string>> result_map;
    ASSERT_TRUE(from_json(index.properties_to_json(), &result_map).ok());
    ASSERT_EQ(result_map.at("common_properties"), index.common_properties());
    ASSERT_TRUE(index.index_properties().empty());
    ASSERT_TRUE(index.search_properties().empty());
    ASSERT_TRUE(index.extra_properties().empty());
    ASSERT_TRUE(index.contains_column(1));
    ASSERT_FALSE(index.contains_column(0));

    index_pb.set_index_properties("{{");
    TabletIndex index2;
    ASSERT_FALSE(index2.init_from_pb(index_pb).ok());

    TabletIndexPB index_pb_out;
    index.to_schema_pb(&index_pb_out);

    ASSERT_EQ(index_pb_out.index_type(), GIN);
    ASSERT_EQ(index_pb_out.index_id(), 0);
    ASSERT_EQ(index_pb_out.index_name(), "test_index");
}

} // namespace starrocks