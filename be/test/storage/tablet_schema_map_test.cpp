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

#include "storage/tablet_schema_map.h"

#include <gtest/gtest.h>

namespace starrocks {

class TabletSchemaMapTest : public testing::Test {
public:
    TabletSchemaMapTest() = default;

protected:
    TabletSchemaPB _gen_schema_pb(TabletSchemaMap::SchemaId schema_id, size_t num_cols);
    TabletSchemaSPtr _gen_schema(TabletSchemaMap* schema_map, TabletSchemaMap::SchemaId schema_id, size_t num_cols);
};

TabletSchemaSPtr TabletSchemaMapTest::_gen_schema(TabletSchemaMap* schema_map, TabletSchemaMap::SchemaId schema_id,
                                                  size_t num_cols) {
    auto schema_pb = _gen_schema_pb(schema_id, num_cols);
    return TabletSchema::create(schema_pb, schema_map);
}

TabletSchemaPB TabletSchemaMapTest::_gen_schema_pb(TabletSchemaMap::SchemaId schema_id, size_t num_cols) {
    TabletSchemaPB schema_pb;

    schema_pb.set_keys_type(UNIQUE_KEYS);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_id(schema_id);

    for (size_t i = 0; i < num_cols; i++) {
        auto c0 = schema_pb.add_column();
        c0->set_unique_id(i);
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(true);
        c0->set_index_length(4);
    }

    return schema_pb;
}

// NOLINTNEXTLINE
TEST_F(TabletSchemaMapTest, test_emplace_schema_pb_change_unique_id) {
    TabletSchemaMap schema_map;
    auto schema_pb_1 = _gen_schema_pb(1, 1);
    auto schema_pb_2 = _gen_schema_pb(1, 2);
    int64_t mem_usage_1;

    // add col1
    auto [dst_schema_1, inserted_1] = schema_map.emplace(schema_pb_1);
    const auto& stats1 = schema_map.stats();
    mem_usage_1 = dst_schema_1->mem_usage();

    ASSERT_TRUE(inserted_1);
    ASSERT_EQ(schema_pb_1.id(), dst_schema_1->id());
    ASSERT_EQ(schema_pb_1.column_size(), dst_schema_1->num_columns());
    ASSERT_EQ(1, stats1.num_items);
    ASSERT_EQ(mem_usage_1, stats1.memory_usage);

    // replace col1
    auto [dst_schema_2, inserted_2] = schema_map.emplace(schema_pb_2);
    const auto& stats2 = schema_map.stats();

    ASSERT_FALSE(inserted_2);
    ASSERT_EQ(schema_pb_2.id(), dst_schema_2->id());
    ASSERT_EQ(schema_pb_2.column_size(), dst_schema_2->num_columns());
    ASSERT_EQ(1, stats2.num_items);
    ASSERT_EQ(mem_usage_1, stats2.memory_usage);
}

// NOLINTNEXTLINE
TEST_F(TabletSchemaMapTest, test_emplace_schema_change_unique_id) {
    TabletSchemaMap schema_map;
    auto src_schema_1 = _gen_schema(&schema_map, 1, 1);
    auto src_schema_2 = _gen_schema(&schema_map, 1, 2);
    int64_t mem_usage_1 = src_schema_1->mem_usage();

    // add col1
    auto [dst_schema_1, inserted_1] = schema_map.emplace(src_schema_1);
    const auto& stats1 = schema_map.stats();

    ASSERT_TRUE(inserted_1);
    ASSERT_EQ(src_schema_1->id(), dst_schema_1->id());
    ASSERT_EQ(src_schema_1->num_columns(), dst_schema_1->num_columns());
    ASSERT_EQ(1, stats1.num_items);
    ASSERT_EQ(mem_usage_1, stats1.memory_usage);

    // modify col1
    auto [dst_schema_2, inserted2] = schema_map.emplace(src_schema_2);
    const auto& stats2 = schema_map.stats();

    ASSERT_FALSE(inserted2);
    ASSERT_EQ(src_schema_2->id(), dst_schema_2->id());
    ASSERT_EQ(src_schema_2->num_columns(), dst_schema_2->num_columns());
    ASSERT_EQ(1, stats2.num_items);
    ASSERT_EQ(mem_usage_1, stats2.memory_usage);
}

// NOLINTNEXTLINE
TEST_F(TabletSchemaMapTest, test_emplace_schema_pb) {
    TabletSchemaMap schema_map;
    auto src_schema_1 = _gen_schema_pb(1, 1);
    auto src_schema_2 = _gen_schema_pb(2, 1);
    int64_t mem_usage_1;
    int64_t mem_usage_2;

    const auto& stats0 = schema_map.stats();
    ASSERT_EQ(0, stats0.num_items);
    ASSERT_FALSE(schema_map.contains(1));

    // add col1
    auto [dst_schema_1, inserted_1] = schema_map.emplace(src_schema_1);
    const auto& stats1 = schema_map.stats();
    mem_usage_1 = dst_schema_1->mem_usage();

    ASSERT_TRUE(inserted_1);
    ASSERT_EQ(src_schema_1.id(), dst_schema_1->id());
    ASSERT_EQ(src_schema_1.column_size(), dst_schema_1->num_columns());
    ASSERT_EQ(1, stats1.num_items);
    ASSERT_EQ(mem_usage_1, stats1.memory_usage);

    // add col1
    auto [dst_schema_1_dup, inserted_1_dup] = schema_map.emplace(src_schema_1);
    const auto& stats1_dup = schema_map.stats();

    ASSERT_FALSE(inserted_1_dup);
    ASSERT_EQ(dst_schema_1.get(), dst_schema_1_dup.get());
    ASSERT_EQ(1, stats1_dup.num_items);
    ASSERT_EQ(mem_usage_1, stats1_dup.memory_usage);

    // add col 2
    auto [dst_schema_2, inserted_2] = schema_map.emplace(src_schema_2);
    const auto& stats2 = schema_map.stats();
    mem_usage_2 = dst_schema_2->mem_usage();

    ASSERT_TRUE(inserted_2);
    ASSERT_EQ(dst_schema_2->id(), 2);
    ASSERT_EQ(2, stats2.num_items);
    ASSERT_EQ(mem_usage_1 + mem_usage_2, stats2.memory_usage);

    // reset col 2
    dst_schema_2.reset();
    const auto& stats4 = schema_map.stats();
    ASSERT_EQ(1, stats4.num_items);
    ASSERT_EQ(mem_usage_1, stats4.memory_usage);

    // reset col 1
    dst_schema_1.reset();
    const auto& stats5 = schema_map.stats();
    ASSERT_EQ(1, stats5.num_items);
    ASSERT_EQ(mem_usage_1, stats5.memory_usage);

    // erase col 1
    dst_schema_1_dup.reset();
    const auto& stats6 = schema_map.stats();
    ASSERT_EQ(0, stats6.num_items);
    ASSERT_EQ(0, stats6.memory_usage);
}

// NOLINTNEXTLINE
TEST_F(TabletSchemaMapTest, test_emplace_schema) {
    TabletSchemaMap schema_map;
    auto src_schema_1 = _gen_schema(&schema_map, 1, 1);
    auto src_schema_2 = _gen_schema(&schema_map, 2, 1);
    int64_t mem_usage_1 = src_schema_1->mem_usage();
    int64_t mem_usage_2 = src_schema_2->mem_usage();

    const auto& stats0 = schema_map.stats();
    ASSERT_EQ(0, stats0.num_items);
    ASSERT_FALSE(schema_map.contains(1));

    // add col1
    auto [dest_schema_1, inserted_1] = schema_map.emplace(src_schema_1);
    const auto& stats1 = schema_map.stats();

    ASSERT_TRUE(inserted_1);
    ASSERT_EQ(src_schema_1->id(), dest_schema_1->id());
    ASSERT_EQ(src_schema_1.get(), dest_schema_1.get());
    ASSERT_EQ(1, stats1.num_items);
    ASSERT_EQ(mem_usage_1, stats1.memory_usage);

    // add col1
    auto [dst_schema_1_dup, inserted_1_dup] = schema_map.emplace(src_schema_1);
    const auto& stats2 = schema_map.stats();

    ASSERT_FALSE(inserted_1_dup);
    ASSERT_EQ(src_schema_1.get(), dst_schema_1_dup.get());
    ASSERT_EQ(1, stats2.num_items);
    ASSERT_EQ(mem_usage_1, stats2.memory_usage);

    // add col 2
    auto [dest_schema_2, inserted3] = schema_map.emplace(src_schema_2);
    const auto& stats3 = schema_map.stats();

    ASSERT_TRUE(inserted3);
    ASSERT_EQ(dest_schema_2->id(), 2);
    ASSERT_EQ(2, stats3.num_items);
    ASSERT_EQ(mem_usage_1 + mem_usage_2, stats3.memory_usage);

    // reset col 2
    dest_schema_2.reset();
    const auto& stats4 = schema_map.stats();
    ASSERT_EQ(2, stats4.num_items);
    ASSERT_EQ(mem_usage_1 + mem_usage_2, stats4.memory_usage);

    src_schema_2.reset();
    const auto& stats5 = schema_map.stats();
    ASSERT_EQ(1, stats5.num_items);
    ASSERT_EQ(mem_usage_1, stats5.memory_usage);

    // reset col 1
    dest_schema_1.reset();
    const auto& stats6 = schema_map.stats();
    ASSERT_EQ(1, stats6.num_items);
    ASSERT_EQ(mem_usage_1, stats5.memory_usage);

    dst_schema_1_dup.reset();
    const auto& stats7 = schema_map.stats();
    ASSERT_EQ(1, stats7.num_items);
    ASSERT_EQ(mem_usage_1, stats7.memory_usage);

    src_schema_1.reset();
    const auto& stats8 = schema_map.stats();
    ASSERT_EQ(0, stats8.num_items);
    ASSERT_EQ(0, stats8.memory_usage);
}

TEST_F(TabletSchemaMapTest, test_get_erase) {
    TabletSchemaMap schema_map;
    auto src_schema_1 = _gen_schema(&schema_map, 1, 1);
    auto src_schema_2 = _gen_schema(&schema_map, 2, 1);

    auto get_schema_1 = schema_map.get(1);
    ASSERT_TRUE(get_schema_1 == nullptr);

    schema_map.emplace(src_schema_1);
    get_schema_1 = schema_map.get(1);
    ASSERT_TRUE(get_schema_1 != nullptr);
    ASSERT_TRUE(get_schema_1->id() == 1);

    auto get_schema_2 = schema_map.get(2);
    ASSERT_TRUE(get_schema_2 == nullptr);

    schema_map.erase(1);
    auto get_schema_3 = schema_map.get(1);
    ASSERT_TRUE(get_schema_3 == nullptr);
}

} // namespace starrocks
