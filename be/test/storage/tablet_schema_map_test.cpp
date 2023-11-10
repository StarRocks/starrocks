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

#include "runtime/mem_tracker.h"

namespace starrocks {

// NOLINTNEXTLINE
TEST(TabletSchemaMapTest, test_all) {
    std::unique_ptr<MemTracker> mem_tracker = std::make_unique<MemTracker>();
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(UNIQUE_KEYS);
    schema_pb.set_num_short_key_columns(1);
    schema_pb.set_id(123);
    auto c0 = schema_pb.add_column();
    c0->set_unique_id(1);
    c0->set_name("c0");
    c0->set_type("INT");
    c0->set_is_key(true);
    c0->set_is_nullable(true);

    TabletSchemaMap schema_map;

    auto stats0 = schema_map.stats();
    ASSERT_EQ(0, stats0.num_items);
    ASSERT_EQ(0, stats0.memory_usage);
    ASSERT_EQ(0, stats0.saved_memory_usage);

    ASSERT_FALSE(schema_map.contains(schema_pb.id(), schema_pb.schema_version()));
    auto [schema0, inserted0] = schema_map.emplace(schema_pb);
    ASSERT_TRUE(inserted0);
    ASSERT_TRUE(schema0 != nullptr);
    ASSERT_EQ(schema_pb.id(), schema0->id());
    ASSERT_EQ(schema_pb.keys_type(), schema0->keys_type());
    ASSERT_TRUE(schema0->shared());

    auto stats1 = schema_map.stats();
    ASSERT_EQ(1, stats1.num_items);
    ASSERT_EQ(schema0->mem_usage(), stats1.memory_usage);
    ASSERT_EQ(0, stats1.saved_memory_usage);

    auto [schema1, inserted1] = schema_map.emplace(schema_pb);
    ASSERT_FALSE(inserted1);
    ASSERT_EQ(schema0.get(), schema1.get());

    auto stats2 = schema_map.stats();
    ASSERT_EQ(1, stats2.num_items);
    ASSERT_EQ(schema0->mem_usage(), stats2.memory_usage);
    ASSERT_EQ(schema0->mem_usage(), stats2.saved_memory_usage);

    TabletSchemaPB schema_pb2 = schema_pb;
    schema_pb2.set_id(456);
    auto [schema2, inserted2] = schema_map.emplace(schema_pb2);
    ASSERT_TRUE(inserted2);
    ASSERT_NE(schema0.get(), schema2.get());
    auto stats3 = schema_map.stats();
    ASSERT_EQ(2, stats3.num_items);
    ASSERT_EQ(schema0->mem_usage() + schema2->mem_usage(), stats3.memory_usage);
    ASSERT_EQ(schema0->mem_usage(), stats3.saved_memory_usage);

    schema0.reset();
    auto stats4 = schema_map.stats();
    ASSERT_EQ(2, stats4.num_items);
    ASSERT_EQ(schema1->mem_usage() + schema2->mem_usage(), stats4.memory_usage);
    ASSERT_EQ(0, stats4.saved_memory_usage);

    schema1.reset();
    auto stats5 = schema_map.stats();
    ASSERT_EQ(1, stats5.num_items);
    ASSERT_EQ(schema2->mem_usage(), stats5.memory_usage);
    ASSERT_EQ(0, stats5.saved_memory_usage);

    ASSERT_EQ(1, schema_map.erase(schema2->id(), schema2->schema_version()));
    auto stats6 = schema_map.stats();
    ASSERT_EQ(0, stats6.num_items);
    ASSERT_EQ(0, stats6.memory_usage);
    ASSERT_EQ(0, stats6.saved_memory_usage);
}

} // namespace starrocks
