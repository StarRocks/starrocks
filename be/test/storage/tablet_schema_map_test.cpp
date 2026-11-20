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

#include <atomic>
#include <chrono>
#include <future>
#include <thread>
#include <vector>

#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"

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

// emplace() checks the column unique ids of a cached schema after releasing the shard lock. The check reads every
// column, and all tablets of a table share one shard, so under the lock it made concurrent scans of a wide table
// queue up behind each other. contains() takes the same shard lock: call it from another thread while the check runs.
TEST_F(TabletSchemaMapTest, test_emplace_checks_unique_id_outside_lock) {
    TabletSchemaMap schema_map;
    auto schema_pb = _gen_schema_pb(1, 8);
    auto other_pb = _gen_schema_pb(1, 8);
    other_pb.mutable_column(0)->set_unique_id(100);
    auto [cached, inserted] = schema_map.emplace(schema_pb);
    ASSERT_TRUE(inserted);

    std::vector<std::future<bool>> probes;
    SyncPoint::GetInstance()->SetCallBack("TabletSchemaMap::emplace:check_unique_id", [&](void* arg) {
        if (arg != &schema_map) {
            return;
        }
        probes.emplace_back(std::async(std::launch::async, [&schema_map]() { return schema_map.contains(1); }));
        // Times out if the check runs under the shard lock.
        EXPECT_EQ(std::future_status::ready, probes.back().wait_for(std::chrono::seconds(30)));
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("TabletSchemaMap::emplace:check_unique_id");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    // Passes the check: returns the cached schema.
    auto [same, same_inserted] = schema_map.emplace(schema_pb);
    EXPECT_FALSE(same_inserted);
    EXPECT_EQ(cached.get(), same.get());

    // Fails the check: returns a new schema built from |other_pb|.
    auto [other, other_inserted] = schema_map.emplace(other_pb);
    EXPECT_FALSE(other_inserted);
    EXPECT_NE(cached.get(), other.get());
    EXPECT_EQ(100, other->column(0).unique_id());

    // The TabletSchemaPtr overload, failing the check: returns its argument.
    TabletSchemaMap::TabletSchemaPtr other_schema = TabletSchema::create(other_pb);
    auto [by_ptr, by_ptr_inserted] = schema_map.emplace(other_schema);
    EXPECT_FALSE(by_ptr_inserted);
    EXPECT_EQ(other_schema.get(), by_ptr.get());

    ASSERT_EQ(3, probes.size());
    for (auto& probe : probes) {
        EXPECT_TRUE(probe.get());
    }
}

// Concurrent emplace() calls, half of which fail the unique-id check. When the check fails, emplace() may hold the
// last reference to the cached schema it looked up; dropping it runs the schema's destructor, which erases the map
// entry under the shard lock, so emplace() must drop it after unlocking or it deadlocks.
TEST_F(TabletSchemaMapTest, test_emplace_concurrent) {
    TabletSchemaMap schema_map;
    const auto schema_pb = _gen_schema_pb(1, 16);
    auto other_pb = _gen_schema_pb(1, 16);
    other_pb.mutable_column(0)->set_unique_id(1000);

    std::atomic<int> wrong{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < 8; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < 2000; ++i) {
                const auto& pb = (t + i) % 2 == 0 ? schema_pb : other_pb;
                auto schema = schema_map.emplace(pb).first;
                if (schema == nullptr || schema->column(0).unique_id() != pb.column(0).unique_id()) {
                    wrong.fetch_add(1);
                }
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    EXPECT_EQ(0, wrong.load());
    // Every schema is gone, and with it every map entry.
    EXPECT_EQ(0, schema_map.stats().num_items);
    EXPECT_EQ(0, schema_map.stats().memory_usage);
}

} // namespace starrocks
