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

#include <cstdlib>
#include <memory>
#include <utility>

#include "base/testutil/assert.h"
#include "common/object_pool.h"
#include "exec/pipeline/scan/morsel_queue_factory.h"
#include "exec/scan_node.h"
#include "exec_primitive/pipeline/scan/dynamic_morsel_queue_builder.h"
#include "exec_primitive/pipeline/scan/fixed_morsel_queue_builder.h"
#include "runtime/descriptors.h"
#include "storage/query/olap_dynamic_morsel_queue_builder.h"
#include "storage/query/olap_fixed_morsel_queue_builder.h"
#include "storage/query/olap_morsel_queue.h"
#include "storage/query/split_morsel_queue_builder.h"

namespace starrocks::pipeline {

class MorselQueueBuilderTest : public ::testing::Test {
protected:
    static Morsels make_morsels(int num_morsels) {
        Morsels morsels;
        morsels.reserve(num_morsels);
        for (int i = 0; i < num_morsels; ++i) {
            morsels.emplace_back(std::make_unique<ScanMorsel>(1, TScanRange()));
        }
        return morsels;
    }
};

TEST_F(MorselQueueBuilderTest, primitive_fixed_builder_builds_fixed_queue) {
    auto builder = make_fixed_morsel_queue_builder(make_morsels(3));
    ASSERT_TRUE(builder->can_uniform_distribute());
    ASSERT_EQ(3, builder->num_original_morsels());
    ASSERT_EQ(3, builder->max_degree_of_parallelism());

    builder->set_has_more_scan_ranges(true);
    builder->set_has_more_from_split(true);
    ASSERT_TRUE(builder->has_more_scan_ranges());
    ASSERT_TRUE(builder->has_more_from_split());

    auto queue_or = builder->build();
    ASSERT_TRUE(queue_or.ok()) << queue_or.status().to_string();
    auto queue = std::move(queue_or).value();
    ASSERT_EQ(MorselQueue::Type::FIXED, queue->type());
    ASSERT_EQ(3, queue->num_original_morsels());
    ASSERT_EQ(3, queue->max_degree_of_parallelism());
    ASSERT_TRUE(queue->has_more_scan_ranges());
    ASSERT_TRUE(queue->has_more_from_split());
}

TEST_F(MorselQueueBuilderTest, primitive_dynamic_builder_build_preserves_max_dop) {
    auto builder = make_dynamic_morsel_queue_builder(make_morsels(2), true, 8);
    ASSERT_TRUE(builder->can_uniform_distribute());
    ASSERT_EQ(2, builder->num_original_morsels());
    ASSERT_EQ(8, builder->max_degree_of_parallelism());

    builder->set_has_more_from_split(true);
    auto queue_or = builder->build();
    ASSERT_TRUE(queue_or.ok()) << queue_or.status().to_string();
    auto queue = std::move(queue_or).value();
    ASSERT_EQ(MorselQueue::Type::DYNAMIC, queue->type());
    ASSERT_EQ(2, queue->num_original_morsels());
    ASSERT_EQ(8, queue->max_degree_of_parallelism());
    ASSERT_TRUE(queue->has_more_scan_ranges());
    ASSERT_TRUE(queue->has_more_from_split());
}

TEST_F(MorselQueueBuilderTest, primitive_dynamic_builder_build_from_morsels_does_not_preserve_aggregate_max_dop) {
    auto builder = make_dynamic_morsel_queue_builder(make_morsels(2), true, 8);
    builder->set_has_more_from_split(true);

    auto queue_or = builder->build_from_morsels(make_morsels(1));
    ASSERT_TRUE(queue_or.ok()) << queue_or.status().to_string();
    auto queue = std::move(queue_or).value();
    ASSERT_EQ(MorselQueue::Type::DYNAMIC, queue->type());
    ASSERT_EQ(1, queue->num_original_morsels());
    ASSERT_EQ(1, queue->max_degree_of_parallelism());
    ASSERT_TRUE(queue->has_more_scan_ranges());
    ASSERT_TRUE(queue->has_more_from_split());
}

TEST_F(MorselQueueBuilderTest, primitive_builder_take_morsels_moves_owned_morsels) {
    auto builder = make_fixed_morsel_queue_builder(make_morsels(2));
    auto morsels = builder->take_morsels();
    ASSERT_EQ(2, morsels.size());
    ASSERT_EQ(0, builder->num_original_morsels());
}

TEST_F(MorselQueueBuilderTest, fixed_builder_builds_fixed_queue) {
    auto builder = make_olap_fixed_morsel_queue_builder(make_morsels(3));
    ASSERT_TRUE(builder->can_uniform_distribute());
    ASSERT_EQ(3, builder->num_original_morsels());
    ASSERT_EQ(3, builder->max_degree_of_parallelism());

    builder->set_has_more_scan_ranges(true);
    builder->set_has_more_from_split(true);
    ASSERT_TRUE(builder->has_more_scan_ranges());
    ASSERT_TRUE(builder->has_more_from_split());

    auto queue_or = builder->build();
    ASSERT_TRUE(queue_or.ok()) << queue_or.status().to_string();
    auto queue = std::move(queue_or).value();
    ASSERT_EQ(MorselQueue::Type::FIXED, queue->type());
    ASSERT_EQ(3, queue->num_original_morsels());
    ASSERT_EQ(3, queue->max_degree_of_parallelism());
    ASSERT_TRUE(queue->has_more_scan_ranges());
    ASSERT_TRUE(queue->has_more_from_split());
}

TEST_F(MorselQueueBuilderTest, dynamic_builder_build_preserves_max_dop) {
    auto builder = make_olap_dynamic_morsel_queue_builder(make_morsels(2), true, 8);
    ASSERT_TRUE(builder->can_uniform_distribute());
    ASSERT_EQ(2, builder->num_original_morsels());
    ASSERT_EQ(8, builder->max_degree_of_parallelism());

    builder->set_has_more_from_split(true);
    auto queue_or = builder->build();
    ASSERT_TRUE(queue_or.ok()) << queue_or.status().to_string();
    auto queue = std::move(queue_or).value();
    ASSERT_EQ(MorselQueue::Type::DYNAMIC, queue->type());
    ASSERT_EQ(2, queue->num_original_morsels());
    ASSERT_EQ(8, queue->max_degree_of_parallelism());
    ASSERT_TRUE(queue->has_more_scan_ranges());
    ASSERT_TRUE(queue->has_more_from_split());
}

TEST_F(MorselQueueBuilderTest, dynamic_builder_build_from_morsels_does_not_preserve_aggregate_max_dop) {
    auto builder = make_olap_dynamic_morsel_queue_builder(make_morsels(2), true, 8);
    builder->set_has_more_from_split(true);

    auto queue_or = builder->build_from_morsels(make_morsels(1));
    ASSERT_TRUE(queue_or.ok()) << queue_or.status().to_string();
    auto queue = std::move(queue_or).value();
    ASSERT_EQ(MorselQueue::Type::DYNAMIC, queue->type());
    ASSERT_EQ(1, queue->num_original_morsels());
    ASSERT_EQ(1, queue->max_degree_of_parallelism());
    ASSERT_TRUE(queue->has_more_scan_ranges());
    ASSERT_TRUE(queue->has_more_from_split());
}

TEST_F(MorselQueueBuilderTest, olap_dynamic_builder_from_preserves_generic_builder_state) {
    auto generic_builder = make_dynamic_morsel_queue_builder(make_morsels(2), true, 8);
    generic_builder->set_has_more_from_split(true);

    auto olap_builder = make_olap_dynamic_morsel_queue_builder_from(std::move(generic_builder));
    ASSERT_TRUE(olap_builder->can_uniform_distribute());
    ASSERT_EQ(2, olap_builder->num_original_morsels());
    ASSERT_EQ(8, olap_builder->max_degree_of_parallelism());
    ASSERT_TRUE(olap_builder->has_more_scan_ranges());
    ASSERT_TRUE(olap_builder->has_more_from_split());

    auto queue_or = olap_builder->build();
    ASSERT_TRUE(queue_or.ok()) << queue_or.status().to_string();
    auto queue = std::move(queue_or).value();
    ASSERT_EQ(MorselQueue::Type::DYNAMIC, queue->type());
    ASSERT_EQ(2, queue->num_original_morsels());
    ASSERT_EQ(8, queue->max_degree_of_parallelism());
    ASSERT_TRUE(queue->has_more_scan_ranges());
    ASSERT_TRUE(queue->has_more_from_split());
    ASSERT_NE(nullptr, dynamic_cast<OlapMorselQueue*>(queue.get()));
}

TEST_F(MorselQueueBuilderTest, split_builders_build_split_queues) {
    auto physical_builder = make_physical_split_morsel_queue_builder(make_morsels(1), 4, 1024);
    ASSERT_FALSE(physical_builder->can_uniform_distribute());
    ASSERT_EQ(1, physical_builder->num_original_morsels());
    ASSERT_EQ(4, physical_builder->max_degree_of_parallelism());

    physical_builder->set_has_more_scan_ranges(true);
    physical_builder->set_has_more_from_split(true);
    auto physical_queue_or = physical_builder->build();
    ASSERT_TRUE(physical_queue_or.ok()) << physical_queue_or.status().to_string();
    auto physical_queue = std::move(physical_queue_or).value();
    ASSERT_EQ(MorselQueue::Type::PHYSICAL_SPLIT, physical_queue->type());
    ASSERT_EQ(1, physical_queue->num_original_morsels());
    ASSERT_EQ(4, physical_queue->max_degree_of_parallelism());
    ASSERT_TRUE(physical_queue->has_more_scan_ranges());
    ASSERT_TRUE(physical_queue->has_more_from_split());

    auto logical_builder = make_logical_split_morsel_queue_builder(make_morsels(1), 5, 2048);
    ASSERT_FALSE(logical_builder->can_uniform_distribute());
    ASSERT_EQ(1, logical_builder->num_original_morsels());
    ASSERT_EQ(5, logical_builder->max_degree_of_parallelism());

    auto logical_queue_or = logical_builder->build();
    ASSERT_TRUE(logical_queue_or.ok()) << logical_queue_or.status().to_string();
    auto logical_queue = std::move(logical_queue_or).value();
    ASSERT_EQ(MorselQueue::Type::LOGICAL_SPLIT, logical_queue->type());
    ASSERT_EQ(1, logical_queue->num_original_morsels());
    ASSERT_EQ(5, logical_queue->max_degree_of_parallelism());
}

TEST_F(MorselQueueBuilderTest, split_builders_reject_build_from_morsels) {
    auto builder = make_physical_split_morsel_queue_builder(make_morsels(1), 4, 1024);
    auto queue_or = builder->build_from_morsels(make_morsels(1));
    ASSERT_FALSE(queue_or.ok());
    ASSERT_TRUE(queue_or.status().is_not_supported());
}

TEST_F(MorselQueueBuilderTest, take_morsels_moves_owned_morsels) {
    auto builder = make_olap_fixed_morsel_queue_builder(make_morsels(2));
    auto morsels = builder->take_morsels();
    ASSERT_EQ(2, morsels.size());
    ASSERT_EQ(0, builder->num_original_morsels());
}

// add_split_source_morsels (used by the prepared-split path to deliver refined morsels
// incrementally) must bump the outstanding-morsel counter and set has_more_from_split(true) on
// every driver queue; mark_split_source_morsel_finished drains the counter and clears the flag
// exactly when the last outstanding morsel is consumed. Both are no-ops unless the factory was
// built with enable_random_append_split_morsel.
TEST_F(MorselQueueBuilderTest, individual_factory_split_source_morsel_lifecycle) {
    auto make_queue = [](int n) {
        return std::move(make_fixed_morsel_queue_builder(make_morsels(n))->build()).value();
    };

    {
        std::map<int, MorselQueuePtr> queues;
        queues.emplace(0, make_queue(1));
        queues.emplace(1, make_queue(1));
        IndividualMorselQueueFactory factory(std::move(queues), /*could_local_shuffle=*/false,
                                             /*enable_random_append_split_morsel=*/true);
        const int64_t initial = factory._remaining_split_source_morsels.load();

        factory.add_split_source_morsels(3);
        EXPECT_EQ(initial + 3, factory._remaining_split_source_morsels.load());
        for (auto& q : factory._queue_per_driver_seq) {
            EXPECT_TRUE(q->has_more_from_split());
        }

        // The flag stays true until the very last outstanding morsel is marked finished.
        const int64_t total = initial + 3;
        for (int64_t i = 0; i < total - 1; ++i) {
            ASSERT_TRUE(factory.mark_split_source_morsel_finished().ok());
            for (auto& q : factory._queue_per_driver_seq) {
                EXPECT_TRUE(q->has_more_from_split());
            }
        }
        ASSERT_TRUE(factory.mark_split_source_morsel_finished().ok()); // counter hits zero
        EXPECT_EQ(0, factory._remaining_split_source_morsels.load());
        for (auto& q : factory._queue_per_driver_seq) {
            EXPECT_FALSE(q->has_more_from_split());
        }
    }

    // Feature disabled -> add is a no-op (counter stays 0, no flag flip).
    {
        std::map<int, MorselQueuePtr> queues;
        queues.emplace(0, make_queue(1));
        IndividualMorselQueueFactory factory(std::move(queues), /*could_local_shuffle=*/false,
                                             /*enable_random_append_split_morsel=*/false);
        factory.add_split_source_morsels(3);
        EXPECT_EQ(0, factory._remaining_split_source_morsels.load());
        for (auto& q : factory._queue_per_driver_seq) {
            EXPECT_FALSE(q->has_more_from_split());
        }
    }

    // count <= 0 -> no-op even when enabled.
    {
        std::map<int, MorselQueuePtr> queues;
        queues.emplace(0, make_queue(2));
        IndividualMorselQueueFactory factory(std::move(queues), /*could_local_shuffle=*/false,
                                             /*enable_random_append_split_morsel=*/true);
        const int64_t initial = factory._remaining_split_source_morsels.load();
        factory.add_split_source_morsels(0);
        factory.add_split_source_morsels(-5);
        EXPECT_EQ(initial, factory._remaining_split_source_morsels.load());
    }
}

namespace {
class GroupedQueueScanNode final : public ScanNode {
public:
    GroupedQueueScanNode(ObjectPool* pool, const TPlanNode& node, const DescriptorTbl& desc)
            : ScanNode(pool, node, desc) {}
    Status set_scan_ranges(const std::vector<TScanRangeParams>&) override { return Status::OK(); }
    StatusOr<MorselQueueBuilderPtr> convert_scan_range_to_morsel_queue_builder(
            const std::vector<TScanRangeParams>& ranges, int node_id, int32_t dop, bool parallel,
            TTabletInternalParallelMode::type mode, size_t total) override {
        Morsels morsels;
        bool has_more = false;
        ScanMorsel::build_scan_morsels(node_id, ranges, true, &morsels, &has_more);
        if (parallel) return make_dynamic_morsel_queue_builder(std::move(morsels), false, dop);
        return make_olap_fixed_morsel_queue_builder(std::move(morsels));
    }
};

TScanRangeParams grouped_queue_task(int group, int tablet = 77) {
    TScanRangeParams result;
    TInternalScanRange internal;
    internal.__set_tablet_id(tablet);
    internal.__set_version("42");
    internal.__set_bucket_sequence(group);
    result.scan_range.__set_internal_scan_range(internal);
    TGroupedRangeTask task;
    task.__set_group_id(group);
    task.__set_key_range(TTabletRange{});
    task.__set_sort_key_unique_ids({100, 101, 102});
    result.scan_range.__set_grouped_range_task(task);
    return result;
}
} // namespace

TEST_F(MorselQueueBuilderTest, grouped_queues_are_independent_fixed_and_padding_terminates) {
    ObjectPool pool;
    DescriptorTbl desc;
    TPlanNode plan;
    plan.__set_node_type(TPlanNodeType::LAKE_GROUPED_SCAN_NODE);
    GroupedQueueScanNode node(&pool, plan, desc);
    TScanRangeParams padding;
    padding.__set_empty(true);
    std::map<int32_t, std::vector<TScanRangeParams>> ranges{{0, {grouped_queue_task(0), grouped_queue_task(0, 78)}},
                                                            {1, {grouped_queue_task(1)}},
                                                            {2, {padding}},
                                                            {3, {}}};
    ASSIGN_OR_ABORT(auto factory,
                    node.convert_scan_range_to_morsel_queue_factory({}, ranges, 1, 4, true, false,
                                                                    TTabletInternalParallelMode::FORCE_SPLIT, true));
    ASSERT_FALSE(factory->is_shared());
    EXPECT_FALSE(factory->could_local_shuffle());
    ASSERT_EQ(4, factory->size());
    EXPECT_NE(factory->create(0), factory->create(1));
    for (int i = 0; i < 4; ++i) {
        auto* queue = factory->create(i);
        EXPECT_EQ(MorselQueue::FIXED, queue->type());
        EXPECT_FALSE(queue->has_more());
        int count = 0;
        while (!queue->empty()) {
            ASSIGN_OR_ABORT(auto morsel, queue->try_get());
            ASSERT_NE(nullptr, morsel);
            EXPECT_EQ(i, morsel->get_scan_range()->grouped_range_task.group_id);
            ++count;
        }
        EXPECT_EQ(i == 0 ? 2 : i == 1 ? 1 : 0, count);
    }
}

TEST_F(MorselQueueBuilderTest, GroupedChildrenAppendOnlyToOwningQueue) {
    ObjectPool pool;
    DescriptorTbl desc;
    TPlanNode plan;
    plan.__set_node_type(TPlanNodeType::LAKE_GROUPED_SCAN_NODE);
    GroupedQueueScanNode node(&pool, plan, desc);
    std::map<int32_t, std::vector<TScanRangeParams>> ranges{
            {0, {grouped_queue_task(0)}}, {1, {grouped_queue_task(1)}}, {2, {}}};
    ASSIGN_OR_ABORT(auto factory, node.convert_scan_range_to_morsel_queue_factory(
                                          {}, ranges, 1, 3, true, true, TTabletInternalParallelMode::AUTO, true));
    EXPECT_FALSE(factory->is_shared());
    EXPECT_FALSE(factory->could_local_shuffle());
    const auto old_death_style = ::testing::FLAGS_gtest_death_test_style;
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    EXPECT_EXIT(
            { std::_Exit(factory->enable_random_append_split_morsel() ? 1 : 0); }, ::testing::ExitedWithCode(0), "");
    ::testing::FLAGS_gtest_death_test_style = old_death_style;
    for (int driver = 0; driver < 2; ++driver) {
        auto* queue = factory->create(driver);
        ASSERT_EQ(MorselQueue::DYNAMIC, queue->type());
        EXPECT_FALSE(queue->has_more_scan_ranges());
        EXPECT_FALSE(queue->has_more_from_split());
        ASSIGN_OR_ABORT(auto root, queue->try_get());
        ASSERT_NE(nullptr, root);
        Morsels children;
        for (int child = 0; child < 4; ++child) {
            children.emplace_back(std::make_unique<ScanMorsel>(1, *root->get_scan_range()));
        }
        ASSERT_OK(factory->append_morsels(driver, std::move(children)));
        EXPECT_TRUE(factory->create(2)->empty());
        for (int child = 0; child < 4; ++child) {
            ASSIGN_OR_ABORT(auto next, queue->try_get());
            ASSERT_NE(nullptr, next);
            EXPECT_EQ(*root->get_scan_range(), *next->get_scan_range());
        }
        EXPECT_TRUE(queue->empty());
        EXPECT_FALSE(queue->has_more());
    }
}

TEST_F(MorselQueueBuilderTest, grouped_requests_reject_global_unowned_or_cross_driver_groups) {
    ObjectPool pool;
    DescriptorTbl desc;
    TPlanNode plan;
    plan.__set_node_type(TPlanNodeType::LAKE_GROUPED_SCAN_NODE);
    GroupedQueueScanNode node(&pool, plan, desc);
    auto check = [&](const std::vector<TScanRangeParams>& global,
                     const std::map<int32_t, std::vector<TScanRangeParams>>& drivers, bool colocate = true) {
        auto result = node.convert_scan_range_to_morsel_queue_factory(global, drivers, 1, 2, colocate, false,
                                                                      TTabletInternalParallelMode::AUTO, true);
        EXPECT_FALSE(result.ok());
    };
    check({grouped_queue_task(0)}, {});
    check({grouped_queue_task(0)}, {{0, {grouped_queue_task(0)}}});
    check({}, {});
    check({}, {{0, {grouped_queue_task(0)}}}, false);
    check({}, {{0, {grouped_queue_task(0), grouped_queue_task(1)}}});
    check({}, {{0, {grouped_queue_task(0)}}, {1, {grouped_queue_task(0)}}});
    auto missing = grouped_queue_task(0);
    missing.scan_range.__isset.grouped_range_task = false;
    check({}, {{0, {missing}}});
    auto mismatch = grouped_queue_task(0);
    mismatch.scan_range.internal_scan_range.__set_bucket_sequence(1);
    check({}, {{0, {mismatch}}});
    auto streaming = grouped_queue_task(0);
    streaming.__set_has_more(true);
    check({}, {{0, {streaming}}});
    check({}, {{2, {grouped_queue_task(0)}}});
}

} // namespace starrocks::pipeline
