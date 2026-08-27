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

#include <memory>
#include <utility>

#include "exec/pipeline/scan/morsel_queue_factory.h"
#include "exec_primitive/pipeline/scan/dynamic_morsel_queue_builder.h"
#include "exec_primitive/pipeline/scan/fixed_morsel_queue_builder.h"
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

} // namespace starrocks::pipeline
