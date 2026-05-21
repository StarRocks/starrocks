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

#include "exec/pipeline/scan/dynamic_morsel_queue_builder.h"
#include "exec/pipeline/scan/fixed_morsel_queue_builder.h"
#include "exec/pipeline/scan/olap_dynamic_morsel_queue_builder.h"
#include "exec/pipeline/scan/olap_fixed_morsel_queue_builder.h"
#include "exec/pipeline/scan/split_morsel_queue_builder.h"

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

} // namespace starrocks::pipeline
