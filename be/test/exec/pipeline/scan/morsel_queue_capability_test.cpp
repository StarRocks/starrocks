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

#include "exec/pipeline/scan/bucket_sequence_morsel_queue.h"
#include "exec/pipeline/scan/dynamic_morsel_queue.h"
#include "exec/pipeline/scan/fixed_morsel_queue.h"
#include "exec/pipeline/scan/olap_morsel_queue.h"
#include "exec/pipeline/scan/split_morsel_queue.h"
#include "exec/pipeline/scan/ticketed_morsel_queue.h"

namespace starrocks::pipeline {

class MorselQueueCapabilityTest : public ::testing::Test {};

TEST_F(MorselQueueCapabilityTest, fixed_queue_is_olap_capable_only) {
    FixedMorselQueue queue(Morsels{});

    EXPECT_NE(nullptr, dynamic_cast<OlapMorselQueue*>(&queue));
    EXPECT_EQ(nullptr, dynamic_cast<TicketedMorselQueue*>(&queue));
}

TEST_F(MorselQueueCapabilityTest, dynamic_queue_is_olap_and_ticket_capable) {
    DynamicMorselQueue queue(Morsels{}, false);

    EXPECT_NE(nullptr, dynamic_cast<OlapMorselQueue*>(&queue));
    EXPECT_NE(nullptr, dynamic_cast<TicketedMorselQueue*>(&queue));
}

TEST_F(MorselQueueCapabilityTest, split_queues_are_olap_and_ticket_capable) {
    PhysicalSplitMorselQueue physical_queue(Morsels{}, 1, 1024);
    LogicalSplitMorselQueue logical_queue(Morsels{}, 1, 1024);

    EXPECT_NE(nullptr, dynamic_cast<OlapMorselQueue*>(&physical_queue));
    EXPECT_NE(nullptr, dynamic_cast<TicketedMorselQueue*>(&physical_queue));
    EXPECT_NE(nullptr, dynamic_cast<OlapMorselQueue*>(&logical_queue));
    EXPECT_NE(nullptr, dynamic_cast<TicketedMorselQueue*>(&logical_queue));
}

TEST_F(MorselQueueCapabilityTest, bucket_sequence_queue_is_olap_and_ticket_capable) {
    auto nested_queue = std::make_unique<FixedMorselQueue>(Morsels{});
    BucketSequenceMorselQueue queue(std::move(nested_queue));

    EXPECT_NE(nullptr, dynamic_cast<OlapMorselQueue*>(&queue));
    EXPECT_NE(nullptr, dynamic_cast<TicketedMorselQueue*>(&queue));
}

} // namespace starrocks::pipeline
