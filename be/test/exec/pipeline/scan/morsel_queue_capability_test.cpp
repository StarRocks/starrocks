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

#include "exec_primitive/pipeline/scan/dynamic_morsel_queue.h"
#include "exec_primitive/pipeline/scan/fixed_morsel_queue.h"
#include "exec_primitive/pipeline/scan/ticketed_morsel_queue.h"
#include "storage/query/bucket_sequence_morsel_queue.h"
#include "storage/query/olap_dynamic_morsel_queue.h"
#include "storage/query/olap_fixed_morsel_queue.h"
#include "storage/query/olap_morsel_queue.h"
#include "storage/query/split_morsel_queue.h"

namespace starrocks::pipeline {

namespace {

// Production code discovers these capabilities through a MorselQueue*, so query them the same way. Casting the
// concrete (final) queue type directly lets the compiler fold the result and warn that the cast can never succeed.
template <class Capability>
Capability* query_capability(MorselQueue* queue) {
    return dynamic_cast<Capability*>(queue);
}

} // namespace

class MorselQueueCapabilityTest : public ::testing::Test {};

TEST_F(MorselQueueCapabilityTest, primitive_fixed_queue_has_no_olap_or_ticket_capability) {
    FixedMorselQueue queue(Morsels{});

    EXPECT_EQ(nullptr, query_capability<OlapMorselQueue>(&queue));
    EXPECT_EQ(nullptr, query_capability<TicketedMorselQueue>(&queue));
}

TEST_F(MorselQueueCapabilityTest, primitive_dynamic_queue_has_ticket_capability_only) {
    DynamicMorselQueue queue(Morsels{}, false);

    EXPECT_EQ(nullptr, query_capability<OlapMorselQueue>(&queue));
    EXPECT_NE(nullptr, query_capability<TicketedMorselQueue>(&queue));
}

TEST_F(MorselQueueCapabilityTest, fixed_queue_is_olap_capable_only) {
    OlapFixedMorselQueue queue(Morsels{});

    EXPECT_NE(nullptr, query_capability<OlapMorselQueue>(&queue));
    EXPECT_EQ(nullptr, query_capability<TicketedMorselQueue>(&queue));
}

TEST_F(MorselQueueCapabilityTest, dynamic_queue_is_olap_and_ticket_capable) {
    OlapDynamicMorselQueue queue(Morsels{}, false);

    EXPECT_NE(nullptr, query_capability<OlapMorselQueue>(&queue));
    EXPECT_NE(nullptr, query_capability<TicketedMorselQueue>(&queue));
}

TEST_F(MorselQueueCapabilityTest, split_queues_are_olap_and_ticket_capable) {
    PhysicalSplitMorselQueue physical_queue(Morsels{}, 1, 1024);
    LogicalSplitMorselQueue logical_queue(Morsels{}, 1, 1024);

    EXPECT_NE(nullptr, query_capability<OlapMorselQueue>(&physical_queue));
    EXPECT_NE(nullptr, query_capability<TicketedMorselQueue>(&physical_queue));
    EXPECT_NE(nullptr, query_capability<OlapMorselQueue>(&logical_queue));
    EXPECT_NE(nullptr, query_capability<TicketedMorselQueue>(&logical_queue));
}

TEST_F(MorselQueueCapabilityTest, bucket_sequence_queue_is_olap_and_ticket_capable) {
    auto nested_queue = std::make_unique<OlapFixedMorselQueue>(Morsels{});
    BucketSequenceMorselQueue queue(std::move(nested_queue));

    EXPECT_NE(nullptr, query_capability<OlapMorselQueue>(&queue));
    auto* ticketed_queue = query_capability<TicketedMorselQueue>(&queue);
    ASSERT_NE(nullptr, ticketed_queue);
    EXPECT_TRUE(ticketed_queue->should_attach_ticket_checker(false));
    EXPECT_TRUE(ticketed_queue->should_attach_ticket_checker(true));
}

} // namespace starrocks::pipeline
