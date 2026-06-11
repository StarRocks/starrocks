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

#include "exec/pipeline/scan/morsel_queue_factory.h"

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <string>

#include "exec/pipeline/scan/fixed_morsel_queue.h"

namespace starrocks::pipeline {

namespace {

Morsels make_morsels(int num_morsels) {
    Morsels morsels;
    morsels.reserve(num_morsels);
    for (int i = 0; i < num_morsels; ++i) {
        morsels.emplace_back(std::make_unique<ScanMorsel>(1, TScanRange()));
    }
    return morsels;
}

MorselQueuePtr make_queue(int num_morsels) {
    return std::make_unique<FixedMorselQueue>(make_morsels(num_morsels));
}

} // namespace

TEST(MorselQueueFactoryTest, individual_rejects_append_to_missing_driver_seq) {
    std::map<int, MorselQueuePtr> queues;
    queues.emplace(0, make_queue(1));
    IndividualMorselQueueFactory factory(std::move(queues), false, false);

    ASSERT_EQ(1, factory.size());
    auto status = factory.append_morsels(1, make_morsels(1));

    EXPECT_FALSE(status.ok());
    EXPECT_NE(std::string::npos, status.message().find("driver_seq"));
    EXPECT_EQ(1, factory.size());
}

TEST(MorselQueueFactoryTest, bucket_sequence_rejects_append_to_missing_driver_seq) {
    std::map<int, MorselQueuePtr> queues;
    queues.emplace(0, make_queue(1));
    BucketSequenceMorselQueueFactory factory(std::move(queues), false);

    ASSERT_EQ(1, factory.size());
    auto status = factory.append_morsels(1, make_morsels(1));

    EXPECT_FALSE(status.ok());
    EXPECT_NE(std::string::npos, status.message().find("driver_seq"));
    EXPECT_EQ(1, factory.size());
}

} // namespace starrocks::pipeline
