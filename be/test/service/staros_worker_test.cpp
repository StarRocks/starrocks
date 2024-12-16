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

#ifdef USE_STAROS
#include "service/staros_worker.h"

#include <gtest/gtest.h>

#include <functional>

namespace starrocks {

static void add_shard_listener(std::vector<StarOSWorker::ShardId>* shardIds, int* counter, StarOSWorker::ShardId id) {
    shardIds->push_back(id);
    ++*counter;
}

TEST(StarOSWorkerTest, test_add_listener) {
    int counter = 0;
    std::vector<StarOSWorker::ShardId> ids;

    auto worker = std::make_unique<StarOSWorker>();

    StarOSWorker::ShardInfo info;

    EXPECT_EQ(0, counter);
    EXPECT_TRUE(ids.empty());

    info.id = 1;
    EXPECT_TRUE(worker->add_shard(info).ok());

    // no shard registered, counter and ids will not be modified
    EXPECT_EQ(0, counter);
    EXPECT_TRUE(ids.empty());

    // register the counter;
    worker->register_add_shard_listener(std::bind(&add_shard_listener, &ids, &counter, std::placeholders::_1));

    info.id = 2;
    EXPECT_TRUE(worker->add_shard(info).ok());

    // shard:2 added
    EXPECT_EQ(1, counter);
    EXPECT_EQ(1, ids.size());
    EXPECT_EQ(2, ids[0]);

    // add it again
    EXPECT_TRUE(worker->add_shard(info).ok());
    // no change, the shard:2 is already added
    EXPECT_EQ(1, counter);
    EXPECT_EQ(1, ids.size());
}

} // namespace starrocks
#endif
