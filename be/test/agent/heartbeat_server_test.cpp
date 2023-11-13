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

#include "agent/heartbeat_server.h"

#include "gtest/gtest.h"
#include "service/staros_worker.h"
#include "util/thrift_server.h"

namespace starrocks {

#ifdef USE_STAROS
extern std::shared_ptr<StarOSWorker> g_worker;
#endif

class HeartbeatServerTest : public testing::Test {
public:
    void SetUp() override {
#ifdef USE_STAROS
        g_worker = std::make_shared<StarOSWorker>();
#endif
    }

    void TearDown() override {
#ifdef USE_STAROS
        g_worker.reset();
#endif
    }
};

TEST_F(HeartbeatServerTest, test_heartbeat_info_num_tablets) {
    auto heartbeat_server = std::make_unique<HeartbeatServer>();

    {
        THeartbeatResult result;
        TMasterInfo master_info;
        heartbeat_server->heartbeat(result, master_info);
#ifdef USE_STAROS
        EXPECT_TRUE(result.backend_info.__isset.num_tablets);
        EXPECT_EQ(0, result.backend_info.num_tablets);
#else
        // the field is not set under shared-nothing mode
        EXPECT_FALSE(result.backend_info.__isset.num_tablets);
#endif
    }

#ifdef USE_STAROS
    // Following are all for shared-data mode testing
    StarOSWorker::ShardInfo shard1, shard2;
    shard1.id = 10086;
    shard2.id = 10087;

    // add two shards
    EXPECT_TRUE(g_worker->add_shard(shard1).ok());
    EXPECT_TRUE(g_worker->add_shard(shard2).ok());

    {
        THeartbeatResult result;
        TMasterInfo master_info;
        heartbeat_server->heartbeat(result, master_info);
        EXPECT_TRUE(result.backend_info.__isset.num_tablets);
        EXPECT_EQ(2, result.backend_info.num_tablets);
    }

    // remove one shard
    EXPECT_TRUE(g_worker->remove_shard(shard1.id).ok());
    {
        THeartbeatResult result;
        TMasterInfo master_info;
        heartbeat_server->heartbeat(result, master_info);
        EXPECT_TRUE(result.backend_info.__isset.num_tablets);
        EXPECT_EQ(1, result.backend_info.num_tablets);
    }
#endif
}

} // namespace starrocks
