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

#pragma once

#include "util/phmap/phmap.h"

namespace starrocks {

<<<<<<< HEAD:be/src/exec/hash_map.h
using AggDataPtr = uint8_t*;
using Int32AggHashMap = phmap::flat_hash_map<int32_t, AggDataPtr>;
using Int32AggTwoLevelHashMap = phmap::parallel_flat_hash_map<int32_t, AggDataPtr>;
=======
// defined in common/process_exit.cpp
extern std::atomic<bool> k_starrocks_exit;

TEST(HeartbeatServerTest, test_shutdown_heartbeat) {
    HeartbeatServer server;
    THeartbeatResult result;
    TMasterInfo info;

    k_starrocks_exit = true;
    server.heartbeat(result, info);
    EXPECT_EQ(TStatusCode::SHUTDOWN, result.status.status_code);
    Status status(result.status);
    EXPECT_TRUE(status.is_shutdown());
    k_starrocks_exit = false;
}
>>>>>>> f59b0ac3b2 ([Refactor] refactor backend process exit code (#52116)):be/test/agent/heartbeat_server_test.cpp

} // namespace starrocks
