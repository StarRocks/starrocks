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

namespace starrocks {

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

} // namespace starrocks
