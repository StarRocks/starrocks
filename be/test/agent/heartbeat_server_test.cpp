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

TEST(HeartbeatServerTest, test_print_master_info_all_fields_set) {
    HeartbeatServer server;
    TMasterInfo info;

    // Mock the TMasterInfo structure with all fields set
    info.network_address = "127.0.0.1";
    info.__isset.cluster_id = true;
    info.cluster_id = 1;
    info.__isset.epoch = true;
    info.epoch = 100;
    info.__isset.token = true;
    info.token = "token_value";
    info.__isset.backend_ip = true;
    info.backend_ip = "192.168.1.1";
    info.__isset.http_port = true;
    info.http_port = 8080;
    info.__isset.heartbeat_flags = true;
    info.heartbeat_flags = 2;
    info.__isset.backend_id = true;
    info.backend_id = 123;
    info.__isset.min_active_txn_id = true;
    info.min_active_txn_id = 456;
    info.__isset.run_mode = true;
    info.run_mode = "run_mode_value";
    info.__isset.disabled_disks = true;
    info.disabled_disks = "disk1,disk2";
    info.__isset.decommissioned_disks = true;
    info.decommissioned_disks = "disk3";
    info.__isset.encrypted = true;
    info.encrypted = true;

    std::string expected_output =
            "TMasterInfo(network_address=127.0.0.1, cluster_id=1, epoch=100, token=<hidden>, "
            "backend_ip=192.168.1.1, http_port=8080, heartbeat_flags=2, backend_id=123, "
            "min_active_txn_id=456, run_mode=run_mode_value, disabled_disks=disk1,disk2, "
            "decommissioned_disks=disk3, encrypted=1)";

    EXPECT_EQ(server.print_master_info(info), expected_output);
}

TEST(HeartbeatServerTest, test_print_master_info_some_fields_null) {
    HeartbeatServer server;
    TMasterInfo info;

    // Only some fields are set, others remain unset
    info.network_address = "127.0.0.1";
    info.__isset.cluster_id = false;  // Unset
    info.__isset.epoch = true;
    info.epoch = 100;
    info.__isset.token = false;  // Unset
    info.__isset.backend_ip = true;
    info.backend_ip = "192.168.1.1";

    std::string expected_output =
            "TMasterInfo(network_address=127.0.0.1, cluster_id=<null>, epoch=100, token=<null>, "
            "backend_ip=192.168.1.1, http_port=<null>, heartbeat_flags=<null>, backend_id=<null>, "
            "min_active_txn_id=<null>, run_mode=<null>, disabled_disks=<null>, "
            "decommissioned_disks=<null>, encrypted=<null>)";

    EXPECT_EQ(server.print_master_info(info), expected_output);
}

} // namespace starrocks
