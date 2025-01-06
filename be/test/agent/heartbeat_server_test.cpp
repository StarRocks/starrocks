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

#include "gen_cpp/Types_types.h"
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

TEST(HeartbeatServerTest, test_print_master_info_with_token_null) {
    HeartbeatServer server;
    TMasterInfo master_info;

    master_info.network_address.__set_hostname("127.0.0.1");
    master_info.network_address.__set_port(8080);
    master_info.__set_cluster_id(12345);
    master_info.__set_epoch(100);
    master_info.__set_backend_ip("192.168.1.1");

    std::string expected_output =
            "TMasterInfo(network_address=TNetworkAddress(hostname=127.0.0.1, port=8080), "
            "cluster_id=12345, epoch=100, token=<null>, backend_ip=192.168.1.1, "
            "http_port=<null>, heartbeat_flags=<null>, backend_id=<null>, "
            "min_active_txn_id=0, run_mode=<null>, disabled_disks=<null>, "
            "decommissioned_disks=<null>, encrypted=<null>, stop_regular_tablet_report=<null>)";

    EXPECT_EQ(server.print_master_info(master_info), expected_output);
}

TEST(HeartbeatServerTest, test_print_master_info_with_token_hidden) {
    HeartbeatServer server;
    TMasterInfo master_info;

    master_info.network_address.__set_hostname("127.0.0.1");
    master_info.network_address.__set_port(8080);
    master_info.__set_cluster_id(12345);
    master_info.__set_epoch(100);
    master_info.__set_token("secret_token");
    master_info.__set_backend_ip("192.168.1.1");

    std::string expected_output =
            "TMasterInfo(network_address=TNetworkAddress(hostname=127.0.0.1, port=8080), "
            "cluster_id=12345, epoch=100, token=<hidden>, backend_ip=192.168.1.1, "
            "http_port=<null>, heartbeat_flags=<null>, backend_id=<null>, "
            "min_active_txn_id=0, run_mode=<null>, disabled_disks=<null>, "
            "decommissioned_disks=<null>, encrypted=<null>, stop_regular_tablet_report=<null>)";

    EXPECT_EQ(server.print_master_info(master_info), expected_output);
}

} // namespace starrocks
