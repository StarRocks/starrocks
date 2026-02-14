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

#include "common/system/backend_options.h"

#include <vector>

#include "base/network/cidr.h"
#include "common/config.h"
#include "gen_cpp/Types_types.h"
#include "gtest/gtest.h"

namespace starrocks {

class BackendOptionsTest : public ::testing::Test {
protected:
    void SetUp() override {
        _priority_networks_backup = config::priority_networks;
        _localhost_backup = BackendOptions::_s_localhost;
        _local_ip_backup = BackendOptions::_s_local_ip;
        _resolved_ip_backup = BackendOptions::_s_resolved_ip;
        _priority_cidrs_backup = BackendOptions::_s_priority_cidrs;
        _backend_backup = BackendOptions::_backend;
        _bind_ipv6_backup = BackendOptions::_bind_ipv6;
        _is_cn_backup = BackendOptions::_is_cn;
    }

    void TearDown() override {
        config::priority_networks = _priority_networks_backup;
        BackendOptions::_s_localhost = _localhost_backup;
        BackendOptions::_s_local_ip = _local_ip_backup;
        BackendOptions::_s_resolved_ip = _resolved_ip_backup;
        BackendOptions::_s_priority_cidrs = _priority_cidrs_backup;
        BackendOptions::_backend = _backend_backup;
        BackendOptions::_bind_ipv6 = _bind_ipv6_backup;
        BackendOptions::_is_cn = _is_cn_backup;
    }

private:
    std::string _priority_networks_backup;
    std::string _localhost_backup;
    std::string _local_ip_backup;
    std::string _resolved_ip_backup;
    std::vector<CIDR> _priority_cidrs_backup;
    TBackend _backend_backup;
    bool _bind_ipv6_backup = false;
    bool _is_cn_backup = false;
};

TEST_F(BackendOptionsTest, SetLocalhostSetsHostAndIp) {
    BackendOptions::set_localhost("127.0.0.1");

    EXPECT_EQ("127.0.0.1", BackendOptions::get_localhost());
    EXPECT_EQ("127.0.0.1", BackendOptions::get_local_ip());
}

TEST_F(BackendOptionsTest, SetLocalhostWithIpKeepsSeparatedValues) {
    BackendOptions::set_localhost("backend.local", "10.10.10.9");

    EXPECT_EQ("backend.local", BackendOptions::get_localhost());
    EXPECT_EQ("10.10.10.9", BackendOptions::get_local_ip());
}

TEST_F(BackendOptionsTest, GetLocalBackendReflectsHostAndPorts) {
    BackendOptions::set_localhost("192.168.0.10");

    TBackend backend = BackendOptions::get_localBackend();
    EXPECT_EQ("192.168.0.10", backend.host);
    EXPECT_EQ(config::be_port, backend.be_port);
    EXPECT_EQ(config::be_http_port, backend.http_port);
}

TEST_F(BackendOptionsTest, AnalyzePriorityCidrsAndMatchIp) {
    BackendOptions::_s_priority_cidrs.clear();
    config::priority_networks = "10.10.10.0/24;2001:db8::/32";

    ASSERT_TRUE(BackendOptions::analyze_priority_cidrs());
    ASSERT_EQ(2, BackendOptions::_s_priority_cidrs.size());

    EXPECT_TRUE(BackendOptions::is_in_prior_network("10.10.10.8"));
    EXPECT_TRUE(BackendOptions::is_in_prior_network("2001:db8::1"));
    EXPECT_FALSE(BackendOptions::is_in_prior_network("10.10.11.8"));
}

TEST_F(BackendOptionsTest, EmptyPriorityNetworkConfigIsAllowed) {
    BackendOptions::_s_priority_cidrs.clear();
    config::priority_networks.clear();

    ASSERT_TRUE(BackendOptions::analyze_priority_cidrs());
    EXPECT_TRUE(BackendOptions::_s_priority_cidrs.empty());
    EXPECT_FALSE(BackendOptions::is_in_prior_network("10.10.10.8"));
}

} // namespace starrocks
