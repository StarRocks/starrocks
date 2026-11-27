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

#include <netinet/in.h>

#include <vector>

#include "base/network/cidr.h"
#include "base/network/network_util.h"
#include "base/testutil/assert.h"
#include "common/config_network_fwd.h"
#include "common/status.h"
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

namespace {

std::vector<InetAddress> loopback_only_hosts() {
    return {InetAddress("127.0.0.1", AF_INET, true), InetAddress("::1", AF_INET6, true)};
}

bool has_non_loopback(const std::vector<InetAddress>& hosts) {
    for (const auto& host : hosts) {
        if (!host.is_loopback()) {
            return true;
        }
    }
    return false;
}

} // namespace

TEST_F(BackendOptionsTest, WaitForNonLoopbackReturnsAtOnceWhenAvailable) {
    int calls = 0;
    auto provider = [&](std::vector<InetAddress>* hosts) {
        ++calls;
        *hosts = loopback_only_hosts();
        hosts->emplace_back("10.0.0.1", AF_INET, false);
        return Status::OK();
    };
    std::vector<InetAddress> hosts;
    ASSERT_OK(BackendOptions::get_hosts_waiting_for_non_loopback(provider, 60000, 1, &hosts));
    EXPECT_EQ(1, calls);
    EXPECT_EQ(3, hosts.size());
}

// The address shows up after startup, e.g. a DHCP lease is acquired after the process started.
TEST_F(BackendOptionsTest, WaitForNonLoopbackPicksUpLateAddress) {
    int calls = 0;
    auto provider = [&](std::vector<InetAddress>* hosts) {
        ++calls;
        *hosts = loopback_only_hosts();
        if (calls >= 3) {
            hosts->emplace_back("10.133.68.156", AF_INET, false);
        }
        return Status::OK();
    };
    std::vector<InetAddress> hosts;
    ASSERT_OK(BackendOptions::get_hosts_waiting_for_non_loopback(provider, 60000, 1, &hosts));
    EXPECT_EQ(3, calls);
    ASSERT_TRUE(has_non_loopback(hosts));
    EXPECT_EQ("10.133.68.156", hosts.back().get_host_address());
}

TEST_F(BackendOptionsTest, WaitForNonLoopbackGivesUpAfterTimeout) {
    int calls = 0;
    auto provider = [&](std::vector<InetAddress>* hosts) {
        ++calls;
        *hosts = loopback_only_hosts();
        return Status::OK();
    };
    std::vector<InetAddress> hosts;
    ASSERT_OK(BackendOptions::get_hosts_waiting_for_non_loopback(provider, 50, 5, &hosts));
    EXPECT_GT(calls, 1);
    EXPECT_EQ(2, hosts.size());
    EXPECT_FALSE(has_non_loopback(hosts));
}

TEST_F(BackendOptionsTest, WaitForNonLoopbackDisabledByZeroTimeout) {
    int calls = 0;
    auto provider = [&](std::vector<InetAddress>* hosts) {
        ++calls;
        *hosts = loopback_only_hosts();
        return Status::OK();
    };
    std::vector<InetAddress> hosts;
    ASSERT_OK(BackendOptions::get_hosts_waiting_for_non_loopback(provider, 0, 1000, &hosts));
    EXPECT_EQ(1, calls);
    EXPECT_EQ(2, hosts.size());
}

TEST_F(BackendOptionsTest, WaitForNonLoopbackPropagatesProviderError) {
    int calls = 0;
    auto provider = [&](std::vector<InetAddress>* hosts) {
        ++calls;
        return Status::InternalError("getifaddrs failed");
    };
    std::vector<InetAddress> hosts;
    auto st = BackendOptions::get_hosts_waiting_for_non_loopback(provider, 60000, 1, &hosts);
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(1, calls);
}

} // namespace starrocks
