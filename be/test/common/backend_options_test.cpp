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

#include "service/backend_options.h"

#include <netinet/in.h>

#include <vector>

#include "common/status.h"
#include "gtest/gtest.h"
#include "testutil/assert.h"
#include "util/network_util.h"

namespace starrocks {

class BackendOptionsTest : public ::testing::Test {};

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
