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

#include "exec/pipeline/sink/exchange_sink_operator.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace starrocks::pipeline {

// used for mock of
Status (*hostname_to_ip_ptr)(const std::string&, std::string&, bool) = hostname_to_ip;

class ExchangeSinkOperatorChannelTest : public ::testing::Test {
protected:
    ExchangeSinkOperator::Channel* channel;

    ExchangeSinkOperator* parent = nullptr;
    TUniqueId fragment_instance_id;
    TNetworkAddress brpc_dest;
    PlanNodeId dest_node_id = 1000;
    int32_t num_shuffles = 16;
    bool enable_exchange_pass_through = true;
    bool enable_exchange_perf = true;
    PassThroughChunkBuffer* pass_through_chunk_buffer = nullptr;

    RuntimeState* state;

    void SetUp() override { state = new RuntimeState(); }

    void TearDown() override {
        delete channel;
        delete state;
    }
};

Status mock_hostname_to_ip(const std::string& host, std::string& ip, bool ipv6) {
    if (host == "valid.hostname.com") {
        ip = "192.168.1.1";
        return Status::OK();
    }
    return Status::InternalError("Invalid host");
}

TEST_F(ExchangeSinkOperatorChannelTest, test_init_with_valid_hostname) {
    brpc_dest.hostname = "valid.hostname.com";
    brpc_dest.port = 123;
    channel = new ExchangeSinkOperator::Channel(parent, brpc_dest, fragment_instance_id, dest_node_id, num_shuffles,
                                                enable_exchange_pass_through, enable_exchange_perf,
                                                pass_through_chunk_buffer);

    hostname_to_ip_ptr = mock_hostname_to_ip;

    Status status = channel->init(state);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(channel->get_brpc_dest_real_ip(), "192.168.1.1");
}

TEST_F(ExchangeSinkOperatorChannelTest, test_init_with_invalid_hostname) {
    brpc_dest.hostname = "invalid.hostname.com";
    brpc_dest.port = 123;
    channel = new ExchangeSinkOperator::Channel(parent, brpc_dest, fragment_instance_id, dest_node_id, num_shuffles,
                                                enable_exchange_pass_through, enable_exchange_perf,
                                                pass_through_chunk_buffer);

    hostname_to_ip_ptr = mock_hostname_to_ip;

    Status status = channel->init(state);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), Status::InternalError("Invalid host").code());
}
} // namespace starrocks::pipeline