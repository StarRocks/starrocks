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

#include "runtime/thrift_rpc_helper.h"

#include <gtest/gtest.h>

#include "base/network/network_util.h"
#include "base/testutil/assert.h"
#include "gen_cpp/BackendService.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/TFileBrokerService.h"
#include "runtime/client_cache.h"

namespace starrocks {

class ThriftRpcHelperTest : public ::testing::Test {
protected:
    void SetUp() override { ThriftRpcHelper::clear(); }
    void TearDown() override { ThriftRpcHelper::clear(); }
};

TEST_F(ThriftRpcHelperTest, client_caches_get_typed_cache) {
    BackendServiceClientCache backend_cache;
    FrontendServiceClientCache frontend_cache;
    BrokerServiceClientCache broker_cache;
    ThriftRpcClientCaches caches{&backend_cache, &frontend_cache, &broker_cache};

    EXPECT_EQ(&backend_cache, caches.get<BackendServiceClient>());
    EXPECT_EQ(&frontend_cache, caches.get<FrontendServiceClient>());
    EXPECT_EQ(&broker_cache, caches.get<TFileBrokerServiceClient>());
}

TEST_F(ThriftRpcHelperTest, rpc_after_clear_returns_setup_error) {
    FrontendServiceClientCache frontend_cache;
    ThriftRpcHelper::setup({nullptr, &frontend_cache, nullptr});
    ThriftRpcHelper::clear();

    auto st = ThriftRpcHelper::rpc<FrontendServiceClient>(
            "127.0.0.1", 9020, [](FrontendServiceConnection&) { ADD_FAILURE() << "callback should not run"; }, 1, 0);
    EXPECT_STATUS(Status::ThriftRpcError(""), st);
    EXPECT_EQ(
            "Rpc error: Thrift client has not been setup to send rpc. Maybe BE has not been started completely. Please "
            "retry later",
            st.to_string());
}

TEST_F(ThriftRpcHelperTest, fe_rpc_impl) {
    {
        auto addr = make_network_address("127.0.0.1", 9020);
        FrontendServiceConnection client;
        auto st = ThriftRpcHelper::rpc_impl<FrontendServiceClient>(
                [](FrontendServiceConnection& client) {
                    throw apache::thrift::protocol::TProtocolException(
                            apache::thrift::protocol::TProtocolException::INVALID_DATA, "invalid TType");
                },
                client, addr);
        EXPECT_STATUS(Status::ThriftRpcError(""), st);
        EXPECT_EQ(
                "Rpc error: FE RPC failure, address=TNetworkAddress(hostname=127.0.0.1, port=9020), reason=invalid "
                "TType",
                st.to_string());
    }
    {
        auto addr = make_network_address("127.0.0.1", 9020);
        FrontendServiceConnection client;
        auto st = ThriftRpcHelper::rpc_impl<FrontendServiceClient>(
                [](FrontendServiceConnection& client) {
                    throw apache::thrift::protocol::TProtocolException(
                            apache::thrift::protocol::TProtocolException::SIZE_LIMIT, "message size limit");
                },
                client, addr);
        EXPECT_STATUS(Status::ThriftRpcError(""), st);
        EXPECT_EQ(
                "Rpc error: FE RPC failure, address=TNetworkAddress(hostname=127.0.0.1, port=9020), reason=message "
                "size limit",
                st.to_string());
    }

    {
        auto addr = make_network_address("127.0.0.1", 9020);
        FrontendServiceConnection client;
        auto st = ThriftRpcHelper::rpc_impl<FrontendServiceClient>(
                [](FrontendServiceConnection& client) {
                    throw apache::thrift::transport::TTransportException(
                            apache::thrift::transport::TTransportException::TIMED_OUT, "timeout");
                },
                client, addr);
        EXPECT_STATUS(Status::ThriftRpcError(""), st);
        EXPECT_EQ("Rpc error: FE RPC failure, address=TNetworkAddress(hostname=127.0.0.1, port=9020), reason=timeout",
                  st.to_string());
    }

    {
        auto addr = make_network_address("127.0.0.1", 9020);
        FrontendServiceConnection client;
        auto st = ThriftRpcHelper::rpc_impl<FrontendServiceClient>(
                [](FrontendServiceConnection& client) {
                    throw apache::thrift::transport::TTransportException(
                            apache::thrift::transport::TTransportException::CORRUPTED_DATA, "corrupted data");
                },
                client, addr);
        EXPECT_STATUS(Status::ThriftRpcError(""), st);
        EXPECT_EQ(
                "Rpc error: FE RPC failure, address=TNetworkAddress(hostname=127.0.0.1, port=9020), reason=corrupted "
                "data",
                st.to_string());
    }

    {
        auto addr = make_network_address("127.0.0.1", 9020);
        FrontendServiceConnection client;
        auto st = ThriftRpcHelper::rpc_impl<FrontendServiceClient>(
                [](FrontendServiceConnection& client) { throw apache::thrift::TException("some error"); }, client,
                addr);
        EXPECT_STATUS(Status::ThriftRpcError(""), st);
        EXPECT_EQ(
                "Rpc error: FE RPC failure, address=TNetworkAddress(hostname=127.0.0.1, port=9020), "
                "reason=some error",
                st.to_string());
    }
}

TEST_F(ThriftRpcHelperTest, be_cn_rpc_impl) {
    {
        auto addr = make_network_address("127.0.0.1", 8060);
        BackendServiceConnection client;
        auto st = ThriftRpcHelper::rpc_impl<BackendServiceClient>(
                [](BackendServiceConnection& client) { throw apache::thrift::TException("some error"); }, client, addr);
        EXPECT_STATUS(Status::ThriftRpcError(""), st);
        EXPECT_EQ(
                "Rpc error: BE/CN RPC failure, address=TNetworkAddress(hostname=127.0.0.1, port=8060), "
                "reason=some error",
                st.to_string());
    }
}

TEST_F(ThriftRpcHelperTest, broker_rpc_impl) {
    {
        auto addr = make_network_address("127.0.0.1", 8060);
        BrokerServiceConnection client;
        auto st = ThriftRpcHelper::rpc_impl<TFileBrokerServiceClient>(
                [](BrokerServiceConnection& client) { throw apache::thrift::TException("some error"); }, client, addr);
        EXPECT_STATUS(Status::ThriftRpcError(""), st);
        EXPECT_EQ(
                "Rpc error: Broker RPC failure, address=TNetworkAddress(hostname=127.0.0.1, port=8060), "
                "reason=some error",
                st.to_string());
    }
}

} // namespace starrocks
