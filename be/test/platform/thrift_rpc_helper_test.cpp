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

#include "platform/thrift_rpc_helper.h"

#include <gtest/gtest.h>
#include <thrift/transport/TServerSocket.h>

#include <limits>
#include <memory>
#include <vector>

#include "base/network/network_util.h"
#include "base/testutil/assert.h"
#include "base/testutil/sync_point.h"
#include "common/config_rpc_client_fwd.h"
#include "common/util/thrift_client_cache.h"
#include "gen_cpp/BackendService.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/TFileBrokerService.h"

namespace starrocks {

class ThriftRpcHelperTest : public ::testing::Test {
protected:
    void SetUp() override { ThriftRpcHelper::clear(); }
    void TearDown() override { ThriftRpcHelper::clear(); }
};

TEST_F(ThriftRpcHelperTest, retry_backoff_ceiling) {
    const std::vector<int64_t> expected = {100, 200, 400, 800, 1000, 1000};
    for (int i = 0; i < static_cast<int>(expected.size()); ++i) {
        EXPECT_EQ(expected[i], ThriftRpcHelper::retry_backoff_ceiling_ms(100, i));
    }
    EXPECT_EQ(25, ThriftRpcHelper::retry_backoff_ceiling_ms(25, 0));
    EXPECT_EQ(100, ThriftRpcHelper::retry_backoff_ceiling_ms(25, 2));
    EXPECT_EQ(0, ThriftRpcHelper::retry_backoff_ceiling_ms(0, 10));
    EXPECT_EQ(0, ThriftRpcHelper::retry_backoff_ceiling_ms(-1, 10));
    EXPECT_EQ(0, ThriftRpcHelper::retry_backoff_ceiling_ms(std::numeric_limits<int64_t>::min(), 10));
    EXPECT_EQ(1000, ThriftRpcHelper::retry_backoff_ceiling_ms(std::numeric_limits<int64_t>::max(), 0));
    EXPECT_EQ(1000, ThriftRpcHelper::retry_backoff_ceiling_ms(1, std::numeric_limits<int>::max()));
    EXPECT_EQ(0, ThriftRpcHelper::retry_backoff_ceiling_ms(0, std::numeric_limits<int>::max()));
}

class ThriftRpcHelperRetryTest : public ThriftRpcHelperTest {
protected:
    void SetUp() override {
        ThriftRpcHelperTest::SetUp();
        _saved_interval_ms = config::thrift_client_retry_interval_ms;
        config::thrift_client_retry_interval_ms = 100;
        // Callbacks below simulate RPC outcomes. A listening socket is enough for
        // the real client cache's initial connect and subsequent reopens.
        _listener = std::make_unique<apache::thrift::transport::TServerSocket>("127.0.0.1", 0);
        _listener->listen();
        _port = _listener->getPort();
        ASSERT_GT(_port, 0);
        auto* sync_point = SyncPoint::GetInstance();
        sync_point->SetCallBack("ThriftRpcHelper::rpc::backoff_ms", [this](void* arg) {
            auto* delay_ms = static_cast<int64_t*>(arg);
            _delays_ms.push_back(*delay_ms);
            *delay_ms = 0;
        });
        sync_point->EnableProcessing();
    }

    void TearDown() override {
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearAllCallBacks();
        config::thrift_client_retry_interval_ms = _saved_interval_ms;
        _cache.close_connections(make_network_address("127.0.0.1", _port));
        _listener->close();
        ThriftRpcHelperTest::TearDown();
    }

    Status call(const ThriftRpcHelper::ConnectionCallBack<FrontendServiceClient>& callback, int retry_times = 2) {
        return ThriftRpcHelper::rpc<FrontendServiceClient>(&_cache, "127.0.0.1", _port, callback, 1000, retry_times);
    }

    FrontendServiceClientCache _cache;
    std::unique_ptr<apache::thrift::transport::TServerSocket> _listener;
    int _port = 0;
    int64_t _saved_interval_ms = 0;
    std::vector<int64_t> _delays_ms;
};

TEST_F(ThriftRpcHelperRetryTest, success_does_not_back_off) {
    int attempts = 0;
    ASSERT_OK(call([&](FrontendServiceConnection&) { ++attempts; }));
    EXPECT_EQ(1, attempts);
    EXPECT_TRUE(_delays_ms.empty());
}

TEST_F(ThriftRpcHelperRetryTest, recovers_after_retry) {
    int attempts = 0;
    ASSERT_OK(call([&](FrontendServiceConnection&) {
        if (++attempts < 3) {
            throw apache::thrift::TException("transient failure");
        }
    }));
    EXPECT_EQ(3, attempts);
    ASSERT_EQ(2, _delays_ms.size());
    EXPECT_GE(_delays_ms[0], 0);
    EXPECT_LE(_delays_ms[0], 100);
    EXPECT_GE(_delays_ms[1], 0);
    EXPECT_LE(_delays_ms[1], 200);
}

TEST_F(ThriftRpcHelperRetryTest, exhausted_retries_preserve_error_without_final_wait) {
    int attempts = 0;
    auto st = call([&](FrontendServiceConnection&) {
        ++attempts;
        throw apache::thrift::transport::TTransportException(apache::thrift::transport::TTransportException::TIMED_OUT,
                                                             "original timeout");
    });
    EXPECT_STATUS(Status::ThriftRpcError(""), st);
    EXPECT_NE(std::string::npos, st.message().find("original timeout"));
    EXPECT_EQ(3, attempts);
    EXPECT_EQ(2, _delays_ms.size());
}

TEST_F(ThriftRpcHelperRetryTest, zero_retries_does_not_wait) {
    int attempts = 0;
    auto st = call(
            [&](FrontendServiceConnection&) {
                ++attempts;
                throw apache::thrift::TException("failure");
            },
            0);
    EXPECT_STATUS(Status::ThriftRpcError(""), st);
    EXPECT_EQ(1, attempts);
    EXPECT_TRUE(_delays_ms.empty());
}

TEST_F(ThriftRpcHelperRetryTest, samples_full_jitter_and_snapshots_interval) {
    constexpr int retries = 32;
    int attempts = 0;
    auto st = call(
            [&](FrontendServiceConnection&) {
                ++attempts;
                config::thrift_client_retry_interval_ms = 0;
                throw apache::thrift::TException("failure");
            },
            retries);
    EXPECT_STATUS(Status::ThriftRpcError(""), st);
    EXPECT_EQ(retries + 1, attempts);
    ASSERT_EQ(retries, _delays_ms.size());
    bool has_positive_delay = false;
    bool has_different_delay = false;
    for (int i = 0; i < retries; ++i) {
        EXPECT_GE(_delays_ms[i], 0);
        EXPECT_LE(_delays_ms[i], ThriftRpcHelper::retry_backoff_ceiling_ms(100, i));
        has_positive_delay |= _delays_ms[i] > 0;
        has_different_delay |= _delays_ms[i] != _delays_ms[0];
    }
    EXPECT_TRUE(has_positive_delay);
    EXPECT_TRUE(has_different_delay);
}

TEST_F(ThriftRpcHelperRetryTest, nonpositive_interval_disables_wait) {
    for (int64_t interval_ms : {0, -1}) {
        config::thrift_client_retry_interval_ms = interval_ms;
        _delays_ms.clear();
        auto st = call([](FrontendServiceConnection&) { throw apache::thrift::TException("failure"); });
        EXPECT_STATUS(Status::ThriftRpcError(""), st);
        EXPECT_EQ((std::vector<int64_t>{0, 0}), _delays_ms);
    }
}

TEST_F(ThriftRpcHelperRetryTest, initial_connection_failure_does_not_retry) {
    _listener->close();
    auto st = call([](FrontendServiceConnection&) { ADD_FAILURE() << "callback should not run"; });
    EXPECT_STATUS(Status::ThriftRpcError(""), st);
    EXPECT_TRUE(_delays_ms.empty());
}

TEST_F(ThriftRpcHelperRetryTest, reopen_failure_preserves_original_error) {
    SyncPoint::GetInstance()->SetCallBack("ThriftRpcHelper::rpc::backoff_ms", [this](void* arg) {
        _delays_ms.push_back(*static_cast<int64_t*>(arg));
        *static_cast<int64_t*>(arg) = 0;
        _listener->close();
    });
    int attempts = 0;
    auto st = call([&](FrontendServiceConnection&) {
        ++attempts;
        throw apache::thrift::TException("original failure");
    });
    EXPECT_STATUS(Status::ThriftRpcError(""), st);
    EXPECT_NE(std::string::npos, st.message().find("original failure"));
    EXPECT_EQ(1, attempts);
    EXPECT_EQ(1, _delays_ms.size());
}

TEST_F(ThriftRpcHelperTest, rpc_after_clear_returns_setup_error) {
    FrontendServiceClientCache frontend_cache;
    ThriftRpcHelper::setup(nullptr, &frontend_cache, nullptr);
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
