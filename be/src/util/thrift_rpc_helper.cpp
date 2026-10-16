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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/thrift_rpc_helper.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "util/thrift_rpc_helper.h"

#include <algorithm>
#include <random>
#include <sstream>

#include "common/status.h"
#include "gen_cpp/FrontendService.h"
#include "monotime.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "testutil/sync_point.h"
#include "util/network_util.h"
#include "util/thrift_util.h"

namespace starrocks {

using apache::thrift::protocol::TProtocol;
using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::transport::TSocket;
using apache::thrift::transport::TTransport;
using apache::thrift::transport::TBufferedTransport;

ExecEnv* ThriftRpcHelper::_s_exec_env = nullptr;

void ThriftRpcHelper::setup(ExecEnv* exec_env) {
    _s_exec_env = exec_env;
}

int64_t ThriftRpcHelper::retry_backoff_ceiling_ms(int64_t retry_interval_ms, int retry_index) {
    constexpr int64_t max_backoff_ms = 1000;
    // Clamp before shifting to avoid overflow for large intervals or retry counts.
    // Any positive millisecond interval reaches the cap within ten doublings.
    const int64_t base_ms = std::clamp<int64_t>(retry_interval_ms, 0, max_backoff_ms);
    return std::min(base_ms << std::clamp(retry_index, 0, 10), max_backoff_ms);
}

template <typename T>
struct ThriftMsgTypeTraits {};

template <>
struct ThriftMsgTypeTraits<FrontendServiceClient> {
    constexpr static const char* rpc_name = "FE RPC";
};

template <>
struct ThriftMsgTypeTraits<BackendServiceClient> {
    constexpr static const char* rpc_name = "BE/CN RPC";
};

template <>
struct ThriftMsgTypeTraits<TFileBrokerServiceClient> {
    constexpr static const char* rpc_name = "Broker RPC";
};

template <typename T>
Status ThriftRpcHelper::rpc_impl(const std::function<void(ClientConnection<T>&)>& callback, ClientConnection<T>& client,
                                 const TNetworkAddress& address) noexcept {
    std::stringstream ss;
    try {
        callback(client);
        return Status::OK();
    } catch (apache::thrift::TException& e) {
        ss << ThriftMsgTypeTraits<T>::rpc_name << " failure, address=" << address << ", reason=" << e.what();
    }

    return Status::ThriftRpcError(ss.str());
}

template <typename T>
Status ThriftRpcHelper::rpc(const std::string& ip, const int32_t port,
                            std::function<void(ClientConnection<T>&)> callback, int timeout_ms, int retry_times) {
    if (UNLIKELY(_s_exec_env == nullptr)) {
        return Status::ThriftRpcError(
                "Thrift client has not been setup to send rpc. Maybe BE has not been started completely. Please retry "
                "later");
    }
    TNetworkAddress address = make_network_address(ip, port);
    Status status;
    ClientConnection<T> client(_s_exec_env->get_client_cache<T>(), address, timeout_ms, &status);
    if (!status.ok()) {
        _s_exec_env->get_client_cache<T>()->close_connections(address);
        LOG(WARNING) << "Connect " << ThriftMsgTypeTraits<T>::rpc_name << " failed, address=" << address
                     << ", status=" << status.message();
        return status;
    }

    // Keep a mutable config change from altering an RPC's backoff sequence midway.
    const int64_t retry_interval_ms = config::thrift_client_retry_interval_ms;
    int i = 0;
    do {
        status = rpc_impl(callback, client, address);
        if (status.ok()) {
            return Status::OK();
        }
        const int64_t ceiling_ms = retry_backoff_ceiling_ms(retry_interval_ms, i);
        int64_t backoff_ms = 0;
        if (i < retry_times && ceiling_ms > 0) {
            static thread_local std::mt19937 rng(std::random_device{}());
            backoff_ms = std::uniform_int_distribution<int64_t>(0, ceiling_ms)(rng);
        }
        LOG(WARNING) << "rpc failed: " << status << ", retry times: " << i << "/" << retry_times
                     << ", address=" << address << ", timeout_ms=" << timeout_ms << ", retry_backoff_ms=" << backoff_ms;
        if (i < retry_times) {
            TEST_SYNC_POINT_CALLBACK("ThriftRpcHelper::rpc::backoff_ms", &backoff_ms);
            SleepFor(MonoDelta::FromMilliseconds(backoff_ms));
        }
        // Preserve the final reopen to avoid returning a broken connection to the cache,
        // but only wait when another RPC attempt will actually run.
        // reopen failure will disable this connection to prevent it from being used again.
        auto st = client.reopen(timeout_ms);
        if (!st.ok()) {
            LOG(WARNING) << "rpc client reopen failed. address=" << address << ", status=" << st.message()
                         << ", retry times: " << i << "/" << retry_times;
            break;
        }
    } while (i++ < retry_times);
    return status;
}

template Status ThriftRpcHelper::rpc<FrontendServiceClient>(
        const std::string& ip, const int32_t port,
        std::function<void(ClientConnection<FrontendServiceClient>&)> callback, int timeout_ms, int retry_times);

template Status ThriftRpcHelper::rpc<BackendServiceClient>(
        const std::string& ip, const int32_t port,
        std::function<void(ClientConnection<BackendServiceClient>&)> callback, int timeout_ms, int retry_times);

template Status ThriftRpcHelper::rpc<TFileBrokerServiceClient>(
        const std::string& ip, const int32_t port,
        std::function<void(ClientConnection<TFileBrokerServiceClient>&)> callback, int timeout_ms, int retry_times);

} // namespace starrocks
