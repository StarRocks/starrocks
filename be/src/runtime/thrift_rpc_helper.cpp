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

#include "runtime/thrift_rpc_helper.h"

#include <sstream>

#include "base/network/network_util.h"
#include "base/time/monotime.h"
#include "common/config_rpc_client_fwd.h"
#include "common/status.h"
#include "common/util/thrift_util.h"
#include "gen_cpp/BackendService.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/TFileBrokerService.h"

namespace starrocks {

ThriftRpcClientCaches ThriftRpcHelper::_s_client_caches;

void ThriftRpcHelper::setup(const ThriftRpcClientCaches& client_caches) {
    _s_client_caches = client_caches;
}

void ThriftRpcHelper::clear() {
    _s_client_caches = {};
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
                            const std::function<void(ClientConnection<T>&)>& callback) {
    return rpc(_s_client_caches.get<T>(), ip, port, callback, config::thrift_rpc_timeout_ms);
}

template <typename T>
Status ThriftRpcHelper::rpc(const std::string& ip, const int32_t port,
                            const std::function<void(ClientConnection<T>&)>& callback, int timeout_ms,
                            int retry_times) {
    return rpc(_s_client_caches.get<T>(), ip, port, callback, timeout_ms, retry_times);
}

template <typename T>
Status ThriftRpcHelper::rpc(ClientCache<T>* client_cache, const std::string& ip, const int32_t port,
                            const std::function<void(ClientConnection<T>&)>& callback) {
    return rpc(client_cache, ip, port, callback, config::thrift_rpc_timeout_ms);
}

template <typename T>
Status ThriftRpcHelper::rpc(ClientCache<T>* client_cache, const std::string& ip, const int32_t port,
                            const std::function<void(ClientConnection<T>&)>& callback, int timeout_ms,
                            int retry_times) {
    if (UNLIKELY(client_cache == nullptr)) {
        return Status::ThriftRpcError(
                "Thrift client has not been setup to send rpc. Maybe BE has not been started completely. Please retry "
                "later");
    }
    TNetworkAddress address = make_network_address(ip, port);
    Status status;
    ClientConnection<T> client(client_cache, address, timeout_ms, &status);
    if (!status.ok()) {
        client_cache->close_connections(address);
        LOG(WARNING) << "Connect " << ThriftMsgTypeTraits<T>::rpc_name << " failed, address=" << address
                     << ", status=" << status.message();
        return status;
    }

    int i = 0;
    do {
        status = rpc_impl(callback, client, address);
        if (status.ok()) {
            return Status::OK();
        }
        LOG(WARNING) << "rpc failed: " << status << ", retry times: " << i << "/" << retry_times
                     << ", address=" << address << ", timeout_ms=" << timeout_ms;
        SleepFor(MonoDelta::FromMilliseconds(config::thrift_client_retry_interval_ms));
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
        const std::function<void(ClientConnection<FrontendServiceClient>&)>& callback);

template Status ThriftRpcHelper::rpc<BackendServiceClient>(
        const std::string& ip, const int32_t port,
        const std::function<void(ClientConnection<BackendServiceClient>&)>& callback);

template Status ThriftRpcHelper::rpc<TFileBrokerServiceClient>(
        const std::string& ip, const int32_t port,
        const std::function<void(ClientConnection<TFileBrokerServiceClient>&)>& callback);

template Status ThriftRpcHelper::rpc<FrontendServiceClient>(
        const std::string& ip, const int32_t port,
        const std::function<void(ClientConnection<FrontendServiceClient>&)>& callback, int timeout_ms, int retry_times);

template Status ThriftRpcHelper::rpc<BackendServiceClient>(
        const std::string& ip, const int32_t port,
        const std::function<void(ClientConnection<BackendServiceClient>&)>& callback, int timeout_ms, int retry_times);

template Status ThriftRpcHelper::rpc<TFileBrokerServiceClient>(
        const std::string& ip, const int32_t port,
        const std::function<void(ClientConnection<TFileBrokerServiceClient>&)>& callback, int timeout_ms,
        int retry_times);

template Status ThriftRpcHelper::rpc<FrontendServiceClient>(
        FrontendServiceClientCache* client_cache, const std::string& ip, const int32_t port,
        const std::function<void(ClientConnection<FrontendServiceClient>&)>& callback);

template Status ThriftRpcHelper::rpc<BackendServiceClient>(
        BackendServiceClientCache* client_cache, const std::string& ip, const int32_t port,
        const std::function<void(ClientConnection<BackendServiceClient>&)>& callback);

template Status ThriftRpcHelper::rpc<TFileBrokerServiceClient>(
        BrokerServiceClientCache* client_cache, const std::string& ip, const int32_t port,
        const std::function<void(ClientConnection<TFileBrokerServiceClient>&)>& callback);

template Status ThriftRpcHelper::rpc<FrontendServiceClient>(
        FrontendServiceClientCache* client_cache, const std::string& ip, const int32_t port,
        const std::function<void(ClientConnection<FrontendServiceClient>&)>& callback, int timeout_ms, int retry_times);

template Status ThriftRpcHelper::rpc<BackendServiceClient>(
        BackendServiceClientCache* client_cache, const std::string& ip, const int32_t port,
        const std::function<void(ClientConnection<BackendServiceClient>&)>& callback, int timeout_ms, int retry_times);

template Status ThriftRpcHelper::rpc<TFileBrokerServiceClient>(
        BrokerServiceClientCache* client_cache, const std::string& ip, const int32_t port,
        const std::function<void(ClientConnection<TFileBrokerServiceClient>&)>& callback, int timeout_ms,
        int retry_times);

} // namespace starrocks
