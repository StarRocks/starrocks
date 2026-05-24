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

#pragma once

#include <functional>
#include <string>

#include "common/status.h"
#include "common/util/thrift_client_cache.h"
#include "gen_cpp/Types_types.h"

namespace starrocks {

// this class is a helper for jni call. easy for unit test
class ThriftRpcHelper {
public:
    template <class T>
    using ConnectionCallBack = std::function<void(ClientConnection<T>&)>;
    static void setup(BackendServiceClientCache* backend_client_cache,
                      FrontendServiceClientCache* frontend_client_cache, BrokerServiceClientCache* broker_client_cache);
    static void clear();

    // for default timeout
    template <typename T>
    static Status rpc(const std::string& ip, const int32_t port, const ConnectionCallBack<T>& callback);

    template <typename T>
    static Status rpc(const TNetworkAddress& endpoint, const ConnectionCallBack<T>& callback, int timeout_ms,
                      int retry_times = 2) {
        return rpc(endpoint.hostname, endpoint.port, callback, timeout_ms, retry_times);
    }

    template <typename T>
    static Status rpc(const std::string& ip, const int32_t port, const ConnectionCallBack<T>& callback, int timeout_ms,
                      int retry_times = 2);

    // for default timeout with explicit client cache
    template <typename T>
    static Status rpc(ClientCache<T>* client_cache, const std::string& ip, const int32_t port,
                      const ConnectionCallBack<T>& callback);

    template <typename T>
    static Status rpc(ClientCache<T>* client_cache, const TNetworkAddress& endpoint,
                      const ConnectionCallBack<T>& callback, int timeout_ms, int retry_times = 2) {
        return rpc(client_cache, endpoint.hostname, endpoint.port, callback, timeout_ms, retry_times);
    }

    template <typename T>
    static Status rpc(ClientCache<T>* client_cache, const std::string& ip, const int32_t port,
                      const ConnectionCallBack<T>& callback, int timeout_ms, int retry_times = 2);

    template <typename T>
    static Status rpc_impl(const ConnectionCallBack<T>& callback, ClientConnection<T>& client,
                           const TNetworkAddress& address) noexcept;

private:
    template <typename T>
    static ClientCache<T>* client_cache();

    static BackendServiceClientCache* _s_backend_client_cache;
    static FrontendServiceClientCache* _s_frontend_client_cache;
    static BrokerServiceClientCache* _s_broker_client_cache;
};

} // namespace starrocks
