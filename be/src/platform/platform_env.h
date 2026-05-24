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

#include <memory>

#include "common/status.h"
#include "common/util/thrift_client_cache.h"

namespace starrocks {

class BrpcStubCache;
class BthreadTimer;
class HttpBrpcStubCache;
class MetricRegistry;
#ifndef __APPLE__
class LakeServiceBrpcStubCache;
#endif

class PlatformEnv {
public:
    static PlatformEnv* GetInstance() {
        static PlatformEnv s_platform_env;
        return &s_platform_env;
    }

    PlatformEnv();
    ~PlatformEnv();

    PlatformEnv(const PlatformEnv&) = delete;
    PlatformEnv& operator=(const PlatformEnv&) = delete;

    Status init(MetricRegistry* metrics);
    void destroy();

    BackendServiceClientCache* backend_client_cache() const { return _backend_client_cache.get(); }
    FrontendServiceClientCache* frontend_client_cache() const { return _frontend_client_cache.get(); }
    BrokerServiceClientCache* broker_client_cache() const { return _broker_client_cache.get(); }
    BrpcStubCache* brpc_stub_cache() const { return _brpc_stub_cache.get(); }
    HttpBrpcStubCache* http_brpc_stub_cache() const;
#ifndef __APPLE__
    LakeServiceBrpcStubCache* lake_service_brpc_stub_cache() const;
#endif

private:
    std::unique_ptr<BackendServiceClientCache> _backend_client_cache;
    std::unique_ptr<FrontendServiceClientCache> _frontend_client_cache;
    std::unique_ptr<BrokerServiceClientCache> _broker_client_cache;
    std::unique_ptr<BthreadTimer> _rpc_timer;
    std::unique_ptr<BrpcStubCache> _brpc_stub_cache;
};

} // namespace starrocks
