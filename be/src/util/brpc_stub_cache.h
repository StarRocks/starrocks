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
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/brpc_stub_cache.h

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

#pragma once

#include <memory>
#include <mutex>
#include <vector>

#include "common/statusor.h"
#include "exec/pipeline/schedule/pipeline_timer.h"
#include "gen_cpp/Types_types.h" // TNetworkAddress
#include "service/brpc.h"
#include "util/internal_service_recoverable_stub.h"
#include "util/network_util.h"
#include "util/spinlock.h"

#ifndef __APPLE__
#include "util/lake_service_recoverable_stub.h"
#endif

namespace starrocks {

constexpr int TIMER_TASK_RUNNING = 1;

class ExecEnv;

template <typename StubCacheT>
class EndpointCleanupTask : public starrocks::pipeline::LightTimerTask {
public:
    EndpointCleanupTask(StubCacheT* cache, const butil::EndPoint& endpoint) : _cache(cache), _endpoint(endpoint){};
    void Run() override { _cache->cleanup_expired(_endpoint); }

private:
    StubCacheT* _cache;
    butil::EndPoint _endpoint;
};

class BrpcStubCache {
public:
    BrpcStubCache(ExecEnv* exec_env);
    ~BrpcStubCache();

    std::shared_ptr<PInternalService_RecoverableStub> get_stub(const butil::EndPoint& endpoint);
    std::shared_ptr<PInternalService_RecoverableStub> get_stub(const TNetworkAddress& taddr);
    std::shared_ptr<PInternalService_RecoverableStub> get_stub(const std::string& host, int port);
    void cleanup_expired(const butil::EndPoint& endpoint);

private:
    struct StubPool {
        StubPool();
        ~StubPool();
        std::shared_ptr<PInternalService_RecoverableStub> get_or_create(const butil::EndPoint& endpoint);

        std::vector<std::shared_ptr<PInternalService_RecoverableStub>> _stubs;
        int64_t _idx;
        std::shared_ptr<EndpointCleanupTask<BrpcStubCache>> _cleanup_task;
    };

    SpinLock _lock;
    butil::FlatMap<butil::EndPoint, std::shared_ptr<StubPool>> _stub_map;
    pipeline::PipelineTimer* _pipeline_timer;
};

class HttpBrpcStubCache {
public:
    static HttpBrpcStubCache* getInstance();
    StatusOr<std::shared_ptr<PInternalService_RecoverableStub>> get_http_stub(const TNetworkAddress& taddr);
    void cleanup_expired(const butil::EndPoint& endpoint);
    void shutdown();

private:
    HttpBrpcStubCache();
    HttpBrpcStubCache(const HttpBrpcStubCache&) = delete;
    HttpBrpcStubCache& operator=(const HttpBrpcStubCache&) = delete;
    ~HttpBrpcStubCache();

    SpinLock _lock;
    butil::FlatMap<butil::EndPoint, std::pair<std::shared_ptr<PInternalService_RecoverableStub>,
                                              std::shared_ptr<EndpointCleanupTask<HttpBrpcStubCache>>>>
            _stub_map;
    pipeline::PipelineTimer* _pipeline_timer;
};

#ifndef __APPLE__
class LakeServiceBrpcStubCache {
public:
    static LakeServiceBrpcStubCache* getInstance();
    StatusOr<std::shared_ptr<starrocks::LakeService_RecoverableStub>> get_stub(const std::string& host, int port);
    void cleanup_expired(const butil::EndPoint& endpoint);
    void shutdown();

private:
    LakeServiceBrpcStubCache();
    LakeServiceBrpcStubCache(const LakeServiceBrpcStubCache&) = delete;
    LakeServiceBrpcStubCache& operator=(const LakeServiceBrpcStubCache&) = delete;
    ~LakeServiceBrpcStubCache();

    SpinLock _lock;
    butil::FlatMap<butil::EndPoint, std::pair<std::shared_ptr<LakeService_RecoverableStub>,
                                              std::shared_ptr<EndpointCleanupTask<LakeServiceBrpcStubCache>>>>
            _stub_map;
    pipeline::PipelineTimer* _pipeline_timer;
};
#endif

} // namespace starrocks
