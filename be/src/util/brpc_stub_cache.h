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

#include <exec/pipeline/schedule/pipeline_timer.h>

#include <memory>
#include <mutex>
#include <vector>

#include "common/statusor.h"
#include "gen_cpp/Types_types.h" // TNetworkAddress
#include "service/brpc.h"
#include "util/internal_service_recoverable_stub.h"
#include "util/lake_service_recoverable_stub.h"
#include "util/network_util.h"
#include "util/spinlock.h"

namespace starrocks {

class ExecEnv;
class EndpointCleanupTask;

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
        EndpointCleanupTask* _cleanup_task = nullptr;
    };

    SpinLock _lock;
    butil::FlatMap<butil::EndPoint, std::shared_ptr<StubPool>> _stub_map;
    pipeline::PipelineTimer* _pipeline_timer;
};

class HttpBrpcStubCache {
public:
    static HttpBrpcStubCache* getInstance();
    StatusOr<std::shared_ptr<PInternalService_RecoverableStub>> get_http_stub(const TNetworkAddress& taddr);

private:
    HttpBrpcStubCache();
    HttpBrpcStubCache(const HttpBrpcStubCache&) = delete;
    HttpBrpcStubCache& operator=(const HttpBrpcStubCache&) = delete;

    SpinLock _lock;
    butil::FlatMap<butil::EndPoint, std::shared_ptr<PInternalService_RecoverableStub>> _stub_map;
};

class LakeServiceBrpcStubCache {
public:
    static LakeServiceBrpcStubCache* getInstance();
    StatusOr<std::shared_ptr<starrocks::LakeService_RecoverableStub>> get_stub(const std::string& host, int port);

private:
    LakeServiceBrpcStubCache();
    LakeServiceBrpcStubCache(const LakeServiceBrpcStubCache&) = delete;
    LakeServiceBrpcStubCache& operator=(const LakeServiceBrpcStubCache&) = delete;

    SpinLock _lock;
    butil::FlatMap<butil::EndPoint, std::shared_ptr<LakeService_RecoverableStub>> _stub_map;
};

class EndpointCleanupTask : public starrocks::pipeline::PipelineTimerTask {
public:
    EndpointCleanupTask(BrpcStubCache* cache, const butil::EndPoint& endpoint) : _cache(cache), _endpoint(endpoint){};
    void Run() override;

private:
    BrpcStubCache* _cache;
    butil::EndPoint _endpoint;
};

} // namespace starrocks
