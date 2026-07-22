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

#include "base/time/time.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/statusor.h"
#include "exec/pipeline/schedule/pipeline_timer.h"
#include "gen_cpp/Types_types.h" // TNetworkAddress
#include "gen_cpp/internal_service.pb.h"
#include "service/brpc.h"
#include "util/internal_service_recoverable_stub.h"
#include "util/network_util.h"
#include "util/spinlock.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

class ExecEnv;

template <typename StubCacheT>
class EndpointCleanupTask : public pipeline::PipelineTimerTask {
public:
    EndpointCleanupTask(StubCacheT* cache, const butil::EndPoint& endpoint, int64_t ttl_seconds)
            : _cache(cache), _endpoint(endpoint), _ttl_seconds(ttl_seconds) {}

    void Run() override {
        std::lock_guard<SpinLock> l(_cache->_lock);
        if (_cache->_stopping) {
            return;
        }
        if (!_cache->is_cleanup_task_owner_locked(_endpoint, this)) {
            return;
        }
        const int64_t now_us = butil::gettimeofday_us();
        if (now_us >= _deadline) {
            LOG(INFO) << "cleanup brpc stub, endpoint:" << _endpoint << ", idle for "
                      << (now_us - _deadline) / 1000 << "ms past deadline";
            _cache->_stub_map.erase(_endpoint);
            return;
        }

        auto new_task = std::make_shared<EndpointCleanupTask<StubCacheT>>(_cache, _endpoint, _ttl_seconds);
        new_task->renew_deadline_locked(_deadline);
        if (!_cache->replace_cleanup_task_locked(_endpoint, new_task)) {
            return;
        }

        timespec tm = butil::microseconds_to_timespec(_deadline);
        auto status = _cache->_pipeline_timer->schedule(new_task.get(), tm);
        if (!status.ok()) {
            LOG(WARNING) << "Failed to reschedule brpc cleanup task: " << _endpoint;
            _cache->_stub_map.erase(_endpoint);
        }
    }

    void renew_deadline_locked(int64_t new_deadline) { _deadline = new_deadline; }
    int64_t deadline_locked() const { return _deadline; }

private:
    StubCacheT* _cache;
    butil::EndPoint _endpoint;
    int64_t _deadline{0};
    int64_t _ttl_seconds{0};
};

class BrpcStubCache {
public:
    BrpcStubCache(ExecEnv* exec_env);
    ~BrpcStubCache();

    std::shared_ptr<PInternalService_RecoverableStub> get_stub(const butil::EndPoint& endpoint);
    std::shared_ptr<PInternalService_RecoverableStub> get_stub(const TNetworkAddress& taddr);
    std::shared_ptr<PInternalService_RecoverableStub> get_stub(const std::string& host, int port);

private:
    friend class EndpointCleanupTask<BrpcStubCache>;

    bool is_cleanup_task_owner_locked(const butil::EndPoint& endpoint,
                                      const EndpointCleanupTask<BrpcStubCache>* task) const {
        auto pool = _stub_map.seek(endpoint);
        return pool != nullptr && (*pool)->_cleanup_task.get() == task;
    }

    bool replace_cleanup_task_locked(const butil::EndPoint& endpoint,
                                     std::shared_ptr<EndpointCleanupTask<BrpcStubCache>> task);

    // StubPool is used to store all stubs with a single endpoint, and the client in the same BE process maintains up to
    // brpc_max_connections_per_server single connections with each server.
    // These connections will be created during the first few accesses and will be reused later.
    struct StubPool {
        StubPool();
        ~StubPool();
        std::shared_ptr<PInternalService_RecoverableStub> get_or_create(const butil::EndPoint& endpoint);

        std::vector<std::shared_ptr<PInternalService_RecoverableStub>> _stubs;
        int64_t _idx = -1;
        std::shared_ptr<EndpointCleanupTask<BrpcStubCache>> _cleanup_task;
    };

    SpinLock _lock;
    butil::FlatMap<butil::EndPoint, std::shared_ptr<StubPool>> _stub_map;
    pipeline::PipelineTimer* _pipeline_timer;
    bool _stopping{false};
};

class HttpBrpcStubCache {
public:
    static HttpBrpcStubCache* getInstance();
    StatusOr<std::shared_ptr<PInternalService_RecoverableStub>> get_http_stub(const TNetworkAddress& taddr);
    void shutdown();

private:
    HttpBrpcStubCache();
    HttpBrpcStubCache(const HttpBrpcStubCache&) = delete;
    HttpBrpcStubCache& operator=(const HttpBrpcStubCache&) = delete;
    ~HttpBrpcStubCache();

    friend class EndpointCleanupTask<HttpBrpcStubCache>;

    bool is_cleanup_task_owner_locked(const butil::EndPoint& endpoint,
                                      const EndpointCleanupTask<HttpBrpcStubCache>* task) const {
        auto entry = _stub_map.seek(endpoint);
        return entry != nullptr && entry->cleanup_task.get() == task;
    }

    bool replace_cleanup_task_locked(const butil::EndPoint& endpoint,
                                     std::shared_ptr<EndpointCleanupTask<HttpBrpcStubCache>> task);

    struct StubEntry {
        std::shared_ptr<PInternalService_RecoverableStub> stub;
        std::shared_ptr<EndpointCleanupTask<HttpBrpcStubCache>> cleanup_task;
    };

    SpinLock _lock;
    butil::FlatMap<butil::EndPoint, StubEntry> _stub_map;
    pipeline::PipelineTimer* _pipeline_timer;
    bool _stopping{false};
};

} // namespace starrocks
