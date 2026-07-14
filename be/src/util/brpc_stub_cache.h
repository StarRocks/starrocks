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

<<<<<<< HEAD:be/src/util/brpc_stub_cache.h
=======
#include "base/brpc/brpc.h"
#include "base/concurrency/spinlock.h"
#include "base/network/network_util.h"
#include "base/time/time.h"
#include "common/brpc/internal_service_recoverable_stub.h"
#include "common/bthread_timer.h"
#include "common/logging.h"
>>>>>>> 2bbca67281 ([BugFix] Fix bRPC stub cache clean timer leak (#75973)):be/src/common/brpc/brpc_stub_cache.h
#include "common/statusor.h"
#include "exec/pipeline/schedule/pipeline_timer.h"
#include "gen_cpp/Types_types.h" // TNetworkAddress
#include "service/brpc.h"
#include "util/internal_service_recoverable_stub.h"
#include "util/lake_service_recoverable_stub.h"
#include "util/network_util.h"
#include "util/spinlock.h"

namespace starrocks {

constexpr int TIMER_TASK_RUNNING = 1;

class ExecEnv;

template <typename StubCacheT>
<<<<<<< HEAD:be/src/util/brpc_stub_cache.h
class EndpointCleanupTask : public starrocks::pipeline::LightTimerTask {
=======
class EndpointCleanupTask : public BthreadTimerTask {
>>>>>>> 2bbca67281 ([BugFix] Fix bRPC stub cache clean timer leak (#75973)):be/src/common/brpc/brpc_stub_cache.h
public:
    // ttl_seconds is the cache-wide expire window (config::brpc_stub_expire_s).
    EndpointCleanupTask(StubCacheT* cache, const butil::EndPoint& endpoint, int64_t ttl_seconds)
            : _cache(cache), _endpoint(endpoint), _ttl_seconds(ttl_seconds) {}
    // The actual cleanup/renewal decision must run while the cache's _lock is held so
    // that _stopping and _deadline are observed atomically with the cache state.
    void Run() override {
        std::lock_guard<SpinLock> l(_cache->_lock);
        if (_cache->_stopping) {
            return;
        }
        // Make sure this task is still the authoritative cleanup task for the
        // endpoint before rescheduling. If shutdown() cleared the cache or a new
        // entry was created for the same endpoint, this task is stale and must
        // not schedule anything.
        if (!_cache->is_cleanup_task_owner_locked(_endpoint, this)) {
            return;
        }
        int64_t now_us = butil::gettimeofday_us();
        if (now_us >= _deadline) {
            LOG(INFO) << "cleanup brpc stub, endpoint:" << _endpoint << ", idle for " << (now_us - _deadline) / 1000
                      << "ms past deadline";
            _cache->_stub_map.erase(_endpoint);
            return;
        }
        auto new_task = std::make_shared<EndpointCleanupTask<StubCacheT>>(_cache, _endpoint, _ttl_seconds);
        new_task->_deadline = _deadline;
        if (!_cache->replace_cleanup_task_locked(_endpoint, new_task)) {
            return;
        }
        timespec tm = butil::microseconds_to_timespec(_deadline);
        auto status = _cache->_timer->schedule(new_task.get(), tm);
        if (!status.ok()) {
            LOG(WARNING) << "Failed to reschedule brpc cleanup task: " << _endpoint;
            // Drop the entry; the next get_*_stub() will recreate it with a fresh task.
            _cache->_stub_map.erase(_endpoint);
        }
    }

    // Reset the absolute deadline (in butil::gettimeofday_us() units) used by the next
    // Run() invocation to decide between evict and reschedule. Caller must hold the
    // cache lock.
    void renew_deadline_locked(int64_t new_deadline) { _deadline = new_deadline; }
    int64_t deadline_locked() const { return _deadline; }

private:
    StubCacheT* _cache;
    butil::EndPoint _endpoint;
    // Absolute deadline (in butil::gettimeofday_us() units) used to decide whether a
    // firing task should evict the stub or simply reschedule itself. Read/written only
    // under the cache's _lock, so it does not need to be atomic.
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
<<<<<<< HEAD:be/src/util/brpc_stub_cache.h
=======
    friend class EndpointCleanupTask<BrpcStubCache>;

    template <typename CacheT, typename ExtractFn>
    friend void wait_clean_tasks_terminate(CacheT* cache, ExtractFn extract);

    template <typename CacheT>
    friend void reset_state_for_rebind(CacheT* cache, BthreadTimer* timer);

    bool is_cleanup_task_owner_locked(const butil::EndPoint& endpoint,
                                      const EndpointCleanupTask<BrpcStubCache>* task) const {
        auto pool = _stub_map.seek(endpoint);
        return pool != nullptr && (*pool)->_cleanup_task.get() == task;
    }

    bool replace_cleanup_task_locked(const butil::EndPoint& endpoint,
                                     std::shared_ptr<EndpointCleanupTask<BrpcStubCache>> task);

    struct Metrics;
>>>>>>> 2bbca67281 ([BugFix] Fix bRPC stub cache clean timer leak (#75973)):be/src/common/brpc/brpc_stub_cache.h
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
<<<<<<< HEAD:be/src/util/brpc_stub_cache.h
    pipeline::PipelineTimer* _pipeline_timer;
=======
    BthreadTimer* _timer;
    std::unique_ptr<Metrics> _metrics;
    bool _stopping{false};
>>>>>>> 2bbca67281 ([BugFix] Fix bRPC stub cache clean timer leak (#75973)):be/src/common/brpc/brpc_stub_cache.h
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
<<<<<<< HEAD:be/src/util/brpc_stub_cache.h

    SpinLock _lock;
    butil::FlatMap<butil::EndPoint, std::pair<std::shared_ptr<PInternalService_RecoverableStub>,
                                              std::shared_ptr<EndpointCleanupTask<HttpBrpcStubCache>>>>
            _stub_map;
    pipeline::PipelineTimer* _pipeline_timer;
=======
    void bind_timer(BthreadTimer* timer);
    friend class EndpointCleanupTask<HttpBrpcStubCache>;

    template <typename CacheT, typename ExtractFn>
    friend void wait_clean_tasks_terminate(CacheT* cache, ExtractFn extract);

    template <typename CacheT>
    friend void reset_state_for_rebind(CacheT* cache, BthreadTimer* timer);

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
    BthreadTimer* _timer;
    bool _stopping{false};
>>>>>>> 2bbca67281 ([BugFix] Fix bRPC stub cache clean timer leak (#75973)):be/src/common/brpc/brpc_stub_cache.h
};

class LakeServiceBrpcStubCache {
public:
    static LakeServiceBrpcStubCache* getInstance();
    StatusOr<std::shared_ptr<starrocks::LakeService_RecoverableStub>> get_stub(const std::string& host, int port);
    void shutdown();

private:
    LakeServiceBrpcStubCache();
    LakeServiceBrpcStubCache(const LakeServiceBrpcStubCache&) = delete;
    LakeServiceBrpcStubCache& operator=(const LakeServiceBrpcStubCache&) = delete;
    ~LakeServiceBrpcStubCache();
<<<<<<< HEAD:be/src/util/brpc_stub_cache.h

    SpinLock _lock;
    butil::FlatMap<butil::EndPoint, std::pair<std::shared_ptr<LakeService_RecoverableStub>,
                                              std::shared_ptr<EndpointCleanupTask<LakeServiceBrpcStubCache>>>>
            _stub_map;
    pipeline::PipelineTimer* _pipeline_timer;
=======
    void bind_timer(BthreadTimer* timer);
    friend class EndpointCleanupTask<LakeServiceBrpcStubCache>;

    template <typename CacheT, typename ExtractFn>
    friend void wait_clean_tasks_terminate(CacheT* cache, ExtractFn extract);

    template <typename CacheT>
    friend void reset_state_for_rebind(CacheT* cache, BthreadTimer* timer);

    bool is_cleanup_task_owner_locked(const butil::EndPoint& endpoint,
                                      const EndpointCleanupTask<LakeServiceBrpcStubCache>* task) const {
        auto entry = _stub_map.seek(endpoint);
        return entry != nullptr && entry->cleanup_task.get() == task;
    }

    bool replace_cleanup_task_locked(const butil::EndPoint& endpoint,
                                     std::shared_ptr<EndpointCleanupTask<LakeServiceBrpcStubCache>> task);

    struct StubEntry {
        std::shared_ptr<LakeService_RecoverableStub> stub;
        std::shared_ptr<EndpointCleanupTask<LakeServiceBrpcStubCache>> cleanup_task;
    };

    SpinLock _lock;
    butil::FlatMap<butil::EndPoint, StubEntry> _stub_map;
    BthreadTimer* _timer;
    bool _stopping{false};
>>>>>>> 2bbca67281 ([BugFix] Fix bRPC stub cache clean timer leak (#75973)):be/src/common/brpc/brpc_stub_cache.h
};

} // namespace starrocks
