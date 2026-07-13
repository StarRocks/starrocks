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

#include "base/brpc/brpc.h"
#include "base/concurrency/spinlock.h"
#include "base/network/network_util.h"
#include "base/time/time.h"
#include "common/brpc/internal_service_recoverable_stub.h"
#include "common/bthread_timer.h"
#include "common/logging.h"
#include "common/statusor.h"
#include "gen_cpp/Types_types.h" // TNetworkAddress

#ifndef __APPLE__
#include "common/brpc/lake_service_recoverable_stub.h"
#endif

namespace starrocks {

class MetricRegistry;

constexpr int TIMER_TASK_RUNNING = 1;

template <typename StubCacheT>
class EndpointCleanupTask : public BthreadTimerTask {
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
        int64_t now_us = butil::gettimeofday_us();
        if (now_us >= _deadline) {
            LOG(INFO) << "cleanup brpc stub, endpoint:" << _endpoint << ", idle for " << (now_us - _deadline) / 1000
                      << "ms past deadline";
            _cache->_stub_map.erase(_endpoint);
            return;
        }
        auto new_task = std::make_shared<EndpointCleanupTask<StubCacheT>>(_cache, _endpoint, _ttl_seconds);
        new_task->_deadline = _deadline;
        _cache->replace_cleanup_task_locked(_endpoint, new_task);
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
    explicit BrpcStubCache(BthreadTimer* timer, MetricRegistry* metrics = nullptr);
    ~BrpcStubCache();

    std::shared_ptr<PInternalService_RecoverableStub> get_stub(const butil::EndPoint& endpoint);
    std::shared_ptr<PInternalService_RecoverableStub> get_stub(const TNetworkAddress& taddr);
    std::shared_ptr<PInternalService_RecoverableStub> get_stub(const std::string& host, int port);

private:
    friend class EndpointCleanupTask<BrpcStubCache>;

    template <typename CacheT, typename ExtractFn>
    friend void wait_clean_tasks_terminate(CacheT* cache, ExtractFn extract);

    template <typename CacheT>
    friend void reset_state_for_rebind(CacheT* cache, BthreadTimer* timer);

    void replace_cleanup_task_locked(const butil::EndPoint& endpoint,
                                     std::shared_ptr<EndpointCleanupTask<BrpcStubCache>> task);

    struct Metrics;
    struct StubPool {
        StubPool();
        ~StubPool();
        std::shared_ptr<PInternalService_RecoverableStub> get_or_create(const butil::EndPoint& endpoint);

        std::vector<std::shared_ptr<PInternalService_RecoverableStub>> _stubs;
        int64_t _idx{-1};
        std::shared_ptr<EndpointCleanupTask<BrpcStubCache>> _cleanup_task;
    };

    SpinLock _lock;
    butil::FlatMap<butil::EndPoint, std::shared_ptr<StubPool>> _stub_map;
    BthreadTimer* _timer;
    std::unique_ptr<Metrics> _metrics;
    bool _stopping{false};
};

class HttpBrpcStubCache {
public:
    HttpBrpcStubCache(const HttpBrpcStubCache&) = delete;
    HttpBrpcStubCache& operator=(const HttpBrpcStubCache&) = delete;

    static void initialize(BthreadTimer* timer);
    static HttpBrpcStubCache* getInstance();
    StatusOr<std::shared_ptr<PInternalService_RecoverableStub>> get_http_stub(const TNetworkAddress& taddr);
    void shutdown();

private:
    explicit HttpBrpcStubCache(BthreadTimer* timer);
    ~HttpBrpcStubCache();
    void bind_timer(BthreadTimer* timer);
    friend class EndpointCleanupTask<HttpBrpcStubCache>;

    template <typename CacheT, typename ExtractFn>
    friend void wait_clean_tasks_terminate(CacheT* cache, ExtractFn extract);

    template <typename CacheT>
    friend void reset_state_for_rebind(CacheT* cache, BthreadTimer* timer);

    void replace_cleanup_task_locked(const butil::EndPoint& endpoint,
                                     std::shared_ptr<EndpointCleanupTask<HttpBrpcStubCache>> task);

    struct StubEntry {
        std::shared_ptr<PInternalService_RecoverableStub> stub;
        std::shared_ptr<EndpointCleanupTask<HttpBrpcStubCache>> cleanup_task;
    };

    SpinLock _lock;
    butil::FlatMap<butil::EndPoint, StubEntry> _stub_map;
    BthreadTimer* _timer;
    bool _stopping{false};
};

#ifndef __APPLE__
class LakeServiceBrpcStubCache {
public:
    LakeServiceBrpcStubCache(const LakeServiceBrpcStubCache&) = delete;
    LakeServiceBrpcStubCache& operator=(const LakeServiceBrpcStubCache&) = delete;

    static void initialize(BthreadTimer* timer);
    static LakeServiceBrpcStubCache* getInstance();
    StatusOr<std::shared_ptr<starrocks::LakeService_RecoverableStub>> get_stub(const std::string& host, int port);
    void shutdown();

private:
    explicit LakeServiceBrpcStubCache(BthreadTimer* timer);
    ~LakeServiceBrpcStubCache();
    void bind_timer(BthreadTimer* timer);
    friend class EndpointCleanupTask<LakeServiceBrpcStubCache>;

    template <typename CacheT, typename ExtractFn>
    friend void wait_clean_tasks_terminate(CacheT* cache, ExtractFn extract);

    template <typename CacheT>
    friend void reset_state_for_rebind(CacheT* cache, BthreadTimer* timer);

    void replace_cleanup_task_locked(const butil::EndPoint& endpoint,
                                     std::shared_ptr<EndpointCleanupTask<LakeServiceBrpcStubCache>> task);

    struct StubEntry {
        std::shared_ptr<LakeService_RecoverableStub> stub;
        std::shared_ptr<EndpointCleanupTask<LakeServiceBrpcStubCache>> cleanup_task;
    };

    SpinLock _lock;
    butil::FlatMap<butil::EndPoint, StubEntry> _stub_map;
    BthreadTimer* _timer;
    bool _stopping{false};
};
#endif

} // namespace starrocks
