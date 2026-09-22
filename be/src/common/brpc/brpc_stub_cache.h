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

#include <algorithm>
#include <memory>
#include <mutex>
#include <vector>

#include "base/brpc/brpc.h"
#include "base/concurrency/spinlock.h"
#include "base/network/network_util.h"
#include "base/time/time.h"
#include "common/brpc/internal_service_recoverable_stub.h"
#include "common/bthread_timer.h"
#include "common/config_network_fwd.h"
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
    EndpointCleanupTask(StubCacheT* cache, const butil::EndPoint& endpoint) : _cache(cache), _endpoint(endpoint) {}
    // The actual cleanup/renewal decision must run while the cache's _lock is held so
    // that _stopping and _last_use_us are observed atomically with the cache state.
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
        int64_t idle_us = now_us - _last_use_us;
        bool expired = idle_us >= seconds_to_us(config::brpc_stub_expire_s);
        bool retire_unhealthy = !expired && unhealthy_tier_enabled() &&
                                idle_us >= seconds_to_us(config::brpc_unhealthy_stub_expire_s) &&
                                _cache->is_endpoint_unhealthy_locked(_endpoint);
        if (expired || retire_unhealthy) {
            const char* reason = retire_unhealthy ? "all channels in brpc's failed state" : "reached idle expire";
            int32_t window_s = retire_unhealthy ? config::brpc_unhealthy_stub_expire_s : config::brpc_stub_expire_s;
            LOG(INFO) << "cleanup brpc stub, endpoint:" << _endpoint << ", idle for " << idle_us / 1000
                      << "ms, reason: " << reason << ", window " << window_s << "s";
            _cache->_stub_map.erase(_endpoint);
            return;
        }
        auto new_task = std::make_shared<EndpointCleanupTask<StubCacheT>>(_cache, _endpoint);
        new_task->_last_use_us = _last_use_us;
        if (!_cache->replace_cleanup_task_locked(_endpoint, new_task)) {
            return;
        }
        timespec tm = butil::microseconds_to_timespec(next_fire_time_locked(now_us));
        auto status = _cache->_timer->schedule(new_task.get(), tm);
        if (!status.ok()) {
            LOG(WARNING) << "Failed to reschedule brpc cleanup task: " << _endpoint;
            // Drop the entry; the next get_*_stub() will recreate it with a fresh task.
            _cache->_stub_map.erase(_endpoint);
        }
    }

    // Reset the idle clock (in butil::gettimeofday_us() units). Caller must hold the
    // cache lock.
    void renew_locked(int64_t now_us) { _last_use_us = now_us; }

    int64_t next_fire_time_locked(int64_t now_us) const {
        int64_t hard_deadline = _last_use_us + seconds_to_us(config::brpc_stub_expire_s);
        if (!unhealthy_tier_enabled()) {
            return hard_deadline;
        }
        int64_t early_deadline = _last_use_us + seconds_to_us(config::brpc_unhealthy_stub_expire_s);
        if (now_us < early_deadline) {
            return early_deadline;
        }
        // Idle long enough to qualify but still healthy. Health can change, so keep
        // re-evaluating until the unconditional TTL runs out. The cadence is clamped
        // to a second so that a zero or negative config cannot spin the timer.
        return std::min(now_us + seconds_to_us(std::max<int64_t>(config::brpc_unhealthy_stub_expire_s, 1)),
                        hard_deadline);
    }

private:
    static int64_t seconds_to_us(int64_t seconds) { return seconds * 1000 * 1000; }

    // Configuring the unhealthy window at or above the unconditional one turns the
    // rule off and restores single-tier behaviour exactly.
    static bool unhealthy_tier_enabled() { return config::brpc_unhealthy_stub_expire_s < config::brpc_stub_expire_s; }

    StubCacheT* _cache;
    butil::EndPoint _endpoint;
    // Time of the most recent lookup for this endpoint, in butil::gettimeofday_us()
    // units. Read/written only under the cache's _lock, so it does not need to be
    // atomic.
    int64_t _last_use_us{0};
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

    bool is_cleanup_task_owner_locked(const butil::EndPoint& endpoint,
                                      const EndpointCleanupTask<BrpcStubCache>* task) const {
        auto pool = _stub_map.seek(endpoint);
        return pool != nullptr && (*pool)->_cleanup_task.get() == task;
    }

    // True only when every channel in the pool is in brpc's failed state, so that a
    // pool with just one usable channel is not retired early.
    bool is_endpoint_unhealthy_locked(const butil::EndPoint& endpoint) const {
        auto pool = _stub_map.seek(endpoint);
        if (pool == nullptr || (*pool)->_stubs.empty()) {
            return false;
        }
        for (const auto& stub : (*pool)->_stubs) {
            if (!stub->channel_failed()) {
                return false;
            }
        }
        return true;
    }

    bool replace_cleanup_task_locked(const butil::EndPoint& endpoint,
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

    bool is_cleanup_task_owner_locked(const butil::EndPoint& endpoint,
                                      const EndpointCleanupTask<HttpBrpcStubCache>* task) const {
        auto entry = _stub_map.seek(endpoint);
        return entry != nullptr && entry->cleanup_task.get() == task;
    }

    bool is_endpoint_unhealthy_locked(const butil::EndPoint& endpoint) const {
        auto entry = _stub_map.seek(endpoint);
        return entry != nullptr && entry->stub != nullptr && entry->stub->channel_failed();
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

    bool is_cleanup_task_owner_locked(const butil::EndPoint& endpoint,
                                      const EndpointCleanupTask<LakeServiceBrpcStubCache>* task) const {
        auto entry = _stub_map.seek(endpoint);
        return entry != nullptr && entry->cleanup_task.get() == task;
    }

    bool is_endpoint_unhealthy_locked(const butil::EndPoint& endpoint) const {
        auto entry = _stub_map.seek(endpoint);
        return entry != nullptr && entry->stub != nullptr && entry->stub->channel_failed();
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
};
#endif

} // namespace starrocks
