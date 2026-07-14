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

#include "common/brpc/brpc_stub_cache.h"

#include "base/failpoint/fail_point.h"
#include "base/metrics.h"
#include "base/time/time.h"
#include "common/config_network_fwd.h"
#include "gen_cpp/internal_service.pb.h"
#ifndef __APPLE__
#include "gen_cpp/lake_service.pb.h"
#endif

namespace starrocks {

namespace {

const char* const kBrpcEndpointStubCountMetric = "brpc_endpoint_stub_count";

template <typename Cache>
Cache*& singleton_cache() {
    static Cache* cache = nullptr;
    return cache;
}

template <typename Cache>
std::mutex& singleton_cache_mutex() {
    static std::mutex mutex;
    return mutex;
}

inline int64_t absolute_deadline_us(int64_t ttl_seconds) {
    return butil::gettimeofday_us() + ttl_seconds * 1000 * 1000;
}
} // namespace

template <typename CacheT, typename ExtractFn>
void wait_clean_tasks_terminate(CacheT* cache, ExtractFn extract) {
    std::vector<std::shared_ptr<EndpointCleanupTask<CacheT>>> tasks;
    BthreadTimer* timer = nullptr;
    {
        std::lock_guard<SpinLock> l(cache->_lock);
        cache->_stopping = true;
        timer = cache->_timer;
        cache->_timer = nullptr;
        for (auto& stub : cache->_stub_map) {
            tasks.push_back(extract(stub.second));
        }
        cache->_stub_map.clear();
    }
    if (timer != nullptr) {
        for (auto& task : tasks) {
            task->unschedule_and_join(timer);
        }
    }
}

template <typename CacheT>
void reset_state_for_rebind(CacheT* cache, BthreadTimer* timer) {
    DCHECK(timer != nullptr);
    std::lock_guard<SpinLock> l(cache->_lock);
    DCHECK(cache->_stub_map.empty() || cache->_timer == timer);
    cache->_stopping = false;
    cache->_timer = timer;
}

struct BrpcStubCache::Metrics {
    Metrics(MetricRegistry* metric_registry, BrpcStubCache* cache) : registry(metric_registry), cache(cache) {
        DCHECK(registry != nullptr);
        registry->register_metric(kBrpcEndpointStubCountMetric, &brpc_endpoint_stub_count);
        registry->register_hook(kBrpcEndpointStubCountMetric, [this] {
            std::lock_guard<SpinLock> l(this->cache->_lock);
            brpc_endpoint_stub_count.set_value(this->cache->_stub_map.size());
        });
    }

    ~Metrics() {
        registry->deregister_hook(kBrpcEndpointStubCountMetric);
        brpc_endpoint_stub_count.hide();
    }

    MetricRegistry* registry;
    BrpcStubCache* cache;
    UIntGauge brpc_endpoint_stub_count{MetricUnit::NOUNIT};
};

BrpcStubCache::BrpcStubCache(BthreadTimer* timer, MetricRegistry* metric_registry) : _timer(timer) {
    _stub_map.init(239);
    if (metric_registry != nullptr) {
        _metrics = std::make_unique<Metrics>(metric_registry, this);
    }
}

BrpcStubCache::~BrpcStubCache() {
    _metrics.reset();
    wait_clean_tasks_terminate(this, [](const std::shared_ptr<StubPool>& pool) { return pool->_cleanup_task; });
}

bool BrpcStubCache::replace_cleanup_task_locked(
        const butil::EndPoint& endpoint,
        std::shared_ptr<EndpointCleanupTask<BrpcStubCache>> task) {
    auto pool = _stub_map.seek(endpoint);
    if (pool != nullptr) {
        (*pool)->_cleanup_task = std::move(task);
        return true;
    }
    return false;
}

std::shared_ptr<PInternalService_RecoverableStub> BrpcStubCache::get_stub(const butil::EndPoint& endpoint) {
    std::lock_guard<SpinLock> l(_lock);

    auto stub_pool = _stub_map.seek(endpoint);
    if (stub_pool == nullptr) {
        auto new_pool = std::make_shared<StubPool>();
        new_pool->_cleanup_task =
                std::make_shared<EndpointCleanupTask<BrpcStubCache>>(this, endpoint, config::brpc_stub_expire_s);
        new_pool->_cleanup_task->renew_deadline_locked(absolute_deadline_us(config::brpc_stub_expire_s));
        _stub_map.insert(endpoint, new_pool);
        stub_pool = _stub_map.seek(endpoint);

        timespec tm = butil::microseconds_to_timespec((*stub_pool)->_cleanup_task->deadline_locked());
        auto status = _timer->schedule((*stub_pool)->_cleanup_task.get(), tm);
        if (!status.ok()) {
            LOG(WARNING) << "Failed to schedule brpc cleanup task: " << endpoint;
            _stub_map.erase(endpoint);
            return new_pool->get_or_create(endpoint);
        }
    } else {
        (*stub_pool)->_cleanup_task->renew_deadline_locked(absolute_deadline_us(config::brpc_stub_expire_s));
    }

    return (*stub_pool)->get_or_create(endpoint);
}

std::shared_ptr<PInternalService_RecoverableStub> BrpcStubCache::get_stub(const TNetworkAddress& taddr) {
    return get_stub(taddr.hostname, taddr.port);
}

std::shared_ptr<PInternalService_RecoverableStub> BrpcStubCache::get_stub(const std::string& host, int port) {
    butil::EndPoint endpoint;
    std::string realhost;
    std::string brpc_url;
    realhost = host;
    if (!is_valid_ip(host)) {
        Status status = hostname_to_ip(host, realhost);
        if (!status.ok()) {
            LOG(WARNING) << "failed to get ip from host:" << status.to_string();
            return nullptr;
        }
    }
    brpc_url = get_host_port(realhost, port);
    if (str2endpoint(brpc_url.c_str(), &endpoint)) {
        LOG(WARNING) << "unknown endpoint, host=" << host;
        return nullptr;
    }
    return get_stub(endpoint);
}

BrpcStubCache::StubPool::StubPool() {
    _stubs.reserve(config::brpc_max_connections_per_server);
}

BrpcStubCache::StubPool::~StubPool() {
    _stubs.clear();
    _cleanup_task.reset();
}

std::shared_ptr<PInternalService_RecoverableStub> BrpcStubCache::StubPool::get_or_create(
        const butil::EndPoint& endpoint) {
    if (UNLIKELY(_stubs.size() < config::brpc_max_connections_per_server)) {
        auto stub = std::make_shared<PInternalService_RecoverableStub>(endpoint, "");
        if (!stub->reset_channel().ok()) {
            return nullptr;
        }
        _stubs.push_back(stub);
        return stub;
    }
    if (++_idx >= config::brpc_max_connections_per_server) {
        _idx = 0;
    }
    return _stubs[_idx];
}

void HttpBrpcStubCache::initialize(BthreadTimer* timer) {
    DCHECK(timer != nullptr);
    std::lock_guard<std::mutex> l(singleton_cache_mutex<HttpBrpcStubCache>());
    auto*& cache = singleton_cache<HttpBrpcStubCache>();
    if (cache == nullptr) {
        cache = new HttpBrpcStubCache(timer);
        return;
    }
    cache->bind_timer(timer);
}

HttpBrpcStubCache* HttpBrpcStubCache::getInstance() {
    return singleton_cache<HttpBrpcStubCache>();
}

HttpBrpcStubCache::HttpBrpcStubCache(BthreadTimer* timer) : _timer(timer) {
    _stub_map.init(500);
}

HttpBrpcStubCache::~HttpBrpcStubCache() {
    shutdown();
}

void HttpBrpcStubCache::bind_timer(BthreadTimer* timer) {
    reset_state_for_rebind(this, timer);
}

void HttpBrpcStubCache::shutdown() {
    wait_clean_tasks_terminate(this, [](const StubEntry& entry) { return entry.cleanup_task; });
}

bool HttpBrpcStubCache::replace_cleanup_task_locked(
        const butil::EndPoint& endpoint,
        std::shared_ptr<EndpointCleanupTask<HttpBrpcStubCache>> task) {
    auto entry = _stub_map.seek(endpoint);
    if (entry != nullptr) {
        entry->cleanup_task = std::move(task);
        return true;
    }
    return false;
}

StatusOr<std::shared_ptr<PInternalService_RecoverableStub>> HttpBrpcStubCache::get_http_stub(
        const TNetworkAddress& taddr) {
    butil::EndPoint endpoint;
    std::string realhost;
    std::string brpc_url;
    realhost = taddr.hostname;
    if (!is_valid_ip(taddr.hostname)) {
        Status status = hostname_to_ip(taddr.hostname, realhost);
        if (!status.ok()) {
            LOG(WARNING) << "failed to get ip from host:" << status.to_string();
            return nullptr;
        }
    }
    brpc_url = get_host_port(realhost, taddr.port);
    if (str2endpoint(brpc_url.c_str(), &endpoint)) {
        return Status::RuntimeError("unknown endpoint, host = " + taddr.hostname);
    }
    // get is exist
    std::lock_guard<SpinLock> l(_lock);
    if (_timer == nullptr) {
        return Status::ServiceUnavailable("HttpBrpcStubCache is not initialized");
    }

    auto stub_pair_ptr = _stub_map.seek(endpoint);
    if (stub_pair_ptr == nullptr) {
        // create
        auto new_task =
                std::make_shared<EndpointCleanupTask<HttpBrpcStubCache>>(this, endpoint, config::brpc_stub_expire_s);
        auto stub = std::make_shared<PInternalService_RecoverableStub>(endpoint, "http");
        if (!stub->reset_channel().ok()) {
            return Status::RuntimeError("init http brpc channel error on " + taddr.hostname + ":" +
                                        std::to_string(taddr.port));
        }
        new_task->renew_deadline_locked(absolute_deadline_us(config::brpc_stub_expire_s));
        _stub_map.insert(endpoint, StubEntry{stub, new_task});
        stub_pair_ptr = _stub_map.seek(endpoint);

        timespec tm = butil::microseconds_to_timespec(stub_pair_ptr->cleanup_task->deadline_locked());
        auto status = _timer->schedule(stub_pair_ptr->cleanup_task.get(), tm);
        if (!status.ok()) {
            LOG(WARNING) << "Failed to schedule brpc cleanup task: " << endpoint;
            _stub_map.erase(endpoint);
            return stub;
        }
    } else {
        stub_pair_ptr->cleanup_task->renew_deadline_locked(absolute_deadline_us(config::brpc_stub_expire_s));
    }

    return stub_pair_ptr->stub;
}

#ifndef __APPLE__

void LakeServiceBrpcStubCache::initialize(BthreadTimer* timer) {
    DCHECK(timer != nullptr);
    std::lock_guard<std::mutex> l(singleton_cache_mutex<LakeServiceBrpcStubCache>());
    auto*& cache = singleton_cache<LakeServiceBrpcStubCache>();
    if (cache == nullptr) {
        cache = new LakeServiceBrpcStubCache(timer);
        return;
    }
    cache->bind_timer(timer);
}

LakeServiceBrpcStubCache* LakeServiceBrpcStubCache::getInstance() {
    return singleton_cache<LakeServiceBrpcStubCache>();
}

LakeServiceBrpcStubCache::LakeServiceBrpcStubCache(BthreadTimer* timer) : _timer(timer) {
    _stub_map.init(500);
}

LakeServiceBrpcStubCache::~LakeServiceBrpcStubCache() {
    shutdown();
}

void LakeServiceBrpcStubCache::bind_timer(BthreadTimer* timer) {
    reset_state_for_rebind(this, timer);
}

void LakeServiceBrpcStubCache::shutdown() {
    wait_clean_tasks_terminate(this, [](const StubEntry& entry) { return entry.cleanup_task; });
}

bool LakeServiceBrpcStubCache::replace_cleanup_task_locked(
        const butil::EndPoint& endpoint,
        std::shared_ptr<EndpointCleanupTask<LakeServiceBrpcStubCache>> task) {
    auto entry = _stub_map.seek(endpoint);
    if (entry != nullptr) {
        entry->cleanup_task = std::move(task);
        return true;
    }
    return false;
}

DEFINE_FAIL_POINT(get_stub_return_nullptr);
StatusOr<std::shared_ptr<starrocks::LakeService_RecoverableStub>> LakeServiceBrpcStubCache::get_stub(
        const std::string& host, int port) {
    butil::EndPoint endpoint;
    std::string realhost;
    std::string brpc_url;
    realhost = host;
    if (!is_valid_ip(host)) {
        RETURN_IF_ERROR(hostname_to_ip(host, realhost));
    }
    brpc_url = get_host_port(realhost, port);
    if (str2endpoint(brpc_url.c_str(), &endpoint)) {
        return Status::RuntimeError("unknown endpoint, host = " + host);
    }
    // get if exist
    std::lock_guard<SpinLock> l(_lock);
    if (_timer == nullptr) {
        return Status::ServiceUnavailable("LakeServiceBrpcStubCache is not initialized");
    }

    auto stub_pair_ptr = _stub_map.seek(endpoint);
    FAIL_POINT_TRIGGER_EXECUTE(get_stub_return_nullptr, { stub_pair_ptr = nullptr; });
    if (stub_pair_ptr == nullptr) {
        // create
        auto stub = std::make_shared<starrocks::LakeService_RecoverableStub>(endpoint, "");
        auto new_task = std::make_shared<EndpointCleanupTask<LakeServiceBrpcStubCache>>(this, endpoint,
                                                                                        config::brpc_stub_expire_s);
        if (!stub->reset_channel().ok()) {
            return Status::RuntimeError("init lakeService brpc channel error on " + host + ":" + std::to_string(port));
        }
        new_task->renew_deadline_locked(absolute_deadline_us(config::brpc_stub_expire_s));
        _stub_map.insert(endpoint, StubEntry{stub, new_task});
        stub_pair_ptr = _stub_map.seek(endpoint);

        timespec tm = butil::microseconds_to_timespec(stub_pair_ptr->cleanup_task->deadline_locked());
        auto status = _timer->schedule(stub_pair_ptr->cleanup_task.get(), tm);
        if (!status.ok()) {
            LOG(WARNING) << "Failed to schedule brpc cleanup task: " << endpoint;
            _stub_map.erase(endpoint);
            return stub;
        }
    } else {
        stub_pair_ptr->cleanup_task->renew_deadline_locked(absolute_deadline_us(config::brpc_stub_expire_s));
    }

    return stub_pair_ptr->stub;
}

#endif

} // namespace starrocks
