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

#include "util/brpc_stub_cache.h"

#include "common/config.h"
#include "gen_cpp/internal_service.pb.h"
#ifndef __APPLE__
#include "gen_cpp/lake_service.pb.h"
#endif
#include "runtime/exec_env.h"
#include "util/failpoint/fail_point.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

namespace {

inline int64_t absolute_deadline_us(int64_t ttl_seconds) {
    return butil::gettimeofday_us() + ttl_seconds * 1000 * 1000;
}

} // namespace

template <typename CacheT, typename ExtractFn>
void wait_clean_tasks_terminate(CacheT* cache, ExtractFn extract) {
    std::vector<std::shared_ptr<EndpointCleanupTask<CacheT>>> tasks;
    {
        std::lock_guard<SpinLock> l(cache->_lock);
        cache->_stopping = true;
        for (auto& stub : cache->_stub_map) {
            tasks.push_back(extract(stub.second));
        }
        cache->_stub_map.clear();
    }
    if (cache->_pipeline_timer != nullptr) {
        for (auto& task : tasks) {
            (void)cache->_pipeline_timer->unschedule(task.get());
        }
    }
}

BrpcStubCache::BrpcStubCache(ExecEnv* exec_env) : _pipeline_timer(exec_env->pipeline_timer()) {
    _stub_map.init(239);
    REGISTER_GAUGE_STARROCKS_METRIC(brpc_endpoint_stub_count, [this]() {
        std::lock_guard<SpinLock> l(_lock);
        return _stub_map.size();
    });
}

BrpcStubCache::~BrpcStubCache() {
    wait_clean_tasks_terminate(this, [](const std::shared_ptr<StubPool>& pool) { return pool->_cleanup_task; });
}

bool BrpcStubCache::replace_cleanup_task_locked(const butil::EndPoint& endpoint,
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
        auto status = _pipeline_timer->schedule((*stub_pool)->_cleanup_task.get(), tm);
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

BrpcStubCache::StubPool::StubPool() : _idx(-1) {
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

HttpBrpcStubCache* HttpBrpcStubCache::getInstance() {
    static HttpBrpcStubCache cache;
    return &cache;
}

HttpBrpcStubCache::HttpBrpcStubCache() {
    _stub_map.init(500);
    _pipeline_timer = ExecEnv::GetInstance()->pipeline_timer();
}

HttpBrpcStubCache::~HttpBrpcStubCache() {
    shutdown();
}

void HttpBrpcStubCache::shutdown() {
    wait_clean_tasks_terminate(this, [](const StubEntry& entry) { return entry.cleanup_task; });
}

bool HttpBrpcStubCache::replace_cleanup_task_locked(const butil::EndPoint& endpoint,
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
        auto status = _pipeline_timer->schedule(stub_pair_ptr->cleanup_task.get(), tm);
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

LakeServiceBrpcStubCache* LakeServiceBrpcStubCache::getInstance() {
    static LakeServiceBrpcStubCache cache;
    return &cache;
}

LakeServiceBrpcStubCache::LakeServiceBrpcStubCache() {
    _stub_map.init(500);
    _pipeline_timer = ExecEnv::GetInstance()->pipeline_timer();
}

LakeServiceBrpcStubCache::~LakeServiceBrpcStubCache() {
    shutdown();
}

void LakeServiceBrpcStubCache::shutdown() {
    wait_clean_tasks_terminate(this, [](const StubEntry& entry) { return entry.cleanup_task; });
}

bool LakeServiceBrpcStubCache::replace_cleanup_task_locked(
        const butil::EndPoint& endpoint, std::shared_ptr<EndpointCleanupTask<LakeServiceBrpcStubCache>> task) {
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
        auto status = _pipeline_timer->schedule(stub_pair_ptr->cleanup_task.get(), tm);
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
