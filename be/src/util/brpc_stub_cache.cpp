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
#include "gen_cpp/lake_service.pb.h"
#include "runtime/exec_env.h"
#include "util/failpoint/fail_point.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

BrpcStubCache::BrpcStubCache(ExecEnv* exec_env) : _pipeline_timer(exec_env->pipeline_timer()) {
    _stub_map.init(239);
    REGISTER_GAUGE_STARROCKS_METRIC(brpc_endpoint_stub_count, [this]() {
        std::lock_guard<SpinLock> l(_lock);
        return _stub_map.size();
    });
}

BrpcStubCache::~BrpcStubCache() {
    std::vector<std::shared_ptr<StubPool>> pools_to_cleanup;
    {
        std::lock_guard<SpinLock> l(_lock);

        for (auto& stub : _stub_map) {
            pools_to_cleanup.push_back(stub.second);
        }
    }

    for (auto& pool : pools_to_cleanup) {
        pool->_cleanup_task->unschedule(_pipeline_timer);
    }

    _stub_map.clear();
}

std::shared_ptr<PInternalService_RecoverableStub> BrpcStubCache::get_stub(const butil::EndPoint& endpoint) {
    std::lock_guard<SpinLock> l(_lock);

    auto stub_pool = _stub_map.seek(endpoint);
    if (stub_pool == nullptr) {
        auto new_pool = std::make_shared<StubPool>();
        new_pool->_cleanup_task = new EndpointCleanupTask<BrpcStubCache>(this, endpoint);
        _stub_map.insert(endpoint, new_pool);
        stub_pool = _stub_map.seek(endpoint);
    }

    if (_pipeline_timer->unschedule((*stub_pool)->_cleanup_task) != TIMER_TASK_RUNNING) {
        timespec tm = butil::seconds_from_now(config::brpc_stub_expire_s);
        auto status = _pipeline_timer->schedule((*stub_pool)->_cleanup_task, tm);
        if (!status.ok()) {
            LOG(WARNING) << "Failed to schedule brpc cleanup task: " << endpoint;
        }
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

void BrpcStubCache::cleanup_expired(const butil::EndPoint& endpoint) {
    std::lock_guard<SpinLock> l(_lock);

    LOG(INFO) << "cleanup brpc stub, endpoint:" << endpoint;
    _stub_map.erase(endpoint);
}

BrpcStubCache::StubPool::StubPool() : _idx(-1) {
    _stubs.reserve(config::brpc_max_connections_per_server);
}

BrpcStubCache::StubPool::~StubPool() {
    _stubs.clear();
    SAFE_DELETE(_cleanup_task);
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
    std::vector<std::shared_ptr<EndpointCleanupTask<HttpBrpcStubCache>>> task_to_cleanup;

    {
        std::lock_guard<SpinLock> l(_lock);
        for (auto& stub : _stub_map) {
            task_to_cleanup.push_back(stub.second.second);
        }
    }

    for (auto& task : task_to_cleanup) {
        task->unschedule(_pipeline_timer);
    }

    _stub_map.clear();
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
        auto new_task = std::make_shared<EndpointCleanupTask<HttpBrpcStubCache>>(this, endpoint);
        auto stub = std::make_shared<PInternalService_RecoverableStub>(endpoint, "http");
        if (!stub->reset_channel().ok()) {
            return Status::RuntimeError("init http brpc channel error on " + taddr.hostname + ":" +
                                        std::to_string(taddr.port));
        }
        _stub_map.insert(endpoint, std::make_pair(stub, new_task));
        stub_pair_ptr = _stub_map.seek(endpoint);
    }

    // schedule clean up task
    if (_pipeline_timer->unschedule((*stub_pair_ptr).second.get()) != TIMER_TASK_RUNNING) {
        timespec tm = butil::seconds_from_now(config::brpc_stub_expire_s);
        auto status = _pipeline_timer->schedule((*stub_pair_ptr).second.get(), tm);
        if (!status.ok()) {
            LOG(WARNING) << "Failed to schedule http brpc cleanup task: " << endpoint;
        }
    }

    return (*stub_pair_ptr).first;
}

void HttpBrpcStubCache::cleanup_expired(const butil::EndPoint& endpoint) {
    std::lock_guard<SpinLock> l(_lock);

    LOG(INFO) << "cleanup http brpc stub, endpoint:" << endpoint;
    _stub_map.erase(endpoint);
}

LakeServiceBrpcStubCache* LakeServiceBrpcStubCache::getInstance() {
    static LakeServiceBrpcStubCache cache;
    return &cache;
}

LakeServiceBrpcStubCache::LakeServiceBrpcStubCache() {
    _stub_map.init(500);
    _pipeline_timer = ExecEnv::GetInstance()->pipeline_timer();
}

LakeServiceBrpcStubCache::~LakeServiceBrpcStubCache() {
    std::vector<std::shared_ptr<EndpointCleanupTask<LakeServiceBrpcStubCache>>> task_to_cleanup;

    {
        std::lock_guard<SpinLock> l(_lock);
        for (auto& stub : _stub_map) {
            task_to_cleanup.push_back(stub.second.second);
        }
    }

    for (auto& task : task_to_cleanup) {
        task->unschedule(_pipeline_timer);
    }

    _stub_map.clear();
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
        auto new_task = std::make_shared<EndpointCleanupTask<LakeServiceBrpcStubCache>>(this, endpoint);
        if (!stub->reset_channel().ok()) {
            return Status::RuntimeError("init lakeService brpc channel error on " + host + ":" + std::to_string(port));
        }
        _stub_map.insert(endpoint, std::make_pair(stub, new_task));
        stub_pair_ptr = _stub_map.seek(endpoint);
    }

    // schedule clean up task
    if (_pipeline_timer->unschedule((*stub_pair_ptr).second.get()) != TIMER_TASK_RUNNING) {
        timespec tm = butil::seconds_from_now(config::brpc_stub_expire_s);
        auto status = _pipeline_timer->schedule((*stub_pair_ptr).second.get(), tm);
        if (!status.ok()) {
            LOG(WARNING) << "Failed to schedule lake brpc cleanup task: " << endpoint;
        }
    }

    return (*stub_pair_ptr).first;
}

void LakeServiceBrpcStubCache::cleanup_expired(const butil::EndPoint& endpoint) {
    std::lock_guard<SpinLock> l(_lock);

    LOG(INFO) << "cleanup lake service brpc stub, endpoint:" << endpoint;
    _stub_map.erase(endpoint);
}

} // namespace starrocks