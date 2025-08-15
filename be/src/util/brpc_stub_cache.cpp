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
#include "util/misc.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

BrpcStubCache::BrpcStubCache() {
    _stub_map.init(239);
    REGISTER_GAUGE_STARROCKS_METRIC(brpc_endpoint_stub_count, [this]() {
        std::lock_guard<SpinLock> l(_lock);
        return _stub_map.size();
    });
}

BrpcStubCache::~BrpcStubCache() {
    for (auto& stub : _stub_map) {
        delete stub.second;
    }

    _stub_map.clear();
}

std::shared_ptr<PInternalService_RecoverableStub> BrpcStubCache::get_stub(const butil::EndPoint& endpoint) {
    std::lock_guard<SpinLock> l(_lock);
    auto stub_pool = _stub_map.seek(endpoint);
    if (stub_pool == nullptr) {
        StubPool* pool = new StubPool();
        _stub_map.insert(endpoint, pool);
        return pool->get_or_create(endpoint);
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

void BrpcStubCache::check_and_cleanup_expired_stubs(int32_t timeout_ms) {
    std::lock_guard<SpinLock> l(_lock);

    std::vector<butil::EndPoint> expired_endpoints;
    for (auto& entry : _stub_map) {
        auto& endpoint = entry.first;
        auto stub_pool = entry.second;
        if (stub_pool && stub_pool->is_expired(timeout_ms)) {
            expired_endpoints.push_back(endpoint);
        }
    }

    for (const auto& endpoint : expired_endpoints) {
        LOG(INFO) << "cleanup stubs from endpoint:" << endpoint;
        auto stub_pool = _stub_map.seek(endpoint);
        if (stub_pool != nullptr) {
            _stub_map.erase(endpoint);
            delete *stub_pool;
        }
    }
}

BrpcStubCache::StubPool::StubPool() : _idx(-1) {
    _stubs.reserve(config::brpc_max_connections_per_server);
}

BrpcStubCache::StubPool::~StubPool() {
    _stubs.clear();
}

std::shared_ptr<PInternalService_RecoverableStub> BrpcStubCache::StubPool::get_or_create(
        const butil::EndPoint& endpoint) {
    _last_access_time = std::chrono::steady_clock::now();

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

bool BrpcStubCache::StubPool::is_expired(int32_t timeout_ms) {
    auto now = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(now - _last_access_time);
    return duration.count() >= timeout_ms;
}

HttpBrpcStubCache* HttpBrpcStubCache::getInstance() {
    static HttpBrpcStubCache cache;
    return &cache;
}

HttpBrpcStubCache::HttpBrpcStubCache() {
    _stub_map.init(500);
    _last_access_time_map.init(500);
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
    auto now = std::chrono::steady_clock::now();
    auto stub_ptr = _stub_map.seek(endpoint);
    if (stub_ptr != nullptr) {
        // Update last access time
        _last_access_time_map[endpoint] = now;
        return *stub_ptr;
    }
    // create
    auto stub = std::make_shared<PInternalService_RecoverableStub>(endpoint, "http");
    if (!stub->reset_channel().ok()) {
        return Status::RuntimeError("init brpc http channel error on " + taddr.hostname + ":" +
                                    std::to_string(taddr.port));
    }
    _stub_map.insert(endpoint, stub);
    _last_access_time_map.insert(endpoint, now);
    return stub;
}

void HttpBrpcStubCache::check_and_cleanup_expired_stubs(int32_t timeout_ms) {
    std::lock_guard<SpinLock> l(_lock);

    std::vector<butil::EndPoint> expired_endpoints;
    auto now = std::chrono::steady_clock::now();
    for (auto& entry : _last_access_time_map) {
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(now - entry.second);
        if (duration.count() >= timeout_ms) {
            expired_endpoints.push_back(entry.first);
        }
    }

    for (const auto& endpoint : expired_endpoints) {
        LOG(INFO) << "cleanup http stub from endpoint:" << endpoint;
        _stub_map.erase(endpoint);
        _last_access_time_map.erase(endpoint);
    }
}

LakeServiceBrpcStubCache* LakeServiceBrpcStubCache::getInstance() {
    static LakeServiceBrpcStubCache cache;
    return &cache;
}

LakeServiceBrpcStubCache::LakeServiceBrpcStubCache() {
    _stub_map.init(500);
    _last_access_time_map.init(500);
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
    auto now = std::chrono::steady_clock::now();
    auto stub_ptr = _stub_map.seek(endpoint);
    FAIL_POINT_TRIGGER_EXECUTE(get_stub_return_nullptr, { stub_ptr = nullptr; });
    if (stub_ptr != nullptr) {
        // Update last access time
        _last_access_time_map[endpoint] = now;
        return *stub_ptr;
    }
    // create
    auto stub = std::make_shared<starrocks::LakeService_RecoverableStub>(endpoint, "");
    if (!stub->reset_channel().ok()) {
        return Status::RuntimeError("init brpc http channel error on " + host + ":" + std::to_string(port));
    }
    _stub_map.insert(endpoint, stub);
    _last_access_time_map.insert(endpoint, now);
    return stub;
}

void LakeServiceBrpcStubCache::check_and_cleanup_expired_stubs(int32_t timeout_ms) {
    std::lock_guard<SpinLock> l(_lock);

    std::vector<butil::EndPoint> expired_endpoints;
    auto now = std::chrono::steady_clock::now();
    for (auto& entry : _last_access_time_map) {
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(now - entry.second);
        if (duration.count() >= timeout_ms) {
            expired_endpoints.push_back(entry.first);
        }
    }

    for (const auto& endpoint : expired_endpoints) {
        LOG(INFO) << "cleanup lake service stub from endpoint:" << endpoint;
        _stub_map.erase(endpoint);
        _last_access_time_map.erase(endpoint);
    }
}

BrpcStubManager::BrpcStubManager(ExecEnv* exec_env) : _exec_env(exec_env) {
    _cleanup_thread = std::thread([this] {
#ifdef GOOGLE_PROFILER
        ProfilerRegisterThread();
#endif
        while (!_is_stopped.load()) {
            LOG(INFO) << "Start to clean up expired brpc stubs";
            int64_t stub_expire_ms = config::brpc_stub_expire_ms;
            int64_t cleanup_interval_s = config::brpc_stub_cleanup_interval_s;
            _exec_env->brpc_stub_cache()->check_and_cleanup_expired_stubs(stub_expire_ms);
            HttpBrpcStubCache::getInstance()->check_and_cleanup_expired_stubs(stub_expire_ms);
            LakeServiceBrpcStubCache::getInstance()->check_and_cleanup_expired_stubs(stub_expire_ms);
            nap_sleep(cleanup_interval_s, [this] { return _is_stopped.load(); });
        }
    });
    Thread::set_thread_name(_cleanup_thread, "brpc_cleanup_thread");
}

BrpcStubManager::~BrpcStubManager() {
    _is_stopped.store(true);
    if (_cleanup_thread.joinable()) {
        _cleanup_thread.join();
    }
}

} // namespace starrocks
