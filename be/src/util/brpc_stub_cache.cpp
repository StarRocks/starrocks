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
    _cleanup_thread = std::thread([this] {
#ifdef GOOGLE_PROFILER
        ProfilerRegisterThread();
#endif
        while (!_stopped.load()) {
            LOG(INFO) << "Start to clean up expired brpc stubs";
            check_and_cleanup_unhealthy_stubs();
            nap_sleep(config::brpc_stub_cleanup_interval_s, [this] { return _stopped.load(); });
        }
    });
    Thread::set_thread_name(_cleanup_thread, "brpc_cleanup_thread");
}

BrpcStubCache::~BrpcStubCache() {
    _stopped.store(true);
    _cleanup_thread.join();

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

void BrpcStubCache::check_and_cleanup_unhealthy_stubs() {
    std::vector<butil::EndPoint> snapshot;
    {
        std::lock_guard<SpinLock> l(_lock);

        for (auto& entry : _stub_map) {
            snapshot.push_back(entry.first);
        }
    }

    for (const auto& endpoint : snapshot) {
        auto stub_pool = get_stub_pool_readonly(endpoint);
        if (stub_pool && stub_pool->is_unhealthy()) {
            {
                std::lock_guard<SpinLock> l(_lock);

                LOG(INFO) << "cleanup stubs from endpoint:" << endpoint;
                _stub_map.erase(endpoint);
                delete stub_pool;
            }
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

bool BrpcStubCache::StubPool::is_unhealthy() {
    if (_stubs.size() == 0) {
        // When the vector is empty do not need to clean
        return false;
    }

    auto& first_stub = _stubs[0];
    if (!first_stub) {
        return true;
    }

    auto status = first_stub->check_health();
    return !status.ok();
}

BrpcStubCache::StubPool* BrpcStubCache::get_stub_pool_readonly(const butil::EndPoint& endpoint) {
    std::lock_guard<SpinLock> l(_lock);
    auto stub_ptr = _stub_map.seek(endpoint);
    return stub_ptr ? *stub_ptr : nullptr;
}

HttpBrpcStubCache* HttpBrpcStubCache::getInstance() {
    static HttpBrpcStubCache cache;
    return &cache;
}

HttpBrpcStubCache::HttpBrpcStubCache() {
    _stub_map.init(500);
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
    auto stub_ptr = _stub_map.seek(endpoint);
    if (stub_ptr != nullptr) {
        return *stub_ptr;
    }
    // create
    auto stub = std::make_shared<PInternalService_RecoverableStub>(endpoint, "http");
    if (!stub->reset_channel().ok()) {
        return Status::RuntimeError("init brpc http channel error on " + taddr.hostname + ":" +
                                    std::to_string(taddr.port));
    }
    _stub_map.insert(endpoint, stub);
    return stub;
}

LakeServiceBrpcStubCache* LakeServiceBrpcStubCache::getInstance() {
    static LakeServiceBrpcStubCache cache;
    return &cache;
}

LakeServiceBrpcStubCache::LakeServiceBrpcStubCache() {
    _stub_map.init(500);
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
    auto stub_ptr = _stub_map.seek(endpoint);
    FAIL_POINT_TRIGGER_EXECUTE(get_stub_return_nullptr, { stub_ptr = nullptr; });
    if (stub_ptr != nullptr) {
        return *stub_ptr;
    }
    // create
    auto stub = std::make_shared<starrocks::LakeService_RecoverableStub>(endpoint, "");
    if (!stub->reset_channel().ok()) {
        return Status::RuntimeError("init brpc http channel error on " + host + ":" + std::to_string(port));
    }
    _stub_map.insert(endpoint, stub);
    return stub;
}

} // namespace starrocks