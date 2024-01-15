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

#include "common/config.h"
#include "common/statusor.h"
#include "gen_cpp/Types_types.h" // TNetworkAddress
#include "gen_cpp/doris_internal_service.pb.h"
#include "gen_cpp/internal_service.pb.h"
#include "service/brpc.h"
#include "util/network_util.h"
#include "util/spinlock.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

// map used
class BrpcStubCache {
public:
    BrpcStubCache() {
        _stub_map.init(239);
        REGISTER_GAUGE_STARROCKS_METRIC(brpc_endpoint_stub_count, [this]() {
            std::lock_guard<SpinLock> l(_lock);
            return _stub_map.size();
        });
    }
    ~BrpcStubCache() {
        for (auto& stub : _stub_map) {
            delete stub.second;
        }
    }

    PInternalService_Stub* get_stub(const butil::EndPoint& endpoint) {
        std::lock_guard<SpinLock> l(_lock);
        auto stub_pool = _stub_map.seek(endpoint);
        if (stub_pool == nullptr) {
            StubPool* pool = new StubPool();
            _stub_map.insert(endpoint, pool);
            return pool->get_or_create(endpoint);
        }
        return (*stub_pool)->get_or_create(endpoint);
    }

    PInternalService_Stub* get_stub(const TNetworkAddress& taddr) { return get_stub(taddr.hostname, taddr.port); }

    PInternalService_Stub* get_stub(const std::string& host, int port) {
        butil::EndPoint endpoint;
        std::string realhost;
        realhost = host;
        if (!is_valid_ip(host)) {
            realhost = hostname_to_ip(host);
            if (realhost == "") {
                LOG(WARNING) << "failed to get ip from host, host=" << host;
                return nullptr;
            }
        }
        if (str2endpoint(realhost.c_str(), port, &endpoint)) {
            LOG(WARNING) << "unknown endpoint, host=" << host;
            return nullptr;
        }
        return get_stub(endpoint);
    }

private:
    // StubPool is used to store all stubs with a single endpoint, and the client in the same BE process maintains up to
    // brpc_max_connections_per_server single connections with each server.
    // These connections will be created during the first few accesses and will be reused later.
    struct StubPool {
        StubPool() { _stubs.reserve(config::brpc_max_connections_per_server); }

        ~StubPool() {
            for (auto& stub : _stubs) {
                delete stub;
            }
        }

        PInternalService_Stub* get_or_create(const butil::EndPoint& endpoint) {
            if (UNLIKELY(_stubs.size() < config::brpc_max_connections_per_server)) {
                brpc::ChannelOptions options;
                options.connect_timeout_ms = config::rpc_connect_timeout_ms;
                // Explicitly set the max_retry
                // TODO(meegoo): The retry strategy can be customized in the future
                options.max_retry = 3;
                // the single connection of brpc will only maintain one connection with the same server by default,
                // all requests are sent on this connection and the throughput will be limited by this.
                // we use `connection_group` to create multiple single connections to remove this bottleneck.
                options.connection_group = std::to_string(_stubs.size());
                std::unique_ptr<brpc::Channel> channel(new brpc::Channel());
                if (channel->Init(endpoint, &options)) {
                    return nullptr;
                }
                auto stub = new PInternalService_Stub(channel.release(), google::protobuf::Service::STUB_OWNS_CHANNEL);
                _stubs.push_back(stub);
                return stub;
            }
            if (++_idx >= config::brpc_max_connections_per_server) {
                _idx = 0;
            }
            return _stubs[_idx];
        }

        std::vector<PInternalService_Stub*> _stubs;
        int64_t _idx = -1;
    };

    SpinLock _lock;
    butil::FlatMap<butil::EndPoint, StubPool*> _stub_map;
};

class HttpBrpcStubCache {
public:
    static HttpBrpcStubCache* getInstance() {
        static HttpBrpcStubCache cache;
        return &cache;
    }

    StatusOr<PInternalService_Stub*> get_http_stub(const TNetworkAddress& taddr) {
        butil::EndPoint endpoint;
        std::string realhost;
        realhost = taddr.hostname;
        if (!is_valid_ip(taddr.hostname)) {
            realhost = hostname_to_ip(taddr.hostname);
            if (realhost == "") {
                return Status::RuntimeError("failed to get ip from host " + taddr.hostname);
            }
        }
        if (str2endpoint(realhost.c_str(), taddr.port, &endpoint)) {
            return Status::RuntimeError("unknown endpoint, host = " + taddr.hostname);
        }
        // get is exist
        std::lock_guard<SpinLock> l(_lock);
        auto stub_ptr = _stub_map.seek(endpoint);
        if (stub_ptr != nullptr) {
            return *stub_ptr;
        }
        // create
        brpc::ChannelOptions options;
        options.connect_timeout_ms = config::rpc_connect_timeout_ms;
        options.protocol = "http";
        // Explicitly set the max_retry
        // TODO(meegoo): The retry strategy can be customized in the future
        options.max_retry = 3;
        std::unique_ptr<brpc::Channel> channel(new brpc::Channel());
        if (channel->Init(endpoint, &options)) {
            return Status::RuntimeError("init brpc http channel error on " + taddr.hostname + ":" +
                                        std::to_string(taddr.port));
        }
        auto stub = new PInternalService_Stub(channel.release(), google::protobuf::Service::STUB_OWNS_CHANNEL);
        _stub_map.insert(endpoint, stub);
        return stub;
    }

private:
    HttpBrpcStubCache() { _stub_map.init(500); }
    ~HttpBrpcStubCache() {
        for (auto& stub : _stub_map) {
            delete stub.second;
        }
    }
    HttpBrpcStubCache(const HttpBrpcStubCache& cache) = delete;
    HttpBrpcStubCache& operator=(const HttpBrpcStubCache& cache) = delete;

    SpinLock _lock;
    butil::FlatMap<butil::EndPoint, PInternalService_Stub*> _stub_map;
};

} // namespace starrocks
