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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/client_cache.cpp

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

#include "runtime/client_cache.h"

#include <thrift/protocol/TBinaryProtocol.h>

#include <memory>
#include <sstream>

#include "common/logging.h"
#include "gen_cpp/FrontendService.h"
#include "util/hash_util.hpp"
#include "util/network_util.h"

namespace starrocks {

// Hash function for TNetworkAddress. This function must be called hash_value to be picked
// up properly by STL.
inline std::size_t hash_value(const TNetworkAddress& host_port) {
    uint32_t hash = HashUtil::hash(host_port.hostname.c_str(), host_port.hostname.length(), 0);
    return HashUtil::hash(&host_port.port, sizeof(host_port.port), hash);
}

ClientCacheHelper::~ClientCacheHelper() {
    for (auto& it : _client_map) {
        delete it.second;
    }
}

Status ClientCacheHelper::get_client(const TNetworkAddress& hostport, const client_factory& factory_method,
                                     void** client_key, int timeout_ms) {
    std::lock_guard<std::mutex> lock(_lock);
    //VLOG_RPC << "get_client(" << hostport << ")";
    auto cache_entry = _client_cache.find(hostport);

    if (cache_entry == _client_cache.end()) {
        cache_entry = _client_cache.insert(std::make_pair(hostport, std::list<void*>())).first;
        DCHECK(cache_entry != _client_cache.end());
    }

    std::list<void*>& info_list = cache_entry->second;

    if (!info_list.empty()) {
        *client_key = info_list.front();
        VLOG_RPC << "get_client(): cached client for " << hostport;
        info_list.pop_front();
    } else {
        RETURN_IF_ERROR(create_client(hostport, factory_method, client_key, timeout_ms));
    }

    _client_map[*client_key]->set_send_timeout(timeout_ms);
    _client_map[*client_key]->set_recv_timeout(timeout_ms);

    if (_metrics_enabled) {
        _used_clients->increment(1);
    }

    return Status::OK();
}

Status ClientCacheHelper::reopen_client(const client_factory& factory_method, void** client_key, int timeout_ms) {
    std::lock_guard<std::mutex> lock(_lock);
    auto i = _client_map.find(*client_key);
    DCHECK(i != _client_map.end());
    ThriftClientImpl* info = i->second;
    const std::string ipaddress = info->ipaddress();
    int port = info->port();

    info->close();

    // TODO: Thrift TBufferedTransport cannot be re-opened after Close() because it does
    // not clean up internal buffers it reopens. To work around this issue, create a new
    // client instead.
    _client_map.erase(*client_key);
    delete info;
    *client_key = nullptr;

    if (_metrics_enabled) {
        _opened_clients->increment(-1);
    }

    Status status = create_client(make_network_address(ipaddress, port), factory_method, client_key, timeout_ms);
    if (!status.ok()) {
        if (_metrics_enabled) {
            _used_clients->increment(-1);
        }
        return status;
    }

    _client_map[*client_key]->set_send_timeout(timeout_ms);
    _client_map[*client_key]->set_recv_timeout(timeout_ms);
    return Status::OK();
}

Status ClientCacheHelper::create_client(const TNetworkAddress& hostport, const client_factory& factory_method,
                                        void** client_key, int timeout_ms) {
    (void)timeout_ms;
    std::unique_ptr<ThriftClientImpl> client_impl(factory_method(hostport, client_key));
    //VLOG_CONNECTION << "create_client(): adding new client for "
    //                << client_impl->ipaddress() << ":" << client_impl->port();

    client_impl->set_conn_timeout(config::thrift_connect_timeout_seconds * 1000);

    Status status = client_impl->open();

    if (!status.ok()) {
        *client_key = nullptr;
        return status;
    }

    // Because the client starts life 'checked out', we don't add it to the cache map
    _client_map[*client_key] = client_impl.release();

    if (_metrics_enabled) {
        _opened_clients->increment(1);
    }

    return Status::OK();
}

void ClientCacheHelper::release_client(void** client_key) {
    DCHECK(*client_key != nullptr) << "Trying to release NULL client";
    std::lock_guard<std::mutex> lock(_lock);

    auto client_map_entry = _client_map.find(*client_key);
    if (client_map_entry == _client_map.end()) {
        *client_key = nullptr;
        return;
    }

    ThriftClientImpl* info = client_map_entry->second;
    auto iter = _client_cache.find(make_network_address(info->ipaddress(), info->port()));
    if (iter == _client_cache.end()) {
        *client_key = nullptr;
        return;
    }

    if (_max_cache_size_per_host >= 0 && iter->second.size() >= _max_cache_size_per_host) {
        // cache of this host is full, close this client connection and remove if from _client_map
        info->close();
        _client_map.erase(*client_key);
        delete info;

        if (_metrics_enabled) {
            _opened_clients->increment(-1);
        }
    } else {
        iter->second.push_back(*client_key);
    }

    if (_metrics_enabled) {
        _used_clients->increment(-1);
    }

    *client_key = nullptr;
}

void ClientCacheHelper::close_connections(const TNetworkAddress& hostport) {
    std::lock_guard<std::mutex> lock(_lock);
    auto cache_entry = _client_cache.find(hostport);
    if (cache_entry == _client_cache.end()) {
        return;
    }

    auto& client_keys = cache_entry->second;
    for (void* client_key : client_keys) {
        auto client_map_entry = _client_map.find(client_key);
        DCHECK(client_map_entry != _client_map.end());
        ThriftClientImpl* info = client_map_entry->second;
        info->close();
        _client_map.erase(client_key);
        delete info;
    }
    _client_cache.erase(cache_entry);
}

std::string ClientCacheHelper::debug_string() {
    std::stringstream out;
    out << "ClientCacheHelper(#hosts=" << _client_cache.size() << " [";

    for (auto i = _client_cache.begin(); i != _client_cache.end(); ++i) {
        if (i != _client_cache.begin()) {
            out << " ";
        }

        out << i->first << ":" << i->second.size();
    }

    out << "])";
    return out.str();
}

void ClientCacheHelper::init_metrics(MetricRegistry* metrics, const std::string& key_prefix) {
    DCHECK(metrics != nullptr);
    // Not strictly needed if init_metrics is called before any cache
    // usage, but ensures that _metrics_enabled is published.
    std::lock_guard<std::mutex> lock(_lock);

    _used_clients = std::make_unique<IntGauge>(MetricUnit::NOUNIT);
    metrics->register_metric("thrift_used_clients", MetricLabels().add("name", key_prefix), _used_clients.get());

    _opened_clients = std::make_unique<IntGauge>(MetricUnit::NOUNIT);
    metrics->register_metric("thrift_opened_clients", MetricLabels().add("name", key_prefix), _opened_clients.get());
    _metrics_enabled = true;
}

} // namespace starrocks
