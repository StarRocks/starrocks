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

#include "platform/broker_mgr.h"

#include <sstream>

#include "common/config_network_fwd.h"
#include "common/system/backend_options.h"
#include "common/thread/thread.h"
#include "common/util/misc.h"
#include "common/util/thrift_client_cache.h"
#include "gen_cpp/FileBrokerService_types.h"
#include "gen_cpp/TFileBrokerService.h"
#include "platform/thrift_rpc_helper.h"

namespace starrocks {

static constexpr const char* kBrokerCountMetricName = "broker_count";

BrokerMgr::BrokerMgr() = default;

BrokerMgr::~BrokerMgr() {
    if (_metrics != nullptr) {
        _metrics->deregister_hook(kBrokerCountMetricName);
        _broker_count.hide();
    }
    _thread_stop.store(true);
    if (_ping_thread.joinable()) {
        _ping_thread.join();
    }
}

Status BrokerMgr::init(MetricRegistry* metrics) {
    if (_metrics == nullptr && metrics != nullptr) {
        if (!metrics->register_metric(kBrokerCountMetricName, &_broker_count)) {
            return Status::InternalError("register broker_count metric failed");
        }
        if (!metrics->register_hook(kBrokerCountMetricName, [this] { _broker_count.set_value(broker_count()); })) {
            _broker_count.hide();
            return Status::InternalError("register broker_count metric hook failed");
        }
        _metrics = metrics;
    }

    std::stringstream ss;
    ss << BackendOptions::get_localhost() << ":" << config::be_port;
    _client_id = ss.str();

    if (!_ping_thread.joinable()) {
        _thread_stop.store(false);
        _ping_thread = std::thread(&BrokerMgr::ping_worker, this);
        Thread::set_thread_name(_ping_thread, "broker_hrtbeat"); // broker heart beat
    }
    return Status::OK();
}

const std::string& BrokerMgr::get_client_id(const TNetworkAddress& address) {
    std::lock_guard<std::mutex> l(_mutex);
    _broker_set.insert(address);
    return _client_id;
}

size_t BrokerMgr::broker_count() const {
    std::lock_guard<std::mutex> l(_mutex);
    return _broker_set.size();
}

void BrokerMgr::ping(const TNetworkAddress& addr) {
    TBrokerPingBrokerRequest request;

    request.__set_version(TBrokerVersion::VERSION_ONE);
    request.__set_clientId(_client_id);

    TBrokerOperationStatus response;
    Status rpc_status;

    // 500ms is enough
    rpc_status = ThriftRpcHelper::rpc<TFileBrokerServiceClient>(
            addr, [&response, &request](BrokerServiceConnection& client) { client->ping(response, request); }, 500);
    if (!rpc_status.ok()) {
        LOG(WARNING) << "Broker ping failed, broker:" << addr << " failed:" << rpc_status;
    }
}

void BrokerMgr::ping_worker() {
    while (!_thread_stop.load()) {
        std::vector<TNetworkAddress> addresses;
        {
            std::lock_guard<std::mutex> l(_mutex);
            for (auto& addr : _broker_set) {
                addresses.emplace_back(addr);
            }
        }
        for (auto& addr : addresses) {
            ping(addr);
        }
        nap_sleep(5, [this] { return _thread_stop.load(); });
    }
}

} // namespace starrocks
