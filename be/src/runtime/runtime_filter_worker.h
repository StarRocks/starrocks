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

#pragma once

#include <cstdint>
#include <thread>
#include <unordered_map>
#include <vector>

#include "base/concurrency/blocking_queue.hpp"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/runtime_filter_delivery.h"
#include "runtime/runtime_filter_merger.h"
#include "runtime/runtime_filter_port.h"
#include "runtime/runtime_filter_query_lifecycle.h"
#include "runtime/runtime_filter_worker_event.h"

namespace starrocks {

struct RuntimeServices;
struct RpcServices;

class RuntimeFilterWorker : public RuntimeFilterQueryLifecycle {
public:
    RuntimeFilterWorker(const RuntimeServices* runtime_services, const RpcServices* rpc_services);
    ~RuntimeFilterWorker();
    void close();
    // open query for creating runtime filter merger.
    void open_query(const TUniqueId& query_id, const TQueryOptions& query_options, const TRuntimeFilterParams& params,
                    bool is_pipeline);
    void close_query(const TUniqueId& query_id) override;
    void receive_runtime_filter(const PTransmitRuntimeFilterParams& params);
    void execute();
    void send_part_runtime_filter(PTransmitRuntimeFilterParams&& params,
                                  const std::vector<starrocks::TNetworkAddress>& addrs, int timeout_ms,
                                  int64_t rpc_http_min_size, EventType type);
    void send_broadcast_runtime_filter(PTransmitRuntimeFilterParams&& params,
                                       const std::vector<TRuntimeFilterDestination>& destinations, int timeout_ms,
                                       int64_t rpc_http_min_size);

    size_t queue_size() const;
    const RuntimeFilterWorkerMetrics* metrics() const { return _metrics; }

private:
    bool _reach_queue_limit();

    UnboundedBlockingQueue<RuntimeFilterWorkerEvent> _queue;
    std::unordered_map<TUniqueId, RuntimeFilterMerger> _mergers;
    const RuntimeServices* _runtime_services;
    const RpcServices* _rpc_services;
    RuntimeFilterDelivery _delivery;
    std::thread _thread;
    RuntimeFilterWorkerMetrics* _metrics = nullptr;
};

}; // namespace starrocks
