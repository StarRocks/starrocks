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
#include <memory>
#include <thread>
#include <unordered_map>
#include <vector>

#include "base/concurrency/blocking_queue.hpp"
#include "exec/runtime_filter_compat/runtime_filter_merger.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "orchestration/runtime_filter_delivery.h"
#include "orchestration/runtime_filter_worker_event.h"
#include "runtime/runtime_filter_query_lifecycle.h"
#include "runtime/runtime_filter_sender.h"

namespace starrocks {
struct RuntimeServices;
struct RpcServices;
class MemTracker;

namespace orchestration {

class FragmentMgr;

class RuntimeFilterWorker : public RuntimeFilterQueryLifecycle, public RuntimeFilterSender {
public:
    // Events of the same runtime filter always go to the same thread, so a merger is only touched by one
    // thread. num_threads <= 1 keeps the single-threaded behavior.
    RuntimeFilterWorker(const RuntimeServices* runtime_services, const RpcServices* rpc_services,
                        MemTracker* query_pool_mem_tracker, FragmentMgr* fragment_mgr, int32_t num_threads);
    ~RuntimeFilterWorker();
    void close();
    // open query for creating runtime filter merger.
    void open_query(const TUniqueId& query_id, const TQueryOptions& query_options, const TRuntimeFilterParams& params,
                    bool is_pipeline) override;
    void close_query(const TUniqueId& query_id) override;
    void receive_runtime_filter(const PTransmitRuntimeFilterParams& params);
    void send_part_runtime_filter(PTransmitRuntimeFilterParams&& params,
                                  const std::vector<starrocks::TNetworkAddress>& addrs, int timeout_ms,
                                  int64_t rpc_http_min_size, EventType type) override;
    void send_broadcast_runtime_filter(PTransmitRuntimeFilterParams&& params,
                                       const std::vector<TRuntimeFilterDestination>& destinations, int timeout_ms,
                                       int64_t rpc_http_min_size) override;

    size_t queue_size() const;
    size_t num_threads() const { return _shards.size(); }
    const RuntimeFilterWorkerMetrics* metrics() const { return _metrics; }

    // The thread that handles the runtime filter. A skew join's broadcast rf is merged into the shuffle
    // join's rf, so it must be routed by skew_shuffle_filter_id.
    static size_t shard_index(const TUniqueId& query_id, int32_t filter_id, size_t num_shards);

private:
    // One thread with its own queue and the mergers of the runtime filters routed to it.
    struct Shard {
        UnboundedBlockingQueue<RuntimeFilterWorkerEvent> queue;
        std::unordered_map<TUniqueId, RuntimeFilterMerger> mergers;
        std::thread thread;
    };

    bool _reach_queue_limit();
    void _execute(Shard* shard);
    void _put(RuntimeFilterWorkerEvent&& ev, int32_t filter_id);
    void _put_to_all_shards(RuntimeFilterWorkerEvent&& ev);

    std::vector<std::unique_ptr<Shard>> _shards;
    const RuntimeServices* _runtime_services;
    const RpcServices* _rpc_services;
    MemTracker* _query_pool_mem_tracker;
    RuntimeFilterDelivery _delivery;
    RuntimeFilterWorkerMetrics* _metrics = nullptr;
};

} // namespace orchestration
} // namespace starrocks
