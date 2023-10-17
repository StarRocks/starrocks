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

#include <atomic>
#include <map>
#include <memory>
#include <thread>
#include <vector>

#include "common/global_types.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "util/blocking_queue.hpp"
#include "util/ref_count_closure.h"
#include "util/uid_util.h"
namespace starrocks {

class ExecEnv;
class RuntimeState;

class JoinRuntimeFilter;
class RuntimeFilterProbeDescriptor;
class RuntimeFilterBuildDescriptor;

using RuntimeFilterRpcClosure = RefCountClosure<PTransmitRuntimeFilterResult>;
using RuntimeFilterRpcClosures = std::vector<RuntimeFilterRpcClosure*>;
// RuntimeFilterPort is bind to a fragment instance
// and it's to exchange RF(publish/receive) with outside world.
class RuntimeFilterPort {
public:
    RuntimeFilterPort(RuntimeState* state) : _state(state) {}
    void add_listener(RuntimeFilterProbeDescriptor* rf_desc);
    void publish_runtime_filters(std::list<RuntimeFilterBuildDescriptor*>& rf_descs);
    // receiver runtime filter allocated in this fragment instance(broadcast join generate it)
    // or allocated in this query(shuffle join generate global runtime filter)
    void receive_runtime_filter(int32_t filter_id, const JoinRuntimeFilter* rf);
    void receive_shared_runtime_filter(int32_t filter_id, const std::shared_ptr<const JoinRuntimeFilter>& rf);
    std::string listeners(int32_t filter_id);

private:
    std::map<int32_t, std::list<RuntimeFilterProbeDescriptor*>> _listeners;
    RuntimeState* _state;
};

class RuntimeFilterMergerStatus {
public:
    RuntimeFilterMergerStatus() = default;
    RuntimeFilterMergerStatus(RuntimeFilterMergerStatus&& other) noexcept
            : arrives(std::move(other.arrives)),
              expect_number(other.expect_number),
              pool(std::move(other.pool)),
              filters(std::move(other.filters)),
              current_size(other.current_size),
              max_size(other.max_size),
              stop(other.stop),
              recv_first_filter_ts(other.recv_first_filter_ts),
              recv_last_filter_ts(other.recv_last_filter_ts),
              broadcast_filter_ts(other.broadcast_filter_ts) {}
    // which be number send this rf.
    std::unordered_set<int32_t> arrives;
    // how many partitioned rf we expect
    int32_t expect_number;
    ObjectPool pool;
    // each partitioned rf.
    std::map<int32_t, JoinRuntimeFilter*> filters;
    size_t current_size = 0;
    size_t max_size = 0;
    bool stop = false;
    bool ignore_bf = false;

    // statistics.
    // timestamp in ms since unix epoch;
    // we care about diff not abs value.
    int64_t recv_first_filter_ts = 0;
    int64_t recv_last_filter_ts = 0;
    int64_t broadcast_filter_ts = 0;
};

// RuntimeFilterMerger is to merge partitioned RF
// and sent merged RF to consumer nodes.
class RuntimeFilterMerger {
public:
    RuntimeFilterMerger(ExecEnv* env, const UniqueId& query_id, const TQueryOptions& query_options, bool is_pipeline);
    Status init(const TRuntimeFilterParams& params);
    void merge_runtime_filter(PTransmitRuntimeFilterParams& params);

private:
    void _send_total_runtime_filter(int rf_version, int32_t filter_id);
    // filter_id -> where this filter should send to
    std::map<int32_t, std::vector<TRuntimeFilterProberParams>> _targets;
    std::map<int32_t, RuntimeFilterMergerStatus> _statuses;
    ExecEnv* _exec_env;
    UniqueId _query_id;
    TQueryOptions _query_options;
    const bool _is_pipeline;
};

// RuntimeFilterWorker works in a separated thread, and does following jobs:
// 1. deserialize runtime filters.
// 2. merge runtime filters.

// it works in a event-driven way, and possible events are:
// - create a runtime filter merger for a query
// - receive partitioned RF, deserialize it and merge it, and sent total RF(for merge node)
// - receive total RF and send it to RuntimeFilterPort
// - send partitioned RF(for hash join node)
// - close a query(delete runtime filter merger)
struct RuntimeFilterWorkerEvent;
class RuntimeFilterWorker {
public:
    RuntimeFilterWorker(ExecEnv* env);
    ~RuntimeFilterWorker();
    // open query for creating runtime filter merger.
    void open_query(const TUniqueId& query_id, const TQueryOptions& query_options, const TRuntimeFilterParams& params,
                    bool is_pipeline);
    void close_query(const TUniqueId& query_id);
    void receive_runtime_filter(const PTransmitRuntimeFilterParams& params);
    void execute();
    void send_part_runtime_filter(PTransmitRuntimeFilterParams&& params,
                                  const std::vector<starrocks::TNetworkAddress>& addrs, int timeout_ms);
    void send_broadcast_runtime_filter(PTransmitRuntimeFilterParams&& params,
                                       const std::vector<TRuntimeFilterDestination>& destinations, int timeout_ms);

private:
    void _receive_total_runtime_filter(PTransmitRuntimeFilterParams& params);
    void _process_send_broadcast_runtime_filter_event(PTransmitRuntimeFilterParams&& params,
                                                      std::vector<TRuntimeFilterDestination>&& destinations,
                                                      int timeout_ms);
    void _deliver_broadcast_runtime_filter_passthrough(PTransmitRuntimeFilterParams&& params,
                                                       std::vector<TRuntimeFilterDestination>&& destinations,
                                                       int timeout_ms);
    void _deliver_broadcast_runtime_filter_relay(PTransmitRuntimeFilterParams&& params,
                                                 std::vector<TRuntimeFilterDestination>&& destinations, int timeout_ms);
    void _deliver_broadcast_runtime_filter_local(PTransmitRuntimeFilterParams& params,
                                                 const TRuntimeFilterDestination& destinations);

    void _deliver_part_runtime_filter(std::vector<TNetworkAddress>&& transmit_addrs,
                                      PTransmitRuntimeFilterParams&& params, int transmit_timeout_ms);

    UnboundedBlockingQueue<RuntimeFilterWorkerEvent> _queue;
    std::unordered_map<TUniqueId, RuntimeFilterMerger> _mergers;
    ExecEnv* _exec_env;
    std::thread _thread;
};

}; // namespace starrocks
