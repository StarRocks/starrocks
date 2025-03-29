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

#include <column/column.h>
#include <types/logical_type.h>

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <thread>
#include <vector>

#include "common/object_pool.h"
#include "common/status.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "util/blocking_queue.hpp"
#include "util/ref_count_closure.h"
#include "util/uid_util.h"
namespace starrocks {
struct TypeDescriptor;

class ExecEnv;
class RuntimeState;

class RuntimeFilter;
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
    void publish_runtime_filters(const std::list<RuntimeFilterBuildDescriptor*>& rf_descs);

    void publish_runtime_filters_for_skew_broadcast_join(const std::list<RuntimeFilterBuildDescriptor*>& rf_descs_list,
                                                         const std::vector<Columns>& keyColumns,
                                                         const std::vector<bool>& null_safe,
                                                         const std::vector<TypeDescriptor>& type_descs);

    void publish_local_colocate_filters(std::list<RuntimeFilterBuildDescriptor*>& rf_descs);
    // receiver runtime filter allocated in this fragment instance(broadcast join generate it)
    // or allocated in this query(shuffle join generate global runtime filter)
    void receive_runtime_filter(int32_t filter_id, const RuntimeFilter* rf);
    void receive_shared_runtime_filter(int32_t filter_id, const std::shared_ptr<const RuntimeFilter>& rf);
    std::string listeners(int32_t filter_id);

private:
    void publish_skew_boradcast_join_key_columns(RuntimeFilterBuildDescriptor* rf_desc, const ColumnPtr& keyColumn,
                                                 bool null_safe, const TypeDescriptor& type_desc);
    void static prepare_params(PTransmitRuntimeFilterParams& params, RuntimeState* state,
                               RuntimeFilterBuildDescriptor* rf_desc);
    std::map<int32_t, std::list<RuntimeFilterProbeDescriptor*>> _listeners;
    RuntimeState* _state;
};

struct SkewBroadcastRfMaterial {
    LogicalType build_type;
    bool eq_null;
    ColumnPtr key_column;
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
              is_skew_join(other.is_skew_join),
              recv_first_filter_ts(other.recv_first_filter_ts),
              recv_last_filter_ts(other.recv_last_filter_ts),
              broadcast_filter_ts(other.broadcast_filter_ts) {}
    // merge skew_broadcast_rf_material into out's _hash_partition_bf
    Status _merge_skew_broadcast_runtime_filter(RuntimeFilter* out);
    // which be number send this rf.
    std::unordered_set<int32_t> arrives;
    // how many partitioned rf we expect
    int32_t expect_number;
    ObjectPool pool;
    // each partitioned rf.
    std::map<int32_t, RuntimeFilter*> filters;
    size_t current_size = 0;
    size_t max_size = 0;
    bool stop = false;
    bool can_use_bf = true;
    bool is_skew_join;

    // only set when skew join optimization is used
    SkewBroadcastRfMaterial* skew_broadcast_rf_material = nullptr;
    // statistics.
    // timestamp in ms since unix epoch;
    // we care about diff not abs value.
    int64_t recv_first_filter_ts = 0;
    int64_t recv_last_filter_ts = 0;
    int64_t broadcast_filter_ts = 0;

    // only send once
    bool isSent = false;
};

// RuntimeFilterMerger is to merge partitioned RF
// and sent merged RF to consumer nodes.
class RuntimeFilterMerger {
public:
    RuntimeFilterMerger(ExecEnv* env, const UniqueId& query_id, const TQueryOptions& query_options, bool is_pipeline);
    Status init(const TRuntimeFilterParams& params);
    void merge_runtime_filter(PTransmitRuntimeFilterParams& params);
    void store_skew_broadcast_join_runtime_filter(PTransmitRuntimeFilterParams& params);

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

enum EventType {
    RECEIVE_TOTAL_RF = 0,
    CLOSE_QUERY = 1,
    OPEN_QUERY = 2,
    RECEIVE_PART_RF = 3,
    SEND_PART_RF = 4,
    SEND_BROADCAST_GRF = 5,
    SEND_SKEW_JOIN_BROADCAST_RF = 6,
    RECEIVE_SKEW_JOIN_BROADCAST_RF = 7,
    MAX_COUNT
};

inline std::string EventTypeToString(EventType type) {
    switch (type) {
    case RECEIVE_TOTAL_RF:
        return "RECEIVE_TOTAL_RF";
    case CLOSE_QUERY:
        return "CLOSE_QUERY";
    case OPEN_QUERY:
        return "OPEN_QUERY";
    case RECEIVE_PART_RF:
        return "RECEIVE_PART_RF";
    case SEND_SKEW_JOIN_BROADCAST_RF:
        return "SEND_SKEW_JOIN_BROADCAST_RF";
    case SEND_PART_RF:
        return "SEND_PART_RF";
    case SEND_BROADCAST_GRF:
        return "SEND_BROADCAST_GRF";
    case RECEIVE_SKEW_JOIN_BROADCAST_RF:
        return "RECEIVE_SKEW_JOIN_BROADCAST_RF";
    default:
        break;
    }
    __builtin_unreachable();
}
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

struct RuntimeFilterWorkerMetrics {
    void update_event_nums(EventType event_type, int64_t delta) { event_nums[event_type] += delta; }

    void update_rf_bytes(EventType event_type, int64_t delta) { runtime_filter_bytes[event_type] += delta; }

    int64_t total_rf_bytes() {
        int64_t total = 0;
        for (int i = 0; i < EventType::MAX_COUNT; i++) {
            total += runtime_filter_bytes[i];
        }
        return total;
    }

    std::array<std::atomic_int64_t, EventType::MAX_COUNT> event_nums{};
    std::array<std::atomic_int64_t, EventType::MAX_COUNT> runtime_filter_bytes{};
};

class RuntimeFilterWorker {
public:
    RuntimeFilterWorker(ExecEnv* env);
    ~RuntimeFilterWorker();
    void close();
    // open query for creating runtime filter merger.
    void open_query(const TUniqueId& query_id, const TQueryOptions& query_options, const TRuntimeFilterParams& params,
                    bool is_pipeline);
    void close_query(const TUniqueId& query_id);
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
    void _receive_total_runtime_filter(PTransmitRuntimeFilterParams& params);
    void _process_send_broadcast_runtime_filter_event(PTransmitRuntimeFilterParams&& params,
                                                      std::vector<TRuntimeFilterDestination>&& destinations,
                                                      int timeout_ms, int64_t rpc_http_min_size);
    void _deliver_broadcast_runtime_filter_passthrough(PTransmitRuntimeFilterParams&& params,
                                                       std::vector<TRuntimeFilterDestination>&& destinations,
                                                       int timeout_ms, int64_t rpc_http_min_size);
    void _deliver_broadcast_runtime_filter_relay(PTransmitRuntimeFilterParams&& params,
                                                 std::vector<TRuntimeFilterDestination>&& destinations, int timeout_ms,
                                                 int64_t rpc_http_min_size);
    void _deliver_broadcast_runtime_filter_local(PTransmitRuntimeFilterParams& params,
                                                 const TRuntimeFilterDestination& destinations);

    void _deliver_part_runtime_filter(std::vector<TNetworkAddress>&& transmit_addrs,
                                      PTransmitRuntimeFilterParams&& params, int transmit_timeout_ms,
                                      int64_t rpc_http_min_size, const std::string& msg);

    bool _reach_queue_limit();

    UnboundedBlockingQueue<RuntimeFilterWorkerEvent> _queue;
    std::unordered_map<TUniqueId, RuntimeFilterMerger> _mergers;
    ExecEnv* _exec_env;
    std::thread _thread;
    RuntimeFilterWorkerMetrics* _metrics = nullptr;
};

}; // namespace starrocks
