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

#include "orchestration/runtime_filter_worker.h"

#include <google/protobuf/field_mask.pb.h>
#include <google/protobuf/util/field_mask_util.h>

#include <algorithm>
#include <utility>

#include "common/config_exec_flow_fwd.h"
#include "common/config_network_fwd.h"
#include "common/config_runtime_fwd.h"
#include "common/thread/thread.h"
#include "exec/runtime_filter_compat/runtime_filter_serde.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_filter_cache.h"
#include "runtime/service_contexts.h"
#include "runtime/time_guard.h"

namespace starrocks::orchestration {

RuntimeFilterWorker::RuntimeFilterWorker(const RuntimeServices* runtime_services, const RpcServices* rpc_services,
                                         MemTracker* query_pool_mem_tracker, FragmentMgr* fragment_mgr,
                                         int32_t num_threads)
        : _runtime_services(runtime_services),
          _rpc_services(rpc_services),
          _query_pool_mem_tracker(query_pool_mem_tracker),
          _delivery(runtime_services, rpc_services, fragment_mgr) {
    DCHECK(_runtime_services != nullptr);
    DCHECK(_rpc_services != nullptr);
    DCHECK(fragment_mgr != nullptr);
    _metrics = new RuntimeFilterWorkerMetrics();
    const size_t num_shards = std::max<int32_t>(num_threads, 1);
    for (size_t i = 0; i < num_shards; ++i) {
        _shards.emplace_back(std::make_unique<Shard>());
    }
    for (auto& shard : _shards) {
        shard->thread = std::thread([this, s = shard.get()] { _execute(s); });
        Thread::set_thread_name(shard->thread, "runtime_filter");
    }
}

RuntimeFilterWorker::~RuntimeFilterWorker() {
    if (_metrics) {
        delete _metrics;
    }
}

void RuntimeFilterWorker::close() {
    for (auto& shard : _shards) {
        shard->queue.shutdown();
    }
    for (auto& shard : _shards) {
        if (shard->thread.joinable()) {
            shard->thread.join();
        }
    }
}

size_t RuntimeFilterWorker::shard_index(const TUniqueId& query_id, int32_t filter_id, size_t num_shards) {
    if (num_shards <= 1) {
        return 0;
    }
    // Filter ids of a query are small consecutive numbers, so adding them spreads one query's filters
    // over the threads round-robin.
    const uint64_t query_hash = static_cast<uint64_t>(query_id.hi) ^ static_cast<uint64_t>(query_id.lo);
    return (query_hash + static_cast<uint32_t>(filter_id)) % num_shards;
}

void RuntimeFilterWorker::_put(RuntimeFilterWorkerEvent&& ev, int32_t filter_id) {
    _metrics->update_event_nums(ev.type, 1);
    _metrics->update_rf_bytes(ev.type, ev.transmit_rf_request.data().size());
    _shards[shard_index(ev.query_id, filter_id, _shards.size())]->queue.put(std::move(ev));
}

// Every thread keeps its own merger of the query, so the query's lifecycle events go to all threads. Once
// open_query returns, every thread has the OPEN_QUERY ahead of any rf event of the query put afterwards.
void RuntimeFilterWorker::_put_to_all_shards(RuntimeFilterWorkerEvent&& ev) {
    for (size_t i = 0; i + 1 < _shards.size(); ++i) {
        _metrics->update_event_nums(ev.type, 1);
        _shards[i]->queue.put(ev);
    }
    _metrics->update_event_nums(ev.type, 1);
    _shards.back()->queue.put(std::move(ev));
}

void RuntimeFilterWorker::open_query(const TUniqueId& query_id, const TQueryOptions& query_options,
                                     const TRuntimeFilterParams& params, bool is_pipeline) {
    VLOG_FILE << "RuntimeFilterWorker::open_query. query_id = " << query_id << ", params = " << params;
    // OPEN_QUERY is never dropped by the queue limit: without the merger, none of the query's partial filters can be
    // merged, so every global runtime filter of the query would be lost instead of just the ones over the limit.
    RuntimeFilterWorkerEvent ev;
    ev.type = OPEN_QUERY;
    ev.query_id = query_id;
    ev.query_options = query_options;
    ev.create_rf_merger_request = params;
    ev.is_opened_by_pipeline = is_pipeline;
    _put_to_all_shards(std::move(ev));
}

void RuntimeFilterWorker::close_query(const TUniqueId& query_id) {
    VLOG_FILE << "RuntimeFilterWorker::close_query. query_id = " << query_id;
    RuntimeFilterWorkerEvent ev;
    ev.type = CLOSE_QUERY;
    ev.query_id = query_id;
    _put_to_all_shards(std::move(ev));
}

// Over the queue limit, a partial runtime filter is replaced with a smaller one that passes every row, instead of being
// dropped: a dropped partial filter leaves its merged filter incomplete forever. The queued bytes still shrink. Return false if there is no smaller one, and the caller drops it as before.
static bool shrink_over_queue_limit(PTransmitRuntimeFilterParams* params, const char* what) {
    const size_t size = params->data().size();
    if (!RuntimeFilterSerde::shrink_to_pass_all(params->mutable_data())) {
        LOG(WARNING) << "runtime filter worker queue drop " << what << ", query_id=" << params->query_id()
                     << ", filter_id=" << params->filter_id();
        return false;
    }
    LOG(WARNING) << "runtime filter worker queue replaces " << what
                 << " with a filter that passes every row, query_id=" << params->query_id()
                 << ", filter_id=" << params->filter_id() << ", size=" << size << "->" << params->data().size();
    return true;
}

// Copy every field of `src` but its filter, which may be big.
static void copy_without_filter(const PTransmitRuntimeFilterParams& src, PTransmitRuntimeFilterParams* dst) {
    static const google::protobuf::FieldMask mask = [] {
        google::protobuf::FieldMask mask;
        const auto* descriptor = PTransmitRuntimeFilterParams::descriptor();
        for (int i = 0; i < descriptor->field_count(); i++) {
            if (descriptor->field(i)->number() != PTransmitRuntimeFilterParams::kDataFieldNumber) {
                mask.add_paths(descriptor->field(i)->name());
            }
        }
        return mask;
    }();
    google::protobuf::util::FieldMaskUtil::MergeMessageTo(src, mask, {}, dst);
}

bool RuntimeFilterWorker::_reach_queue_limit() {
    if (config::runtime_filter_queue_limit > 0) {
        const size_t size = queue_size();
        if (size > config::runtime_filter_queue_limit) {
            LOG(WARNING) << "runtime filter worker queue size is too large(" << size
                         << "), queue limit = " << config::runtime_filter_queue_limit;
            return true;
        }
    } else if (config::runtime_filter_queue_limit == 0) {
        int64_t mem_usage = _metrics->total_rf_bytes();
        auto* tracker = _query_pool_mem_tracker;
        if (tracker != nullptr && tracker->limit_exceeded_precheck(mem_usage)) {
            LOG(WARNING) << "runtime filter worker queue mem-useage is too large(" << mem_usage
                         << "), query pool consum(" << tracker->consumption() << "), limit(" << tracker->limit() << ")";
            return true;
        }
    }
    return false;
}

void RuntimeFilterWorker::send_part_runtime_filter(PTransmitRuntimeFilterParams&& params,
                                                   const std::vector<TNetworkAddress>& addrs, int timeout_ms,
                                                   int64_t rpc_http_min_size, EventType type) {
    if (_reach_queue_limit()) {
        // only a partial filter, see shrink_over_queue_limit. The skew broadcast material is not a runtime filter, and
        // another sender also sends it.
        if (type != SEND_PART_RF || !shrink_over_queue_limit(&params, "part runtime filter")) {
            return;
        }
    }
    _runtime_services->runtime_filter_cache->add_rf_event(
            {params.query_id(), params.filter_id(), "", EventTypeToString(type)});
    RuntimeFilterWorkerEvent ev;
    ev.type = type;
    ev.transmit_timeout_ms = timeout_ms;
    ev.transmit_via_http_min_size = rpc_http_min_size;
    ev.transmit_addrs = addrs;
    ev.transmit_rf_request = std::move(params);
    ev.query_id.hi = ev.transmit_rf_request.query_id().hi();
    ev.query_id.lo = ev.transmit_rf_request.query_id().lo();
    const int32_t filter_id = ev.transmit_rf_request.filter_id();
    _put(std::move(ev), filter_id);
}

void RuntimeFilterWorker::send_broadcast_runtime_filter(PTransmitRuntimeFilterParams&& params,
                                                        const std::vector<TRuntimeFilterDestination>& destinations,
                                                        int timeout_ms, int64_t rpc_http_min_size) {
    if (_reach_queue_limit()) {
        LOG(WARNING) << "runtime filter worker queue drop broadcast runtime filter, query_id=" << params.query_id()
                     << ", filter_id=" << params.filter_id();
        return;
    }
    _runtime_services->runtime_filter_cache->add_rf_event(
            {params.query_id(), params.filter_id(), "", "SEND_BROADCAST_RF"});
    RuntimeFilterWorkerEvent ev;
    ev.type = SEND_BROADCAST_GRF;
    ev.transmit_timeout_ms = timeout_ms;
    ev.transmit_via_http_min_size = rpc_http_min_size;
    ev.destinations = destinations;
    ev.transmit_rf_request = std::move(params);
    ev.query_id.hi = ev.transmit_rf_request.query_id().hi();
    ev.query_id.lo = ev.transmit_rf_request.query_id().lo();
    const int32_t filter_id = ev.transmit_rf_request.filter_id();
    _put(std::move(ev), filter_id);
}

void RuntimeFilterWorker::receive_runtime_filter(const PTransmitRuntimeFilterParams& params) {
    VLOG_FILE << "RuntimeFilterWorker::receive_runtime_filter: partial=" << params.is_partial()
              << ", query_id=" << params.query_id() << ", finst_id=" << params.finst_id()
              << ", filter_id=" << params.filter_id() << ", probe_size=" << params.probe_finst_ids_size()
              << ", is_pipeline=" << params.is_pipeline();

    const bool reach_queue_limit = _reach_queue_limit();
    // Only a partial filter is replaced over the queue limit: each build instance sends one, so nothing valid can
    // come after it. A total filter may have another valid copy (a broadcast filter has up to three senders) that a
    // pass-all filter, which can not be replaced once installed, could win over. The skew broadcast material is not a
    // runtime filter.
    if (reach_queue_limit && (params.is_skew_broadcast_join() || !params.is_partial())) {
        LOG(WARNING) << "runtime filter worker queue drop receive runtime filter, query_id=" << params.query_id()
                     << ", filter_id=" << params.filter_id();
        return;
    }
    RuntimeFilterWorkerEvent ev;
    if (params.is_skew_broadcast_join()) {
        _runtime_services->runtime_filter_cache->add_rf_event(
                {params.query_id(), params.filter_id(), "", "RECEIVE_SKEW_JOIN_BROADCAST_RF"});
        ev.type = RECEIVE_SKEW_JOIN_BROADCAST_RF;
    } else if (params.is_partial()) {
        _runtime_services->runtime_filter_cache->add_rf_event(
                {params.query_id(), params.filter_id(), "", "RECV_PART_RF"});
        ev.type = RECEIVE_PART_RF;
    } else {
        _runtime_services->runtime_filter_cache->add_rf_event(
                {params.query_id(), params.filter_id(), "", "RECV_TOTAL_RF"});
        ev.type = RECEIVE_TOTAL_RF;
    }
    if (params.has_transmit_timeout_ms()) {
        ev.transmit_timeout_ms = params.transmit_timeout_ms();
    } else {
        ev.transmit_timeout_ms = config::send_rpc_runtime_filter_timeout_ms;
    }
    if (params.has_transmit_via_http_min_size()) {
        ev.transmit_via_http_min_size = params.transmit_via_http_min_size();
    } else {
        ev.transmit_via_http_min_size = config::send_runtime_filter_via_http_rpc_min_size;
    }
    ev.query_id.hi = params.query_id().hi();
    ev.query_id.lo = params.query_id().lo();
    if (!reach_queue_limit) {
        ev.transmit_rf_request = params;
    } else {
        // shrink before copying the request, whose filter may be big.
        std::string pass_all_data;
        if (!RuntimeFilterSerde::to_pass_all(params.data(), &pass_all_data) ||
            pass_all_data.size() > params.data().size()) {
            LOG(WARNING) << "runtime filter worker queue drop receive runtime filter, query_id=" << params.query_id()
                         << ", filter_id=" << params.filter_id();
            return;
        }
        copy_without_filter(params, &ev.transmit_rf_request);
        ev.transmit_rf_request.set_data(std::move(pass_all_data));
        LOG(WARNING) << "runtime filter worker queue replaces receive runtime filter with a filter that passes every "
                        "row, query_id="
                     << params.query_id() << ", filter_id=" << params.filter_id() << ", size=" << params.data().size()
                     << "->" << ev.transmit_rf_request.data().size();
    }
    // A skew join's broadcast rf is merged into the shuffle join's rf, so it goes with the shuffle join's filter.
    const int32_t filter_id =
            ev.type == RECEIVE_SKEW_JOIN_BROADCAST_RF ? params.skew_shuffle_filter_id() : params.filter_id();
    _put(std::move(ev), filter_id);
}

void RuntimeFilterWorker::_execute(Shard* shard) {
    LOG(INFO) << "RuntimeFilterWorker start working.";
    auto& mergers = shard->mergers;
    for (;;) {
        RuntimeFilterWorkerEvent ev;
        if (!shard->queue.blocking_get(&ev)) {
            break;
        }
        DUMP_TRACE_IF_TIMEOUT(config::pipeline_rf_worker_timeout_guard_ms);

        _metrics->update_event_nums(ev.type, -1);
        switch (ev.type) {
        case RECEIVE_TOTAL_RF: {
            _metrics->update_rf_bytes(ev.type, -ev.transmit_rf_request.data().size());
            _delivery.receive_total_runtime_filter(ev.transmit_rf_request, ev.transmit_timeout_ms,
                                                   ev.transmit_via_http_min_size);
            break;
        }

        case CLOSE_QUERY: {
            auto it = mergers.find(ev.query_id);
            if (it != mergers.end()) {
                mergers.erase(it);
            }
            break;
        }

        case OPEN_QUERY: {
            auto it = mergers.find(ev.query_id);
            if (it != mergers.end()) {
                VLOG_QUERY << "open query: rf merger already existed. query_id = " << ev.query_id;
                break;
            }
            RuntimeFilterMerger merger(_runtime_services, _rpc_services, UniqueId(ev.query_id), ev.query_options,
                                       ev.is_opened_by_pipeline);
            Status st = merger.init(ev.create_rf_merger_request);
            if (!st.ok()) {
                VLOG_QUERY << "open query: rf merger initialization failed. error = " << st.message();
                break;
            }
            mergers.insert(std::make_pair(ev.query_id, std::move(merger)));
            break;
        }

        case RECEIVE_PART_RF: {
            _metrics->update_rf_bytes(ev.type, -ev.transmit_rf_request.data().size());
            auto it = mergers.find(ev.query_id);
            if (it == mergers.end()) {
                VLOG_QUERY << "receive part rf: rf merger not existed. query_id = " << ev.query_id;
                break;
            }
            RuntimeFilterMerger& merger = it->second;
            _runtime_services->runtime_filter_cache->add_rf_event(
                    {ev.transmit_rf_request.query_id(), ev.transmit_rf_request.filter_id(), "", "RECV_PART_RF_RPC"});
            merger.merge_runtime_filter(ev.transmit_rf_request);
            break;
        }

        case RECEIVE_SKEW_JOIN_BROADCAST_RF: {
            _metrics->update_rf_bytes(ev.type, -ev.transmit_rf_request.data().size());
            auto it = mergers.find(ev.query_id);
            if (it == mergers.end()) {
                VLOG_QUERY << "receive skew join broadcast rf: rf merger not existed. query_id = " << ev.query_id;
                break;
            }
            RuntimeFilterMerger& merger = it->second;
            _runtime_services->runtime_filter_cache->add_rf_event({ev.transmit_rf_request.query_id(),
                                                                   ev.transmit_rf_request.skew_shuffle_filter_id(), "",
                                                                   "RECEIVE_SKEW_JOIN_BROADCAST_RF"});
            merger.store_skew_broadcast_join_runtime_filter(ev.transmit_rf_request);
            break;
        }
        case SEND_SKEW_JOIN_BROADCAST_RF:
            _metrics->update_rf_bytes(ev.type, -ev.transmit_rf_request.data().size());
            _delivery.deliver_part_runtime_filter(std::move(ev.transmit_addrs), std::move(ev.transmit_rf_request),
                                                  ev.transmit_timeout_ms, ev.transmit_via_http_min_size,
                                                  "SEND_SKEW_BROADCAST_RF_RPC");
            break;
        case SEND_PART_RF: {
            _metrics->update_rf_bytes(ev.type, -ev.transmit_rf_request.data().size());
            _delivery.deliver_part_runtime_filter(std::move(ev.transmit_addrs), std::move(ev.transmit_rf_request),
                                                  ev.transmit_timeout_ms, ev.transmit_via_http_min_size,
                                                  "SEND_PART_RF_RPC");
            break;
        }
        case SEND_BROADCAST_GRF: {
            _metrics->update_rf_bytes(ev.type, -ev.transmit_rf_request.data().size());
            _delivery.process_send_broadcast_runtime_filter_event(std::move(ev.transmit_rf_request),
                                                                  std::move(ev.destinations), ev.transmit_timeout_ms,
                                                                  ev.transmit_via_http_min_size);
            break;
        }

        default:
            VLOG_QUERY << "unknown event type = " << ev.type;
            break;
        }
    }
    LOG(INFO) << "RuntimeFilterWorker going to exit.";
}

size_t RuntimeFilterWorker::queue_size() const {
    size_t size = 0;
    for (const auto& shard : _shards) {
        size += shard->queue.get_size();
    }
    return size;
}

} // namespace starrocks::orchestration
