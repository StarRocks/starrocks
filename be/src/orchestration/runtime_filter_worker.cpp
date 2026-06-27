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

#include <utility>

#include "common/config_exec_flow_fwd.h"
#include "common/config_network_fwd.h"
#include "common/config_runtime_fwd.h"
#include "common/thread/thread.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_filter_cache.h"
#include "runtime/service_contexts.h"
#include "runtime/time_guard.h"

namespace starrocks::orchestration {

RuntimeFilterWorker::RuntimeFilterWorker(const RuntimeServices* runtime_services, const RpcServices* rpc_services,
                                         MemTracker* query_pool_mem_tracker)
        : _runtime_services(runtime_services),
          _rpc_services(rpc_services),
          _query_pool_mem_tracker(query_pool_mem_tracker),
          _delivery(runtime_services, rpc_services),
          _thread([this] { execute(); }) {
    DCHECK(_runtime_services != nullptr);
    DCHECK(_rpc_services != nullptr);
    Thread::set_thread_name(_thread, "runtime_filter");
    _metrics = new RuntimeFilterWorkerMetrics();
}

RuntimeFilterWorker::~RuntimeFilterWorker() {
    if (_metrics) {
        delete _metrics;
    }
}

void RuntimeFilterWorker::close() {
    _queue.shutdown();
    _thread.join();
}

void RuntimeFilterWorker::open_query(const TUniqueId& query_id, const TQueryOptions& query_options,
                                     const TRuntimeFilterParams& params, bool is_pipeline) {
    VLOG_FILE << "RuntimeFilterWorker::open_query. query_id = " << query_id << ", params = " << params;
    if (_reach_queue_limit()) {
        LOG(WARNING) << "runtime filter worker queue drop open query_id = " << query_id;
        return;
    }
    RuntimeFilterWorkerEvent ev;
    ev.type = OPEN_QUERY;
    ev.query_id = query_id;
    ev.query_options = query_options;
    ev.create_rf_merger_request = params;
    ev.is_opened_by_pipeline = is_pipeline;
    _metrics->update_event_nums(ev.type, 1);
    _queue.put(std::move(ev));
}

void RuntimeFilterWorker::close_query(const TUniqueId& query_id) {
    VLOG_FILE << "RuntimeFilterWorker::close_query. query_id = " << query_id;
    RuntimeFilterWorkerEvent ev;
    ev.type = CLOSE_QUERY;
    ev.query_id = query_id;
    _metrics->update_event_nums(ev.type, 1);
    _queue.put(std::move(ev));
}

bool RuntimeFilterWorker::_reach_queue_limit() {
    if (config::runtime_filter_queue_limit > 0) {
        if (_queue.get_size() > config::runtime_filter_queue_limit) {
            LOG(WARNING) << "runtime filter worker queue size is too large(" << _queue.get_size()
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
        LOG(WARNING) << "runtime filter worker queue drop part runtime filter, query_id=" << params.query_id()
                     << ", filter_id=" << params.filter_id();
        return;
    }
    _runtime_services->runtime_filter_cache->add_rf_event(
            {params.query_id(), params.filter_id(), "", EventTypeToString(type)});
    RuntimeFilterWorkerEvent ev;
    ev.type = type;
    ev.transmit_timeout_ms = timeout_ms;
    ev.transmit_via_http_min_size = rpc_http_min_size;
    ev.transmit_addrs = addrs;
    ev.transmit_rf_request = std::move(params);
    _metrics->update_event_nums(ev.type, 1);
    _metrics->update_rf_bytes(ev.type, ev.transmit_rf_request.data().size());
    _queue.put(std::move(ev));
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
    _metrics->update_event_nums(ev.type, 1);
    _metrics->update_rf_bytes(ev.type, ev.transmit_rf_request.data().size());
    _queue.put(std::move(ev));
}

void RuntimeFilterWorker::receive_runtime_filter(const PTransmitRuntimeFilterParams& params) {
    VLOG_FILE << "RuntimeFilterWorker::receive_runtime_filter: partial=" << params.is_partial()
              << ", query_id=" << params.query_id() << ", finst_id=" << params.finst_id()
              << ", filter_id=" << params.filter_id() << ", probe_size=" << params.probe_finst_ids_size()
              << ", is_pipeline=" << params.is_pipeline();

    if (_reach_queue_limit()) {
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
    ev.transmit_rf_request = params;
    _metrics->update_event_nums(ev.type, 1);
    _metrics->update_rf_bytes(ev.type, ev.transmit_rf_request.data().size());
    _queue.put(std::move(ev));
}

void RuntimeFilterWorker::execute() {
    LOG(INFO) << "RuntimeFilterWorker start working.";
    for (;;) {
        RuntimeFilterWorkerEvent ev;
        if (!_queue.blocking_get(&ev)) {
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
            auto it = _mergers.find(ev.query_id);
            if (it != _mergers.end()) {
                _mergers.erase(it);
            }
            break;
        }

        case OPEN_QUERY: {
            auto it = _mergers.find(ev.query_id);
            if (it != _mergers.end()) {
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
            _mergers.insert(std::make_pair(ev.query_id, std::move(merger)));
            break;
        }

        case RECEIVE_PART_RF: {
            _metrics->update_rf_bytes(ev.type, -ev.transmit_rf_request.data().size());
            auto it = _mergers.find(ev.query_id);
            if (it == _mergers.end()) {
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
            auto it = _mergers.find(ev.query_id);
            if (it == _mergers.end()) {
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
    return _queue.get_size();
}

} // namespace starrocks::orchestration
