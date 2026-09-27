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

#include "orchestration/runtime_filter_delivery.h"

#include <algorithm>
#include <random>
#include <utility>

#include "base/uid_util.h"
#include "common/config_network_fwd.h"
#include "common/config_runtime_fwd.h"
#include "common/system/backend_options.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/runtime/fragment_context_manager.h"
#include "exec/runtime/query_context.h"
#include "exec/runtime/query_context_manager.h"
#include "exec/runtime_filter_compat/runtime_filter_port.h"
#include "exec/runtime_filter_compat/runtime_filter_rpc.h"
#include "exec/runtime_filter_compat/runtime_filter_serde.h"
#include "exec/runtime_filter_compat/runtime_filter_worker_context.h"
#include "gutil/strings/substitute.h"
#include "orchestration/fragment_mgr.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_filter.h"
#include "runtime/runtime_filter_cache.h"
#include "runtime/service_contexts.h"

namespace starrocks::orchestration {

// receive total runtime filter in pipeline engine.
static inline void receive_total_runtime_filter_pipeline(const RuntimeServices* runtime_services,
                                                         PTransmitRuntimeFilterParams& params,
                                                         const std::shared_ptr<RuntimeFilter>& shared_rf) {
    auto& pb_query_id = params.query_id();
    TUniqueId query_id;
    query_id.hi = pb_query_id.hi();
    query_id.lo = pb_query_id.lo();
    runtime_services->runtime_filter_cache->add_rf_event(
            {params.query_id(), params.filter_id(), BackendOptions::get_localhost(), "RECV_TOTAL_RF_RPC_PIPELINE"});
    auto query_ctx = runtime_services->query_context_mgr->get(query_id);
    // query_ctx is absent means that the query is finished or any fragments have not arrived, so
    // we conservatively consider that global rf arrives in advance, so cache it for later use.
    if (!query_ctx) {
        runtime_services->runtime_filter_cache->put_if_absent(query_id, params.filter_id(), shared_rf);
        runtime_services->runtime_filter_cache->add_rf_event({params.query_id(), params.filter_id(),
                                                              BackendOptions::get_localhost(),
                                                              "PUT_TOTAL_RF_IN_CACHE_QUERY_NOT_READY"});
    }
    // race condition exists among rf caching, FragmentContext's registration and OperatorFactory's preparation
    query_ctx = runtime_services->query_context_mgr->get(query_id);
    if (!query_ctx) {
        return;
    }
    // the query is already finished, so it is needless to cache rf.
    if (query_ctx->has_no_active_instances() || query_ctx->query_runtime_state().is_query_expired()) {
        return;
    }

    auto& probe_finst_ids = params.probe_finst_ids();
    for (const auto& pb_finst_id : probe_finst_ids) {
        TUniqueId finst_id;
        finst_id.hi = pb_finst_id.hi();
        finst_id.lo = pb_finst_id.lo();
        auto fragment_ctx = query_ctx->fragment_mgr()->get(finst_id);

        // fragment_ctx is absent means that the fragment instance is finished, or it has not arrived, so
        // we conservatively consider that global rf arrives in advance, so cache it for later use.
        if (!fragment_ctx) {
            runtime_services->runtime_filter_cache->put_if_absent(query_id, params.filter_id(), shared_rf);
            runtime_services->runtime_filter_cache->add_rf_event({params.query_id(), params.filter_id(),
                                                                  BackendOptions::get_localhost(),
                                                                  "PUT_TOTAL_RF_IN_CACHE_FRAGMENT_INSTANCE_NOT_READY"});
        }
        // race condition exists among rf caching, FragmentContext's registration and OperatorFactory's preparation
        fragment_ctx = query_ctx->fragment_mgr()->get(finst_id);
        if (!fragment_ctx) {
            continue;
        }
        // FragmentContext is already destructed or invalid, so do nothing.
        if (fragment_ctx->runtime_state()->is_cancelled()) {
            continue;
        }
        fragment_ctx->runtime_filter_port()->receive_shared_runtime_filter(params.filter_id(), shared_rf);
        runtime_services->runtime_filter_cache->add_rf_event(
                {params.query_id(), params.filter_id(), BackendOptions::get_localhost(),
                 strings::Substitute("INSTALL_GRF(num_waiters=$0, instance_id=$1)",
                                     fragment_ctx->runtime_filter_port()->listeners(params.filter_id()),
                                     print_id(finst_id))});
    }
}

void RuntimeFilterDelivery::receive_total_runtime_filter(PTransmitRuntimeFilterParams& request, int timeout_ms,
                                                         int64_t rpc_http_min_size) {
    auto [query_ctx, mem_tracker] =
            get_runtime_filter_mem_tracker(_runtime_services, request.query_id(), request.is_pipeline());
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(mem_tracker.get());
    // deserialize once, and all fragment instance shared that runtime filter.
    RuntimeFilter* rf = nullptr;
    const std::string& data = request.data();
    RuntimeFilterSerde::deserialize(nullptr, &rf, reinterpret_cast<const uint8_t*>(data.data()), data.size());
    if (rf == nullptr) {
        return;
    }

    if (rf->type() != RuntimeFilterSerializeType::IN_FILTER) {
        rf->get_membership_filter()->set_global();
    }

    std::shared_ptr<RuntimeFilter> shared_rf(rf);
    // for pipeline engine
    if (request.has_is_pipeline() && request.is_pipeline()) {
        receive_total_runtime_filter_pipeline(_runtime_services, request, shared_rf);
    } else {
        DCHECK(_fragment_mgr != nullptr);
        _fragment_mgr->receive_runtime_filter(request, shared_rf);
    }

    // not enough, have to forward this request to continue broadcast.
    // copy modified fields out.
    std::vector<PTransmitRuntimeFilterForwardTarget> targets;
    size_t size = request.forward_targets_size();
    for (size_t i = 0; i < size; i++) {
        const auto& fwd = request.forward_targets(i);
        targets.emplace_back(fwd);
    }

    size_t index = 0;
    while (index < size) {
        auto& t = targets[index];
        TNetworkAddress addr;
        addr.hostname = t.host();
        addr.port = t.port();

        request.clear_probe_finst_ids();
        request.clear_forward_targets();
        for (size_t i = 0; i < t.probe_finst_ids_size(); i++) {
            PUniqueId* frag_inst_id = request.add_probe_finst_ids();
            *frag_inst_id = t.probe_finst_ids(i);
        }

        // add forward targets.
        size_t half = (size - index) / 2;
        for (size_t i = 0; i < half; i++) {
            PTransmitRuntimeFilterForwardTarget* fwd = request.add_forward_targets();
            *fwd = targets[index + 1 + i];
        }

        if (half != 0) {
            VLOG_FILE << "RuntimeFilterWorker::receive_total_runtime_filter. target " << addr << " will forward to "
                      << half << " nodes. nodes[0] = " << request.forward_targets(0).host() << ":"
                      << request.forward_targets(0).port();
        }

        index += (1 + half);
        _runtime_services->runtime_filter_cache->add_rf_event(
                {request.query_id(), request.filter_id(), addr.hostname, "FORWARD"});
        submit_async_runtime_filter_rpc(_rpc_services, addr, timeout_ms, rpc_http_min_size, request,
                                        "forward total runtime filter");
    }
}

void RuntimeFilterDelivery::process_send_broadcast_runtime_filter_event(
        PTransmitRuntimeFilterParams&& params, std::vector<TRuntimeFilterDestination>&& destinations, int timeout_ms,
        int64_t rpc_http_min_size) {
    auto [query_ctx, mem_tracker] =
            get_runtime_filter_mem_tracker(_runtime_services, params.query_id(), params.is_pipeline());
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(mem_tracker.get());

    std::random_device rd;
    std::mt19937 rand(rd());
    std::shuffle(destinations.begin(), destinations.end(), rand);
    _runtime_services->runtime_filter_cache->add_rf_event(
            {params.query_id(), params.filter_id(), "",
             strings::Substitute("SEND_BROADCAST_RF_RPC: num_dest=$0", destinations.size())});
    params.set_is_partial(false);
    TNetworkAddress local;
    local.hostname = BackendOptions::get_localhost();
    local.port = config::brpc_port;
    // put the local destination to the last
    const auto last_dest_idx = destinations.size() - 1;
    for (auto i = 0; i < destinations.size() - 1; ++i) {
        if (destinations[i].address == local) {
            std::swap(destinations[i], destinations[last_dest_idx]);
            break;
        }
    }
    auto& last_dest = destinations[last_dest_idx];
    if (last_dest.address == local) {
        _deliver_broadcast_runtime_filter_local(params, last_dest, timeout_ms, rpc_http_min_size);
        destinations.resize(last_dest_idx);
    }

    if (destinations.empty()) {
        return;
    }

    auto passthrough_delivery = params.data().size() <= config::deliver_broadcast_rf_passthrough_bytes_limit;
    if (passthrough_delivery) {
        _deliver_broadcast_runtime_filter_passthrough(std::move(params), std::move(destinations), timeout_ms,
                                                      rpc_http_min_size);
    } else {
        _deliver_broadcast_runtime_filter_relay(std::move(params), std::move(destinations), timeout_ms,
                                                rpc_http_min_size);
    }
}

void RuntimeFilterDelivery::_deliver_broadcast_runtime_filter_relay(
        PTransmitRuntimeFilterParams&& request, std::vector<TRuntimeFilterDestination>&& destinations, int timeout_ms,
        int64_t rpc_http_min_size) {
    DCHECK(!destinations.empty());
    request.clear_probe_finst_ids();
    request.clear_forward_targets();
    auto first_dest = destinations[0];
    for (const auto& id : first_dest.finstance_ids) {
        auto* finst_id = request.add_probe_finst_ids();
        finst_id->set_hi(id.hi);
        finst_id->set_lo(id.lo);
    }
    for (auto i = 1; i < destinations.size(); ++i) {
        auto& rest_dest = destinations[i];
        auto* forward_target = request.add_forward_targets();
        forward_target->set_host(rest_dest.address.hostname);
        forward_target->set_port(rest_dest.address.port);
        for (const auto& id : rest_dest.finstance_ids) {
            auto* finst_id = forward_target->add_probe_finst_ids();
            finst_id->set_hi(id.hi);
            finst_id->set_lo(id.lo);
        }
    }

    _runtime_services->runtime_filter_cache->add_rf_event(
            {request.query_id(), request.filter_id(), first_dest.address.hostname, "DELIVER_BROADCAST_RF_RELAY"});
    submit_async_runtime_filter_rpc(_rpc_services, first_dest.address, timeout_ms, rpc_http_min_size, request,
                                    "deliver broadcast runtime filter relay");
}

void RuntimeFilterDelivery::_deliver_broadcast_runtime_filter_passthrough(
        PTransmitRuntimeFilterParams&& params, std::vector<TRuntimeFilterDestination>&& destinations, int timeout_ms,
        int64_t rpc_http_min_size) {
    DCHECK(!destinations.empty());

    size_t k = 0;
    while (k < destinations.size()) {
        auto num_inflight =
                std::min<size_t>(destinations.size() - k, config::deliver_broadcast_rf_passthrough_inflight_num);
        RuntimeFilterRpcClosures rpc_closures;
        rpc_closures.reserve(num_inflight);
        BatchClosuresJoinAndClean join_and_clean(rpc_closures);
        auto start_idx = k;
        k += num_inflight;
        for (auto i = 0; i < num_inflight; ++i) {
            auto request = params;
            auto& dest = destinations[start_idx + i];
            request.clear_probe_finst_ids();
            request.clear_forward_targets();
            for (const auto& id : dest.finstance_ids) {
                auto* finst_id = request.add_probe_finst_ids();
                finst_id->set_hi(id.hi);
                finst_id->set_lo(id.lo);
            }
            _runtime_services->runtime_filter_cache->add_rf_event({request.query_id(), request.filter_id(),
                                                                   dest.address.hostname,
                                                                   "DELIVER_BROADCAST_RF_PASSTHROUGH"});

            rpc_closures.push_back(new RuntimeFilterRpcClosure());
            auto* closure = rpc_closures.back();
            closure->result.set_filter_id(request.filter_id());
            closure->debug_info = "deliver broadcast runtime filter passthrough";
            closure->ref();
            send_rpc_runtime_filter(_rpc_services, dest.address, closure, timeout_ms, rpc_http_min_size, request);
        }
    }
}

void RuntimeFilterDelivery::_deliver_broadcast_runtime_filter_local(PTransmitRuntimeFilterParams& param,
                                                                    const TRuntimeFilterDestination& local_dest,
                                                                    int timeout_ms, int64_t rpc_http_min_size) {
    param.clear_forward_targets();
    param.clear_probe_finst_ids();
    for (auto& id : local_dest.finstance_ids) {
        auto* finst_id = param.add_probe_finst_ids();
        finst_id->set_hi(id.hi);
        finst_id->set_lo(id.lo);
    }
    _runtime_services->runtime_filter_cache->add_rf_event(
            {param.query_id(), param.filter_id(), "", "DELIVER_BROADCAST_RF_LOCAL"});
    receive_total_runtime_filter(param, timeout_ms, rpc_http_min_size);
}

void RuntimeFilterDelivery::deliver_part_runtime_filter(std::vector<TNetworkAddress>&& transmit_addrs,
                                                        PTransmitRuntimeFilterParams&& params, int transmit_timeout_ms,
                                                        int64_t rpc_http_min_size, const std::string& msg) {
    const std::string debug_info = "deliver part runtime filter. " + msg;
    for (const auto& addr : transmit_addrs) {
        _runtime_services->runtime_filter_cache->add_rf_event(
                {params.query_id(), params.filter_id(), addr.hostname, msg});
        submit_async_runtime_filter_rpc(_rpc_services, addr, transmit_timeout_ms, rpc_http_min_size, params,
                                        debug_info.c_str());
    }
}

} // namespace starrocks::orchestration
