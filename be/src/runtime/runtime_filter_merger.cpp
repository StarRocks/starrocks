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

#include "runtime/runtime_filter_merger.h"

#include <exec/pipeline/hashjoin/hash_joiner_fwd.h>

#include <utility>

#include "base/time/time.h"
#include "common/config_network_fwd.h"
#include "common/config_runtime_fwd.h"
#include "common/system/backend_options.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_filter.h"
#include "runtime/runtime_filter_builder.h"
#include "runtime/runtime_filter_cache.h"
#include "runtime/runtime_filter_factory.h"
#include "runtime/runtime_filter_rpc.h"
#include "runtime/runtime_filter_worker_context.h"
#include "runtime/service_contexts.h"

namespace starrocks {

Status RuntimeFilterMergerStatus::_merge_skew_broadcast_runtime_filter(RuntimeFilter* out) {
    if (skew_broadcast_rf_material == nullptr || skew_broadcast_rf_material->key_column == nullptr) {
        return Status::InternalError("skew broadcast rf material is nullptr");
    }
    // add broadcast's hash table's key column into out's _hash_partition_bf's every element(Instance and driver side)
    // because we can't know which element should be used when insert one row(need partition columns and partition exprs)
    return RuntimeFilterBuilder::fill(out, skew_broadcast_rf_material->build_type,
                                      skew_broadcast_rf_material->key_column, kHashJoinKeyColumnOffset,
                                      skew_broadcast_rf_material->eq_null, true);
}

RuntimeFilterMerger::RuntimeFilterMerger(const RuntimeServices* runtime_services, const RpcServices* rpc_services,
                                         const UniqueId& query_id, const TQueryOptions& query_options, bool is_pipeline)
        : _runtime_services(runtime_services),
          _rpc_services(rpc_services),
          _query_id(query_id),
          _query_options(query_options),
          _is_pipeline(is_pipeline) {}

Status RuntimeFilterMerger::init(const TRuntimeFilterParams& params) {
    _targets = params.id_to_prober_params;
    for (const auto& it : params.runtime_filter_builder_number) {
        int32_t filter_id = it.first;
        RuntimeFilterMergerStatus status;
        status.expect_number = it.second;
        status.max_size = params.runtime_filter_max_size;
        status.current_size = 0;
        status.stop = false;
        if (params.skew_join_runtime_filters.contains(filter_id)) {
            status.is_skew_join = true;
        } else {
            status.is_skew_join = false;
        }
        _statuses.insert(std::make_pair(filter_id, std::move(status)));
    }
    return Status::OK();
}

void finalize_membership_filters(RuntimeFilterMergerStatus* rf_state, const size_t rf_version, const size_t filter_id) {
    for (const auto& [_, filter] : rf_state->filters) {
        const auto* membership_filter = filter->get_membership_filter();
        if (!membership_filter->can_use_bf()) {
            VLOG_FILE << "RuntimeFilterMerger::merge_runtime_filter. some partial rf's size exceeds "
                         "global_runtime_filter_build_max_size, stop building bf and only reserve min/max filter. "
                      << "filter_id=" << filter_id;
            rf_state->exceeded = false;
        }
        rf_state->current_size += membership_filter->size();
    }

    if (rf_state->current_size > rf_state->max_size) {
        // already exceeds max size, no need to build bloom filter, but still reserve min/max filter.
        VLOG_FILE << "RuntimeFilterMerger::merge_runtime_filter. stop building bf since size too "
                     "large. filter_id="
                  << filter_id << ", rf_size=" << rf_state->current_size;
        rf_state->exceeded = false;
    }

    VLOG_FILE << "RuntimeFilterMerger::merge_runtime_filter. merged filter_id=" << filter_id;

    if (!rf_state->exceeded) {
        VLOG_FILE << "RuntimeFilterMerger::merge_runtime_filter, clear bf in all filters";
        if (rf_version >= RF_VERSION_V3) {
            for (auto& [_, rf] : rf_state->filters) {
                rf = RuntimeFilterFactory::to_empty_filter(&rf_state->pool, rf);
            }
        } else {
            for (auto& [_, rf] : rf_state->filters) {
                rf->get_membership_filter()->clear_bf();
            }
        }
        if (rf_state->skew_broadcast_rf_material != nullptr &&
            rf_state->skew_broadcast_rf_material->key_column != nullptr) {
            rf_state->skew_broadcast_rf_material->key_column.reset();
        }
    }
}

void RuntimeFilterMerger::merge_runtime_filter(PTransmitRuntimeFilterParams& params) {
    auto [query_ctx, mem_tracker] =
            get_runtime_filter_mem_tracker(_runtime_services, params.query_id(), params.is_pipeline());
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(mem_tracker.get());

    DCHECK(params.is_partial());
    int32_t filter_id = params.filter_id();
    int32_t be_number = params.build_be_number();

    std::vector<TRuntimeFilterProberParams>* target_nodes = nullptr;
    // check if there is no consumer.
    {
        auto it = _targets.find(filter_id);
        if (it == _targets.end()) return;
        target_nodes = &(it->second);
        if (target_nodes->size() == 0) return;
    }

    RuntimeFilterMergerStatus* status = nullptr;
    {
        auto it = _statuses.find(filter_id);
        if (it == _statuses.end()) return;
        status = &(it->second);
        if (status->arrives.find(be_number) != status->arrives.end()) {
            // duplicated one, just skip it.
            VLOG_FILE << "RuntimeFilterMerger::merge_runtime_filter. duplicated filter_id=" << filter_id
                      << ", be_number=" << be_number;
            return;
        }
        if (status->stop) {
            return;
        }
    }

    int64_t now = UnixMillis();
    if (status->recv_first_filter_ts == 0) {
        status->recv_first_filter_ts = now;
    }
    status->recv_last_filter_ts = now;

    // to merge runtime filters
    ObjectPool* pool = &(status->pool);
    RuntimeFilter* rf = nullptr;
    int rf_version = RuntimeFilterSerde::deserialize(pool, &rf, reinterpret_cast<const uint8_t*>(params.data().data()),
                                                     params.data().size());
    if (rf == nullptr) {
        // something wrong with deserialization.
        return;
    }

    status->arrives.insert(be_number);
    status->filters.insert(std::make_pair(be_number, rf));

    // not ready. still have to wait more filters.
    if (status->filters.size() < status->expect_number) return;

    // skew join's rf from broadcast join not arrived yet, we need to wait.
    if (status->is_skew_join &&
        (status->skew_broadcast_rf_material == nullptr || status->skew_broadcast_rf_material->key_column == nullptr)) {
        return;
    }

    if (rf->type() != RuntimeFilterSerializeType::IN_FILTER) {
        finalize_membership_filters(status, rf_version, filter_id);
    }

    _send_total_runtime_filter(rf_version, filter_id);
}

void RuntimeFilterMerger::store_skew_broadcast_join_runtime_filter(PTransmitRuntimeFilterParams& params) {
    auto [query_ctx, mem_tracker] =
            get_runtime_filter_mem_tracker(_runtime_services, params.query_id(), params.is_pipeline());
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(mem_tracker.get());

    DCHECK(params.is_partial());
    // we use skew_shuffle_filter_id, so it will be merged with coressponding shuffle join's partition rf
    int32_t filter_id = params.skew_shuffle_filter_id();
    DCHECK(filter_id != -1);

    std::vector<TRuntimeFilterProberParams>* target_nodes = nullptr;
    // check if there is no consumer.
    {
        auto it = _targets.find(filter_id);
        if (it == _targets.end()) return;
        target_nodes = &(it->second);
        if (target_nodes->size() == 0) return;
    }

    RuntimeFilterMergerStatus* status = nullptr;
    {
        auto it = _statuses.find(filter_id);
        if (it == _statuses.end()) return;
        status = &(it->second);
        // 1. some instance of broadcast join already rf, we only need to store the first one.
        // 2. if status is stop, we don't need to store rf.
        // 3. if it's not skew join, skip it
        if (status->skew_broadcast_rf_material != nullptr || status->stop || !status->is_skew_join) {
            return;
        }
    }

    int64_t now = UnixMillis();
    if (status->recv_first_filter_ts == 0) {
        status->recv_first_filter_ts = now;
    }
    status->recv_last_filter_ts = now;

    // if shuffle join's rf already too big, just skip
    if (!status->exceeded) return;

    // store material of broadcast join rf
    status->skew_broadcast_rf_material = nullptr;
    auto rf_version = RuntimeFilterSerde::skew_deserialize(&(status->pool), &(status->skew_broadcast_rf_material),
                                                           reinterpret_cast<const uint8_t*>(params.data().data()),
                                                           params.data().size(), params.columntype());
    RETURN_IF(!rf_version.ok(), (void)nullptr);
    if (status->skew_broadcast_rf_material == nullptr) {
        // something wrong with deserialization.
        return;
    }

    // not ready. still have to wait more filters.
    if (status->filters.size() < status->expect_number) return;

    // this only happens when broadcast's rf is the last rf instance arrived
    _send_total_runtime_filter(rf_version.value(), filter_id);
}

void RuntimeFilterMerger::_send_total_runtime_filter(int rf_version, int32_t filter_id) {
    auto status_it = _statuses.find(filter_id);
    DCHECK(status_it != _statuses.end());
    RuntimeFilterMergerStatus* status = &(status_it->second);
    DCHECK(status->isSent == false);
    auto target_it = _targets.find(filter_id);
    DCHECK(target_it != _targets.end());
    std::vector<TRuntimeFilterProberParams>* target_nodes = &(target_it->second);

    RuntimeFilter* out = nullptr;
    RuntimeFilter* first = status->filters.begin()->second;
    ObjectPool* pool = &(status->pool);
    out = first->create_empty(pool);
    if (out->type() != RuntimeFilterSerializeType::IN_FILTER) {
        auto* membership_filter = out->get_membership_filter();
        if (!status->exceeded) {
            if (rf_version >= RF_VERSION_V3) {
                out = RuntimeFilterFactory::to_empty_filter(pool, out);
                membership_filter = out->get_membership_filter();
            } else {
                membership_filter->clear_bf();
            }
        }
        membership_filter->set_global();
    }

    for (auto it : status->filters) {
        out->concat(it.second);
    }

    // this is a skew join and rf from broadcast join already arrived, we need to merge it
    // at this point, every rf instance is stored in _hash_partition_bf, so it's the best time to merge skew broadcast's rf
    if (status->is_skew_join && status->skew_broadcast_rf_material != nullptr &&
        status->skew_broadcast_rf_material->key_column != nullptr) {
        Status res = status->_merge_skew_broadcast_runtime_filter(out);
        if (!res.ok()) {
            VLOG_FILE << "RuntimeFilterMerger::_send_total_runtime_filter failed";
            return;
        }
    }

    // if well enough, then we send it out.

    PTransmitRuntimeFilterParams request;
    // For pipeline engine
    if (_is_pipeline) {
        request.set_is_pipeline(true);
    }
    request.set_filter_id(filter_id);
    request.set_is_partial(false);

    PUniqueId* query_id = request.mutable_query_id();
    query_id->set_hi(_query_id.hi);
    query_id->set_lo(_query_id.lo);

    std::string* send_data = request.mutable_data();
    size_t max_size = RuntimeFilterSerde::max_size(rf_version, out);
    send_data->resize(max_size);

    size_t actual_size = RuntimeFilterSerde::serialize(rf_version, out, reinterpret_cast<uint8_t*>(send_data->data()));
    send_data->resize(actual_size);
    int timeout_ms = config::send_rpc_runtime_filter_timeout_ms;
    if (_query_options.__isset.runtime_filter_send_timeout_ms) {
        timeout_ms = _query_options.runtime_filter_send_timeout_ms;
    }
    int64_t rpc_http_min_size = config::send_runtime_filter_via_http_rpc_min_size;
    if (_query_options.__isset.runtime_filter_rpc_http_min_size) {
        rpc_http_min_size = _query_options.runtime_filter_rpc_http_min_size;
    }

    // we pass this options to the receiver, and receiver can use this option to forward rf to others.
    request.set_transmit_timeout_ms(timeout_ms);
    request.set_transmit_via_http_min_size(rpc_http_min_size);

    int64_t now = UnixMillis();
    status->broadcast_filter_ts = now;

    if (VLOG_FILE_IS_ON) {
        VLOG_FILE << "RuntimeFilterMerger::_send_total_runtime_filter. target_nodes[0]=" << target_nodes->at(0)
                  << ", target_nodes_size=" << target_nodes->size() << ", filter_id=" << request.filter_id()
                  << ", latency(last-first=" << status->recv_last_filter_ts - status->recv_first_filter_ts
                  << ", send-first=" << status->broadcast_filter_ts - status->recv_first_filter_ts << ")"
                  << ", filter=" << out->debug_string();
    }
    request.set_broadcast_timestamp(now);

    std::map<TNetworkAddress, std::vector<TUniqueId>> nodes_to_frag_insts;
    for (const auto& node : (*target_nodes)) {
        const auto& addr = node.fragment_instance_address;
        auto it = nodes_to_frag_insts.find(addr);
        if (it == nodes_to_frag_insts.end()) {
            nodes_to_frag_insts.insert(std::make_pair(addr, std::vector<TUniqueId>{}));
            it = nodes_to_frag_insts.find(addr);
        }
        it->second.push_back(node.fragment_instance_id);
    }

    TNetworkAddress local;
    local.hostname = BackendOptions::get_localhost();
    local.port = config::brpc_port;
    std::vector<std::pair<TNetworkAddress, std::vector<TUniqueId>>> targets;

    // put localhost to the first of targets.
    // local -> local can be very fast
    // but we don't want to go short-circuit because it's complicated.
    // we have to deal with deserialization and shared runtime filter.
    {
        const auto it = nodes_to_frag_insts.find(local);
        if (it != nodes_to_frag_insts.end()) {
            targets.emplace_back(it->first, it->second);
        }
    }
    for (const auto& it : nodes_to_frag_insts) {
        if (it.first != local) {
            targets.emplace_back(it.first, it.second);
        }
    }

    if (VLOG_FILE_IS_ON) {
        std::string targets_string;
        for (const auto& t : targets) {
            targets_string += t.first.hostname + ":" + std::to_string(t.first.port);
            targets_string += ", ";
        }

        VLOG_FILE << "RuntimeFilterMerger::_send_total_runtime_filter. targets=[" << targets_string
                  << "], filter_id=" << request.filter_id() << ", filter=" << out->debug_string();
    }

    size_t index = 0;
    size_t size = targets.size();

    while (index < size) {
        auto& t = targets[index];
        bool is_local = (local == t.first);
        request.clear_probe_finst_ids();
        request.clear_forward_targets();
        for (const auto& inst : t.second) {
            PUniqueId* frag_inst_id = request.add_probe_finst_ids();
            frag_inst_id->set_hi(inst.hi);
            frag_inst_id->set_lo(inst.lo);
        }

        // add forward targets.
        // forward [index+1, index+1+half) to [index]
        size_t half = (size - index) / 2;
        // if X->X, and we split into two half [A, B]
        // then in next step,  X->A, and X->B, which is in-efficient
        // so if X->X, we don't do split.
        if (is_local) {
            half = 0;
        }
        for (size_t i = 0; i < half; i++) {
            auto& ft = targets[index + 1 + i];
            PTransmitRuntimeFilterForwardTarget* fwd = request.add_forward_targets();
            fwd->set_host(ft.first.hostname);
            fwd->set_port(ft.first.port);
            for (const auto& inst : ft.second) {
                PUniqueId* finst_id = fwd->add_probe_finst_ids();
                finst_id->set_hi(inst.hi);
                finst_id->set_lo(inst.lo);
            }
        }

        if (half != 0) {
            VLOG_FILE << "RuntimeFilterMerger::_send_total_runtime_filter. filter_id=" << request.filter_id()
                      << ". target=" << t.first << ". it will forward to " << half
                      << " nodes. nodes[0] = " << request.forward_targets(0).host() << ":"
                      << request.forward_targets(0).port();
        }

        index += (1 + half);
        _runtime_services->runtime_filter_cache->add_rf_event(
                {request.query_id(), request.filter_id(), t.first.hostname, "SEND_TOTAL_RF_RPC"});
        submit_async_runtime_filter_rpc(_rpc_services, t.first, timeout_ms, rpc_http_min_size, request,
                                        "send total runtime filter");
    }

    // we don't need to hold rf any more.
    pool->clear();
    status->isSent = true;
}

} // namespace starrocks
