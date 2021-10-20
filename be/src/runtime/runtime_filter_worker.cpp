// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "runtime/runtime_filter_worker.h"

#include "exprs/vectorized/runtime_filter_bank.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/doris_internal_service.pb.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/runtime_state.h"
#include "service/backend_options.h"
#include "util/brpc_stub_cache.h"
#include "util/defer_op.h"
#include "util/ref_count_closure.h"
#include "util/time.h"

namespace starrocks {

class RuntimeFilterRpcClosure final : public RefCountClosure<PTransmitRuntimeFilterResult> {
public:
    int64_t seq = 0;
};

static const int default_send_rpc_runtime_filter_timeout_ms = 1000;

static void send_rpc_runtime_filter(doris::PBackendService_Stub* stub, RuntimeFilterRpcClosure* rpc_closure,
                                    int timeout_ms, const PTransmitRuntimeFilterParams& request) {
    if (rpc_closure->seq != 0) {
        brpc::Join(rpc_closure->cntl.call_id());
    }
    rpc_closure->ref();
    rpc_closure->cntl.Reset();
    rpc_closure->cntl.set_timeout_ms(timeout_ms);
    stub->transmit_runtime_filter(&rpc_closure->cntl, &request, &rpc_closure->result, rpc_closure);
    rpc_closure->seq++;
}

void RuntimeFilterPort::add_listener(vectorized::RuntimeFilterProbeDescriptor* rf_desc) {
    int32_t rf_id = rf_desc->filter_id();
    if (_listeners.find(rf_id) == _listeners.end()) {
        _listeners.insert({rf_id, std::list<vectorized::RuntimeFilterProbeDescriptor*>()});
    }
    auto& wait_list = _listeners.find(rf_id)->second;
    wait_list.emplace_back(rf_desc);
}

void RuntimeFilterPort::publish_runtime_filters(std::list<vectorized::RuntimeFilterBuildDescriptor*>& rf_descs) {
    RuntimeState* state = _state;
    for (auto* rf_desc : rf_descs) {
        auto* filter = rf_desc->runtime_filter();
        if (filter == nullptr) continue;
        state->runtime_filter_port()->receive_runtime_filter(rf_desc->filter_id(), filter);
    }
    int timeout_ms = default_send_rpc_runtime_filter_timeout_ms;
    if (state->query_options().__isset.runtime_filter_send_timeout_ms) {
        timeout_ms = state->query_options().runtime_filter_send_timeout_ms;
    }

    for (auto* rf_desc : rf_descs) {
        auto* filter = rf_desc->runtime_filter();
        if (filter == nullptr || !rf_desc->has_remote_targets() || rf_desc->merge_nodes().size() == 0) continue;

        // for broadcast join, we need to send only one copy.
        // and we have already assigned sender frag inst id in planner.
        if (rf_desc->join_mode() == TRuntimeFilterBuildJoinMode::BORADCAST) {
            if (rf_desc->sender_finst_id() != state->fragment_instance_id()) {
                continue;
            }
            VLOG_FILE << "RuntimeFilterPort::publish_runtime_filters. broadcast join filter_id = "
                      << rf_desc->filter_id() << ", finst_id = " << rf_desc->sender_finst_id();
        }

        // rf metadata
        PTransmitRuntimeFilterParams params;
        params.set_filter_id(rf_desc->filter_id());
        params.set_is_partial(true);
        PUniqueId* query_id = params.mutable_query_id();
        query_id->set_hi(state->query_id().hi);
        query_id->set_lo(state->query_id().lo);
        PUniqueId* finst_id = params.mutable_finst_id();
        finst_id->set_hi(state->fragment_instance_id().hi);
        finst_id->set_lo(state->fragment_instance_id().lo);
        params.set_build_be_number(state->be_number());

        // print before setting data, otherwise it's too big.
        VLOG_FILE << "RuntimeFilterPort::publish_runtime_filters. merge_node[0] = " << rf_desc->merge_nodes()[0]
                  << ", filter_size = " << filter->size() << ", query_id = " << params.query_id()
                  << ", finst_id = " << params.finst_id() << ", be_number = " << params.build_be_number();

        std::string* rf_data = params.mutable_data();
        size_t max_size = vectorized::RuntimeFilterHelper::max_runtime_filter_serialized_size(filter);
        rf_data->resize(max_size);
        size_t actual_size = vectorized::RuntimeFilterHelper::serialize_runtime_filter(
                filter, reinterpret_cast<uint8_t*>(rf_data->data()));
        rf_data->resize(actual_size);

        state->exec_env()->runtime_filter_worker()->send_part_runtime_filter(std::move(params), rf_desc->merge_nodes(),
                                                                             timeout_ms);
    }
}

void RuntimeFilterPort::receive_runtime_filter(int32_t filter_id, const vectorized::JoinRuntimeFilter* rf) {
    auto it = _listeners.find(filter_id);
    if (it == _listeners.end()) return;
    VLOG_FILE << "RuntimeFilterPort::receive_runtime_filter(local). filter_id = " << filter_id
              << ", filter_size = " << rf->size();
    auto& wait_list = it->second;
    for (auto* rf_desc : wait_list) {
        rf_desc->set_runtime_filter(rf);
    }
}

void RuntimeFilterPort::receive_shared_runtime_filter(int32_t filter_id,
                                                      const std::shared_ptr<const vectorized::JoinRuntimeFilter>& rf) {
    auto it = _listeners.find(filter_id);
    if (it == _listeners.end()) return;
    VLOG_FILE << "RuntimeFilterPort::receive_runtime_filter(shared). filter_id = " << filter_id
              << ", filter_size = " << rf->size();
    auto& wait_list = it->second;
    for (auto* rf_desc : wait_list) {
        rf_desc->set_shared_runtime_filter(rf);
    }
}
RuntimeFilterMerger::RuntimeFilterMerger(ExecEnv* env, const UniqueId& query_id, const TQueryOptions& query_options)
        : _exec_env(env), _query_id(query_id), _query_options(query_options) {}

Status RuntimeFilterMerger::init(const TRuntimeFilterParams& params) {
    _targets = params.id_to_prober_params;
    for (const auto& it : params.runtime_filter_builder_number) {
        int32_t filter_id = it.first;
        RuntimeFilterMergerStatus status;
        status.expect_number = it.second;
        status.max_size = params.runtime_filter_max_size;
        status.current_size = 0;
        status.stop = false;
        _statuses.insert(std::make_pair(filter_id, std::move(status)));
    }
    return Status::OK();
}

void RuntimeFilterMerger::merge_runtime_filter(PTransmitRuntimeFilterParams& params,
                                               RuntimeFilterRpcClosure* rpc_closure) {
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
    vectorized::JoinRuntimeFilter* rf = nullptr;
    vectorized::RuntimeFilterHelper::deserialize_runtime_filter(
            pool, &rf, reinterpret_cast<const uint8_t*>(params.data().data()), params.data().size());
    if (rf == nullptr) {
        // something wrong with deserialization.
        return;
    }

    // exceeds max size, stop building it.
    status->current_size += rf->size();
    if (status->current_size > status->max_size) {
        // alreay exceeds max size, no need to build it.
        VLOG_FILE << "RuntimeFilterMerger::merge_runtime_filter. stop building since size too "
                     "large. size = "
                  << status->current_size;
        status->stop = true;
        return;
    }

    status->arrives.insert(be_number);
    status->filters.insert(std::make_pair(be_number, rf));

    // not ready. still have to wait more filters.
    if (status->filters.size() < status->expect_number) return;
    _send_total_runtime_filter(filter_id, rpc_closure);
}

void RuntimeFilterMerger::_send_total_runtime_filter(int32_t filter_id, RuntimeFilterRpcClosure* rpc_closure) {
    auto status_it = _statuses.find(filter_id);
    DCHECK(status_it != _statuses.end());
    RuntimeFilterMergerStatus* status = &(status_it->second);
    auto target_it = _targets.find(filter_id);
    DCHECK(target_it != _targets.end());
    std::vector<TRuntimeFilterProberParams>* target_nodes = &(target_it->second);

    vectorized::JoinRuntimeFilter* out = nullptr;
    vectorized::JoinRuntimeFilter* first = status->filters.begin()->second;
    ObjectPool* pool = &(status->pool);
    out = first->create_empty(pool);
    for (auto it : status->filters) {
        out->concat(it.second);
    }

    // if well enough, then we send it out.

    PTransmitRuntimeFilterParams request;
    request.set_filter_id(filter_id);
    request.set_is_partial(false);
    PUniqueId* query_id = request.mutable_query_id();
    query_id->set_hi(_query_id.hi);
    query_id->set_lo(_query_id.lo);

    std::string* send_data = request.mutable_data();
    size_t max_size = vectorized::RuntimeFilterHelper::max_runtime_filter_serialized_size(out);
    send_data->resize(max_size);

    size_t actual_size = vectorized::RuntimeFilterHelper::serialize_runtime_filter(
            out, reinterpret_cast<uint8_t*>(send_data->data()));
    send_data->resize(actual_size);
    int timeout_ms = default_send_rpc_runtime_filter_timeout_ms;
    if (_query_options.__isset.runtime_filter_send_timeout_ms) {
        timeout_ms = _query_options.runtime_filter_send_timeout_ms;
    }

    int64_t now = UnixMillis();
    status->broadcast_filter_ts = now;

    VLOG_FILE << "RuntimeFilterMerger::merge_runtime_filter. target_nodes[0] = " << target_nodes->at(0)
              << ", filter_id = " << request.filter_id() << ", filter_size = " << out->size()
              << ", latency(last-first = " << status->recv_last_filter_ts - status->recv_first_filter_ts
              << ", send-first = " << status->broadcast_filter_ts - status->recv_first_filter_ts << ")";
    request.set_broadcast_timestamp(now);

    std::map<TNetworkAddress, std::vector<TUniqueId>> nodes_to_frag_insts;
    for (const auto& node : (*target_nodes)) {
        const auto& addr = node.fragment_instance_address;
        auto it = nodes_to_frag_insts.find(addr);
        if (it == nodes_to_frag_insts.end()) {
            nodes_to_frag_insts.insert(make_pair(addr, std::vector<TUniqueId>{}));
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
            targets.emplace_back(make_pair(it->first, it->second));
        }
    }
    for (const auto& it : nodes_to_frag_insts) {
        if (it.first != local) {
            targets.emplace_back(make_pair(it.first, it.second));
        }
    }

    size_t index = 0;
    size_t size = targets.size();

    while (index < size) {
        auto& t = targets[index];
        bool is_local = (local == t.first);
        doris::PBackendService_Stub* stub = _exec_env->brpc_stub_cache()->get_stub(t.first);
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
            VLOG_FILE << "RuntimeFilterMerger::merge_runtime_filter. target " << t.first << " will forward to " << half
                      << " nodes. nodes[0] = " << request.forward_targets(0).DebugString();
        }

        index += (1 + half);
        send_rpc_runtime_filter(stub, rpc_closure, timeout_ms, request);
    }

    // we don't need to hold rf any more.
    pool->clear();
}

enum EventType {
    RECEIVE_TOTAL_RF = 0,
    CLOSE_QUERY = 1,
    OPEN_QUERY = 2,
    RECEIVE_PART_RF = 3,
    SEND_PART_RF = 4,
};

struct RuntimeFilterWorkerEvent {
public:
    RuntimeFilterWorkerEvent() = default;
    EventType type;
    TUniqueId query_id;
    TQueryOptions query_options;
    TRuntimeFilterParams create_rf_merger_request;
    std::vector<TNetworkAddress> transmit_addrs;
    int transmit_timeout_ms;
    PTransmitRuntimeFilterParams transmit_rf_request;
};

static_assert(std::is_move_assignable<RuntimeFilterWorkerEvent>::value);

RuntimeFilterWorker::RuntimeFilterWorker(ExecEnv* env) : _exec_env(env), _thread([this] { execute(); }) {}

RuntimeFilterWorker::~RuntimeFilterWorker() {
    _queue.shutdown();
    _thread.join();
}

void RuntimeFilterWorker::open_query(const TUniqueId& query_id, const TQueryOptions& query_options,
                                     const TRuntimeFilterParams& params) {
    VLOG_FILE << "RuntimeFilterWorker::open_query. query_id = " << query_id << ", params = " << params;
    RuntimeFilterWorkerEvent ev;
    ev.type = OPEN_QUERY;
    ev.query_id = query_id;
    ev.query_options = query_options;
    ev.create_rf_merger_request = params;
    _queue.put(std::move(ev));
}

void RuntimeFilterWorker::close_query(const TUniqueId& query_id) {
    VLOG_FILE << "RuntimeFilterWorker::close_query. query_id = " << query_id;
    RuntimeFilterWorkerEvent ev;
    ev.type = CLOSE_QUERY;
    ev.query_id = query_id;
    _queue.put(std::move(ev));
}

void RuntimeFilterWorker::send_part_runtime_filter(PTransmitRuntimeFilterParams&& params,
                                                   const std::vector<TNetworkAddress>& addrs, int timeout_ms) {
    RuntimeFilterWorkerEvent ev;
    ev.type = SEND_PART_RF;
    ev.transmit_timeout_ms = timeout_ms;
    ev.transmit_addrs = addrs;
    ev.transmit_rf_request = std::move(params);
    _queue.put(std::move(ev));
}

void RuntimeFilterWorker::receive_runtime_filter(const PTransmitRuntimeFilterParams& params) {
    VLOG_FILE << "RuntimeFilterWorker::receive_runtime_filter: partial = " << params.is_partial()
              << ", query_id = " << params.query_id() << ", finst_id = " << params.finst_id()
              << ", # probe insts = " << params.probe_finst_ids_size();

    RuntimeFilterWorkerEvent ev;
    if (params.is_partial()) {
        ev.type = RECEIVE_PART_RF;
    } else {
        ev.type = RECEIVE_TOTAL_RF;
    }
    ev.query_id.hi = params.query_id().hi();
    ev.query_id.lo = params.query_id().lo();
    ev.transmit_rf_request = params;
    _queue.put(std::move(ev));
}
void RuntimeFilterWorker::_receive_total_runtime_filter(PTransmitRuntimeFilterParams& request,
                                                        RuntimeFilterRpcClosure* rpc_closure) {
    // deserialize once, and all fragment instance shared that runtime filter.
    vectorized::JoinRuntimeFilter* rf = nullptr;
    const std::string& data = request.data();
    vectorized::RuntimeFilterHelper::deserialize_runtime_filter(
            nullptr, &rf, reinterpret_cast<const uint8_t*>(data.data()), data.size());
    if (rf == nullptr) {
        return;
    }
    std::shared_ptr<vectorized::JoinRuntimeFilter> shared_rf(rf);
    _exec_env->fragment_mgr()->receive_runtime_filter(request, shared_rf);

    // not enough, have to forward this request to continue broadcast.
    // copy modifed fields out.
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
        doris::PBackendService_Stub* stub = _exec_env->brpc_stub_cache()->get_stub(addr);

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
            VLOG_FILE << "RuntimeFilterWorker::receive_total_rf. target " << addr << " will forward to " << half
                      << " nodes. nodes[0] = " << request.forward_targets(0).DebugString();
        }

        index += (1 + half);
        send_rpc_runtime_filter(stub, rpc_closure, default_send_rpc_runtime_filter_timeout_ms, request);
    }
}

void RuntimeFilterWorker::execute() {
    LOG(INFO) << "RuntimeFilterWorker start working.";
    RuntimeFilterRpcClosure* rpc_closure = new RuntimeFilterRpcClosure();
    rpc_closure->ref();
    DeferOp deferop([&] { rpc_closure->Run(); });

    for (;;) {
        RuntimeFilterWorkerEvent ev;
        if (!_queue.blocking_get(&ev)) {
            break;
        }
        switch (ev.type) {
        case RECEIVE_TOTAL_RF: {
            _receive_total_runtime_filter(ev.transmit_rf_request, rpc_closure);
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
            RuntimeFilterMerger merger(_exec_env, UniqueId(ev.query_id), ev.query_options);
            Status st = merger.init(ev.create_rf_merger_request);
            if (!st.ok()) {
                VLOG_QUERY << "open query: rf merger initialization failed. error = " << st.get_error_msg();
                break;
            }
            _mergers.insert(std::make_pair(ev.query_id, std::move(merger)));
            break;
        }

        case RECEIVE_PART_RF: {
            auto it = _mergers.find(ev.query_id);
            if (it == _mergers.end()) {
                VLOG_QUERY << "receive part rf: rf merger not existed. query_id = " << ev.query_id;
                break;
            }
            RuntimeFilterMerger& merger = it->second;
            merger.merge_runtime_filter(ev.transmit_rf_request, rpc_closure);
            break;
        }

        case SEND_PART_RF: {
            for (const auto& addr : ev.transmit_addrs) {
                doris::PBackendService_Stub* stub = _exec_env->brpc_stub_cache()->get_stub(addr);
                send_rpc_runtime_filter(stub, rpc_closure, ev.transmit_timeout_ms, ev.transmit_rf_request);
            }
            break;
        }

        default:
            VLOG_QUERY << "unknown event type = " << ev.type;
            break;
        }
    }
    LOG(INFO) << "RuntimeFilterWorker going to exit.";
}

} // namespace starrocks
