// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <unordered_map>

#include "exec/exec_node.h"
#include "exec/pipeline/driver_limiter.h"
#include "exec/pipeline/pipeline.h"
#include "exec/pipeline/pipeline_driver.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/runtime_filter_types.h"
#include "exec/pipeline/scan/morsel.h"
#include "exec/query_cache/cache_param.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/HeartbeatService.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/QueryPlanExtra_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/profile_report_worker.h"
#include "runtime/runtime_filter_worker.h"
#include "runtime/runtime_state.h"
#include "util/hash_util.hpp"

namespace starrocks {

class StreamLoadContext;

namespace pipeline {

using RuntimeFilterPort = starrocks::RuntimeFilterPort;
using PerDriverScanRangesMap = std::map<int32_t, std::vector<TScanRangeParams>>;

class FragmentContext {
    friend FragmentContextManager;

public:
    FragmentContext() {}
    ~FragmentContext() {
        _runtime_filter_hub.close_all_in_filters(_runtime_state.get());
        _drivers.clear();
        close_all_pipelines();
        if (_plan != nullptr) {
            _plan->close(_runtime_state.get());
        }
    }
    const TUniqueId& query_id() const { return _query_id; }
    void set_query_id(const TUniqueId& query_id) { _query_id = query_id; }
    const TUniqueId& fragment_instance_id() const { return _fragment_instance_id; }
    void set_fragment_instance_id(const TUniqueId& fragment_instance_id) {
        _fragment_instance_id = fragment_instance_id;
    }
    void set_fe_addr(const TNetworkAddress& fe_addr) { _fe_addr = fe_addr; }
    const TNetworkAddress& fe_addr() { return _fe_addr; }
    FragmentFuture finish_future() { return _finish_promise.get_future(); }
    RuntimeState* runtime_state() const { return _runtime_state.get(); }
    std::shared_ptr<RuntimeState> runtime_state_ptr() { return _runtime_state; }
    void set_runtime_state(std::shared_ptr<RuntimeState>&& runtime_state) { _runtime_state = std::move(runtime_state); }
    ExecNode*& plan() { return _plan; }

    Pipelines& pipelines() { return _pipelines; }
    void set_pipelines(Pipelines&& pipelines) { _pipelines = std::move(pipelines); }
    Drivers& drivers() { return _drivers; }
    void set_drivers(Drivers&& drivers) {
        _drivers = std::move(drivers);
        _num_drivers.store(_drivers.size());
        _final_status.store(nullptr);
    }

    int num_drivers() const { return _num_drivers.load(); }
    bool count_down_drivers() { return _num_drivers.fetch_sub(1) == 1; }

    void set_final_status(const Status& status);

    Status final_status() const {
        auto* status = _final_status.load();
        return status == nullptr ? Status::OK() : *status;
    }

    void cancel(const Status& status);

    void finish() { cancel(Status::OK()); }

    bool is_canceled() const { return _runtime_state->is_cancelled(); }

    MorselQueueFactoryMap& morsel_queue_factories() { return _morsel_queue_factories; }

    Status prepare_all_pipelines() {
        for (auto& pipe : _pipelines) {
            RETURN_IF_ERROR(pipe->prepare(_runtime_state.get()));
        }
        return Status::OK();
    }

    void close_all_pipelines() {
        for (auto& pipe : _pipelines) {
            pipe->close(_runtime_state.get());
        }
    }

    RuntimeFilterHub* runtime_filter_hub() { return &_runtime_filter_hub; }

    RuntimeFilterPort* runtime_filter_port() { return _runtime_state->runtime_filter_port(); }

    void prepare_pass_through_chunk_buffer();
    void destroy_pass_through_chunk_buffer();

    void set_enable_resource_group() { _enable_resource_group = true; }

    bool enable_resource_group() const { return _enable_resource_group; }

    void set_driver_token(DriverLimiter::TokenPtr driver_token) { _driver_token = std::move(driver_token); }

    query_cache::CacheParam& cache_param() { return _cache_param; }

    void set_enable_cache(bool flag) { _enable_cache = flag; }

    bool enable_cache() const { return _enable_cache; }

    PerDriverScanRangesMap& scan_ranges_per_driver() { return _scan_ranges_per_driver_seq; }

    void set_stream_load_contexts(const std::vector<StreamLoadContext*>& contexts);

    size_t expired_log_count() { return _expired_log_count; }

    void set_expired_log_count(size_t val) { _expired_log_count = val; }

private:
    // Id of this query
    TUniqueId _query_id;
    // Id of this instance
    TUniqueId _fragment_instance_id;
    TNetworkAddress _fe_addr;

    // promise used to determine whether fragment finished its execution
    FragmentPromise _finish_promise;

    // never adjust the order of _runtime_state, _plan, _pipelines and _drivers, since
    // _plan depends on _runtime_state and _drivers depends on _runtime_state.
    std::shared_ptr<RuntimeState> _runtime_state = nullptr;
    ExecNode* _plan = nullptr; // lives in _runtime_state->obj_pool()
    Pipelines _pipelines;
    Drivers _drivers;

    RuntimeFilterHub _runtime_filter_hub;

    MorselQueueFactoryMap _morsel_queue_factories;
    // when _num_drivers counts down to zero, means all drivers has finished, then BE
    // can notify FE via reportExecStatus that fragment instance is done after which
    // FragmentContext can be unregistered safely.
    std::atomic<size_t> _num_drivers;
    std::atomic<Status*> _final_status;
    Status _s_status;

    bool _enable_resource_group = false;

    DriverLimiter::TokenPtr _driver_token = nullptr;
    query_cache::CacheParam _cache_param;
    bool _enable_cache = false;
    PerDriverScanRangesMap _scan_ranges_per_driver_seq;
    std::vector<StreamLoadContext*> _stream_load_contexts;
    bool _channel_stream_load = false;

    size_t _expired_log_count = 0;
};

class FragmentContextManager {
public:
    FragmentContextManager() = default;
    ~FragmentContextManager() = default;

    FragmentContextManager(const FragmentContextManager&) = delete;
    FragmentContextManager(FragmentContextManager&&) = delete;
    FragmentContextManager& operator=(const FragmentContextManager&) = delete;
    FragmentContextManager& operator=(FragmentContextManager&&) = delete;

    FragmentContext* get_or_register(const TUniqueId& fragment_id);
    FragmentContextPtr get(const TUniqueId& fragment_id);

    Status register_ctx(const TUniqueId& fragment_id, FragmentContextPtr fragment_ctx);
    void unregister(const TUniqueId& fragment_id);

    void cancel(const Status& status);

private:
    std::mutex _lock;
    std::unordered_map<TUniqueId, FragmentContextPtr> _fragment_contexts;
};
} // namespace pipeline
} // namespace starrocks
