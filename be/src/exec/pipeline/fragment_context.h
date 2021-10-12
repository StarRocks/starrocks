// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <unordered_map>

#include "exec/exec_node.h"
#include "exec/pipeline/morsel.h"
#include "exec/pipeline/pipeline.h"
#include "exec/pipeline/pipeline_driver.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/HeartbeatService.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/QueryPlanExtra_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/runtime_state.h"
#include "util/hash_util.hpp"
namespace starrocks {
class MemTracker;
namespace pipeline {
class FragmentContext {
    friend FragmentContextManager;

public:
    FragmentContext() : _cancel_flag(false) {}
    ~FragmentContext() {
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
    TNetworkAddress fe_addr() { return _fe_addr; }
    FragmentFuture finish_future() { return _finish_promise.get_future(); }
    MemTracker* mem_tracker() const { return _mem_tracker.get(); }
    void set_mem_tracker(std::unique_ptr<MemTracker> mem_tracker) { _mem_tracker = std::move(mem_tracker); }
    RuntimeState* runtime_state() const { return _runtime_state.get(); }
    void set_runtime_state(std::shared_ptr<RuntimeState>&& runtime_state) { _runtime_state = std::move(runtime_state); }
    ExecNode* plan() const { return _plan; }
    void set_plan(ExecNode* plan) { _plan = plan; }
    Pipelines& pipelines() { return _pipelines; }
    void set_pipelines(Pipelines&& pipelines) { _pipelines = std::move(pipelines); }
    Drivers& drivers() { return _drivers; }
    void set_drivers(Drivers&& drivers) {
        _drivers = std::move(drivers);
        _num_drivers.store(_drivers.size());
        _final_status.store(nullptr);
    }

    bool count_down_drivers() { return _num_drivers.fetch_sub(1) == 1; }

    void set_num_root_drivers(size_t num_root_drivers) { _num_root_drivers.store(num_root_drivers); }

    bool count_down_root_drivers() { return _num_root_drivers.fetch_sub(1) == 1; }

    void set_final_status(const Status& status) {
        if (_final_status.load() != nullptr) {
            return;
        }
        Status* old_status = nullptr;
        if (_final_status.compare_exchange_strong(old_status, &_s_status)) {
            _s_status = status;
        }
    }

    Status final_status() {
        auto* status = _final_status.load();
        return status == nullptr ? Status::OK() : *status;
    }

    void cancel(const Status& status) {
        _cancel_flag.store(true, std::memory_order_release);
        set_final_status(status);
    }

    void finish() { cancel(Status::OK()); }

    bool is_canceled() { return _cancel_flag.load(std::memory_order_acquire) == true; }

    MorselQueueMap& morsel_queues() { return _morsel_queues; }

    Status prepare_all_pipelines() {
        for (auto& pipe : _pipelines) {
            RETURN_IF_ERROR(pipe->prepare(_runtime_state.get(), _mem_tracker.get()));
        }
        return Status::OK();
    }

    void close_all_pipelines() {
        for (auto& pipe : _pipelines) {
            pipe->close(_runtime_state.get());
        }
    }

private:
    // Id of this query
    TUniqueId _query_id;
    // Id of this instance
    TUniqueId _fragment_instance_id;
    TNetworkAddress _fe_addr;

    // promise used to determine whether fragment finished its execution
    FragmentPromise _finish_promise;

    // never adjust the order of _mem_tracker, _runtime_state, _plan, _pipelines and _drivers, since
    // _plan depends on _runtime_state and _drivers depends on _mem_tracker and _runtime_state.
    std::unique_ptr<MemTracker> _mem_tracker = nullptr;
    std::shared_ptr<RuntimeState> _runtime_state = nullptr;
    ExecNode* _plan = nullptr; // lives in _runtime_state->obj_pool()
    Pipelines _pipelines;
    Drivers _drivers;
    // _morsel_queues is mapping from an source_id to its corresponding
    // MorselQueue that is shared among drivers created from the same pipeline,
    // drivers contend for Morsels from MorselQueue.
    MorselQueueMap _morsel_queues;
    // when _num_root_drivers counts down to zero, means that all the root drivers are finished,
    // the fragment instance produces the entire result required, all the outstanding drivers
    // should finish computation.
    std::atomic<size_t> _num_root_drivers;
    // when _num_drivers counts down to zero, means all drivers has finished, then BE
    // can notify FE via reportExecStatus that fragment instance is done after which
    // FragmentContext can be unregistered safely.
    std::atomic<size_t> _num_drivers;
    std::atomic<Status*> _final_status;
    std::atomic<bool> _cancel_flag;
    Status _s_status;
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
    void unregister(const TUniqueId& fragment_id);
    void cancel(const Status& status);

private:
    std::mutex _lock;
    std::unordered_map<TUniqueId, FragmentContextPtr> _fragment_contexts;
};
} // namespace pipeline
} // namespace starrocks
