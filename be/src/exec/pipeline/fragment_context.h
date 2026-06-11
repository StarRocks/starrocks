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
#include <functional>
#include <map>
#include <memory>
#include <memory_resource>
#include <unordered_map>
#include <vector>

#include "base/hash/hash_std.hpp"
#include "base/time/time.h"
#include "base/uid_util.h"
#include "compute_env/pipeline/driver_limiter.h"
#include "compute_env/pipeline/pipeline_timer_context.h"
#include "compute_env/query_cache/cache_param.h"
#include "compute_env/workgroup/work_group_fwd.h"
#include "exec/exec_node.h"
#include "exec/pipeline/adaptive/adaptive_dop_param.h"
#include "exec/pipeline/group_execution/execution_group_fwd.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/primitives/fragment_lifecycle.h"
#include "exec/pipeline/runtime_filter_hub.h"
#include "exec/pipeline/scan/morsel_queue.h"
#include "exec/runtime/fragment_runtime_state.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/HeartbeatService.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/QueryPlanExtra_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/runtime_filter_worker.h"
#include "runtime/runtime_state.h"
#include "storage/primitive/predicate_tree_params.h"

namespace starrocks {

class StreamLoadContext;
class FragmentDictState;

namespace pipeline {

class PassThroughChunkBufferGuard;

using RuntimeFilterPort = starrocks::RuntimeFilterPort;
using PerDriverScanRangesMap = std::map<int32_t, std::vector<TScanRangeParams>>;

class FragmentContext : public FragmentLifecycle {
    friend class FragmentContextManager;

public:
    FragmentContext();
    ~FragmentContext();

    // Fragment-level shared MemPool — delegates to RuntimeState which owns it.
    MemPool* fragment_mem_pool() { return _runtime_state ? _runtime_state->fragment_mem_pool() : nullptr; }

    // PMR memory resource — delegates to RuntimeState which owns it.
    std::pmr::memory_resource* mem_resource() { return _runtime_state ? _runtime_state->mem_resource() : nullptr; }

    const TUniqueId& query_id() const { return _query_id; }
    void set_query_id(const TUniqueId& query_id) { _query_id = query_id; }
    const TUniqueId& fragment_instance_id() const { return _fragment_runtime_state.fragment_instance_id(); }
    void set_fragment_instance_id(const TUniqueId& fragment_instance_id) {
        _fragment_runtime_state.set_fragment_instance_id(fragment_instance_id);
    }
    FragmentRuntimeState& fragment_runtime_state() { return _fragment_runtime_state; }
    const FragmentRuntimeState& fragment_runtime_state() const { return _fragment_runtime_state; }
    void set_fe_addr(const TNetworkAddress& fe_addr) { _fragment_runtime_state.set_fe_addr(fe_addr); }
    const TNetworkAddress& fe_addr() const { return _fragment_runtime_state.fe_addr(); }
    FragmentFuture finish_future() { return _finish_promise.get_future(); }
    RuntimeState* runtime_state() const { return _runtime_state.get(); }
    std::shared_ptr<RuntimeState> runtime_state_ptr() { return _runtime_state; }
    void set_runtime_state(std::shared_ptr<RuntimeState>&& runtime_state) { _runtime_state = std::move(runtime_state); }
    void attach_to_runtime_state(RuntimeState* state);
    FragmentDictState* dict_state() const { return _fragment_dict_state.get(); }
    ExecNode*& plan() { return _plan; }

    void move_tplan(TPlan& tplan);
    const TPlan& tplan() const { return _tplan; }
    void set_data_sink(std::unique_ptr<DataSink> data_sink);

    size_t total_dop() const;

    bool all_execution_groups_finished() const { return _num_finished_execution_groups == _execution_groups.size(); }
    void count_down_execution_group(size_t val = 1);
    void on_execution_group_finished() override { count_down_execution_group(); }

    bool need_report_exec_state();
    void report_exec_state_if_necessary();

    void set_final_status(const Status& status);

    Status final_status() const { return _fragment_runtime_state.final_status(); }

    void cancel(const Status& status, bool cancelled_by_fe = false);

    void finish() { cancel(Status::OK()); }

    MorselQueueFactoryMap& morsel_queue_factories() { return _morsel_queue_factories; }

    void set_pipelines(ExecutionGroups&& exec_groups, Pipelines&& pipelines);

    Status prepare_all_pipelines();

    void instantiate_drivers(Pipeline* pipeline);

    void iterate_drivers(const std::function<void(const DriverPtr&)>& call);

    void clear_all_drivers();
    void close_all_execution_groups();

    RuntimeFilterHub* runtime_filter_hub() { return _fragment_runtime_state.runtime_filter_hub(); }
    const RuntimeFilterHub* runtime_filter_hub() const { return _fragment_runtime_state.runtime_filter_hub(); }

    RuntimeFilterPort* runtime_filter_port() { return _runtime_state->runtime_filter_port(); }

    void prepare_pass_through_chunk_buffer();
    void destroy_pass_through_chunk_buffer();

    void set_driver_token(DriverLimiter::TokenPtr driver_token) { _driver_token = std::move(driver_token); }
    Status set_pipeline_timer(PipelineTimer* pipeline_timer);
    void clear_pipeline_timer();
    PipelineTimerContextPtr pipeline_timer_context() const { return _pipeline_timer_context; }

    query_cache::CacheParam& cache_param() { return _cache_param; }

    void set_enable_cache(bool flag) { _fragment_runtime_state.set_enable_cache(flag); }

    bool enable_cache() const { return _fragment_runtime_state.enable_cache(); }

    void set_stream_load_contexts(const std::vector<StreamLoadContext*>& contexts);

    void set_enable_adaptive_dop(bool val) { _fragment_runtime_state.set_enable_adaptive_dop(val); }
    bool enable_adaptive_dop() const { return _fragment_runtime_state.enable_adaptive_dop(); }
    AdaptiveDopParam& adaptive_dop_param() { return _adaptive_dop_param; }

    const PredicateTreeParams& pred_tree_params() const { return _fragment_runtime_state.pred_tree_params(); }
    void set_pred_tree_params(const PredicateTreeParams& params) {
        _fragment_runtime_state.set_pred_tree_params(params);
    }

    size_t next_driver_id() { return _next_driver_id++; }

    void set_workgroup(workgroup::WorkGroupPtr wg) { _fragment_runtime_state.set_workgroup(std::move(wg)); }
    const workgroup::WorkGroupPtr& workgroup() const { return _fragment_runtime_state.workgroup(); }
    bool enable_resource_group() const { return workgroup() != nullptr; }
    TQueryType::type query_type() const;

    size_t expired_log_count() { return _expired_log_count; }

    void set_expired_log_count(size_t val) { _expired_log_count = val; }

    void init_jit_profile();

    void update_jit_profile(int64_t time_ns);

    void iterate_pipeline(const std::function<void(Pipeline*)>& call);
    Status iterate_pipeline(const std::function<Status(Pipeline*)>& call);

    Status prepare_active_drivers();
    Status submit_active_drivers(DriverExecutor* executor);

    bool enable_group_execution() const { return _enable_group_execution; }
    void set_enable_group_execution(bool enable_group_execution) { _enable_group_execution = enable_group_execution; }

    void set_report_when_finish(bool report) { _report_when_finish = report; }

    // acquire runtime filter from cache
    void acquire_runtime_filters();

    bool enable_event_scheduler() const { return event_scheduler() != nullptr; }
    EventScheduler* event_scheduler() const { return _event_scheduler.get(); }
    void init_event_scheduler();

private:
    void _set_default_workgroup();
    void _close_stream_load_contexts();

    bool _enable_group_execution = false;
    // Id of this query
    TUniqueId _query_id;
    // Id of this instance
    FragmentRuntimeState _fragment_runtime_state;

    // Hold tplan data datasink from delivery request to create driver lazily
    // after delivery request has been finished.
    TPlan _tplan;
    std::unique_ptr<DataSink> _data_sink;

    // promise used to determine whether fragment finished its execution
    FragmentPromise _finish_promise;

    // never adjust the order of _runtime_state, _plan, _pipelines and _drivers, since
    // _plan depends on _runtime_state and _drivers depends on _runtime_state.
    std::shared_ptr<RuntimeState> _runtime_state = nullptr;
    std::unique_ptr<FragmentDictState> _fragment_dict_state;
    ExecNode* _plan = nullptr; // lives in _runtime_state->obj_pool()
    size_t _next_driver_id = 0;
    Pipelines _pipelines;
    ExecutionGroups _execution_groups;
    std::atomic<size_t> _num_finished_execution_groups = 0;

    std::unique_ptr<EventScheduler> _event_scheduler;
    PipelineTimerContextPtr _pipeline_timer_context = nullptr;
    std::shared_ptr<PipelineTimerTask> _timeout_task = nullptr;
    std::shared_ptr<PipelineTimerTask> _report_state_task = nullptr;

    MorselQueueFactoryMap _morsel_queue_factories;
    DriverLimiter::TokenPtr _driver_token = nullptr;

    std::unique_ptr<PassThroughChunkBufferGuard> _pass_through_chunk_buffer_guard;

    query_cache::CacheParam _cache_param;
    std::vector<StreamLoadContext*> _stream_load_contexts;

    AdaptiveDopParam _adaptive_dop_param;

    size_t _expired_log_count = 0;

    std::atomic<int64_t> _last_report_exec_state_ns = MonotonicNanos();

    RuntimeProfile::Counter* _jit_counter = nullptr;
    RuntimeProfile::Counter* _jit_timer = nullptr;

    bool _report_when_finish{};
};
} // namespace pipeline
} // namespace starrocks
