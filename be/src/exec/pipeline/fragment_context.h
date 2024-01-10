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

#include <unordered_map>

#include "exec/exec_node.h"
#include "exec/pipeline/adaptive/adaptive_dop_param.h"
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

// clang-format off
template <typename T>
concept DriverPtrCallable = std::invocable<T, const DriverPtr&> &&
        (std::same_as<std::invoke_result_t<T, const DriverPtr&>, void> ||
         std::same_as<std::invoke_result_t<T, const DriverPtr&>, Status>);
// clang-format on

class FragmentContext {
    friend FragmentContextManager;

public:
    FragmentContext();
    ~FragmentContext();
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

    void move_tplan(TPlan& tplan);
    const TPlan& tplan() const { return _tplan; }
    void set_data_sink(std::unique_ptr<DataSink> data_sink);

    size_t total_dop() const;
    Pipelines& pipelines() { return _pipelines; }
    void set_pipelines(Pipelines&& pipelines) { _pipelines = std::move(pipelines); }
    size_t num_drivers() const;

    bool all_pipelines_finished() const { return _num_finished_pipelines == _pipelines.size(); }
    void count_down_pipeline(size_t val = 1);

    bool need_report_exec_state();
    void report_exec_state_if_necessary();

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

    template <DriverPtrCallable Func>
    [[nodiscard]] auto iterate_drivers(Func call) {
        using ReturnType = std::invoke_result_t<Func, const DriverPtr&>;

        for (const auto& pipeline : _pipelines) {
            for (const auto& driver : pipeline->drivers()) {
                if constexpr (std::is_same_v<ReturnType, Status>) {
                    RETURN_IF_ERROR(call(driver));
                } else {
                    call(driver);
                }
            }
        }
        if constexpr (std::is_same_v<ReturnType, Status>) {
            return Status::OK();
        }
    }

    void clear_all_drivers();
    void close_all_pipelines();

    RuntimeFilterHub* runtime_filter_hub() { return &_runtime_filter_hub; }

    RuntimeFilterPort* runtime_filter_port() { return _runtime_state->runtime_filter_port(); }

    void prepare_pass_through_chunk_buffer();
    void destroy_pass_through_chunk_buffer();

    void set_driver_token(DriverLimiter::TokenPtr driver_token) { _driver_token = std::move(driver_token); }

    query_cache::CacheParam& cache_param() { return _cache_param; }

    void set_enable_cache(bool flag) { _enable_cache = flag; }

    bool enable_cache() const { return _enable_cache; }

    void set_stream_load_contexts(const std::vector<StreamLoadContext*>& contexts);

    void set_enable_adaptive_dop(bool val) { _enable_adaptive_dop = val; }
    bool enable_adaptive_dop() const { return _enable_adaptive_dop; }
    AdaptiveDopParam& adaptive_dop_param() { return _adaptive_dop_param; }

    size_t next_driver_id() { return _next_driver_id++; }

    void set_workgroup(workgroup::WorkGroupPtr wg) { _workgroup = std::move(wg); }
    const workgroup::WorkGroupPtr& workgroup() const { return _workgroup; }
    bool enable_resource_group() const { return _workgroup != nullptr; }

    // STREAM MV
    Status reset_epoch();
    void set_is_stream_pipeline(bool is_stream_pipeline) { _is_stream_pipeline = is_stream_pipeline; }
    bool is_stream_pipeline() const { return _is_stream_pipeline; }
    void count_down_epoch_pipeline(RuntimeState* state, size_t val = 1);

    // for ut
    void set_is_stream_test(bool is_stream_test) { _is_stream_test = is_stream_test; }
    bool is_stream_test() const { return _is_stream_test; }

    size_t expired_log_count() { return _expired_log_count; }

    void set_expired_log_count(size_t val) { _expired_log_count = val; }

private:
    // Id of this query
    TUniqueId _query_id;
    // Id of this instance
    TUniqueId _fragment_instance_id;
    TNetworkAddress _fe_addr;

    // Hold tplan data datasink from delivery request to create driver lazily
    // after delivery request has been finished.
    TPlan _tplan;
    std::unique_ptr<DataSink> _data_sink;

    // promise used to determine whether fragment finished its execution
    FragmentPromise _finish_promise;

    // never adjust the order of _runtime_state, _plan, _pipelines and _drivers, since
    // _plan depends on _runtime_state and _drivers depends on _runtime_state.
    std::shared_ptr<RuntimeState> _runtime_state = nullptr;
    ExecNode* _plan = nullptr; // lives in _runtime_state->obj_pool()
    size_t _next_driver_id = 0;
    Pipelines _pipelines;
    std::atomic<size_t> _num_finished_pipelines = 0;

    RuntimeFilterHub _runtime_filter_hub;

    MorselQueueFactoryMap _morsel_queue_factories;
    workgroup::WorkGroupPtr _workgroup = nullptr;

    std::atomic<Status*> _final_status = nullptr;
    Status _s_status;

    DriverLimiter::TokenPtr _driver_token = nullptr;

    query_cache::CacheParam _cache_param;
    bool _enable_cache = false;
    std::vector<StreamLoadContext*> _stream_load_contexts;
    bool _channel_stream_load = false;

    // STREAM MV
    std::atomic<size_t> _num_finished_epoch_pipelines = 0;
    bool _is_stream_pipeline = false;
    bool _is_stream_test = false;

    bool _enable_adaptive_dop = false;
    AdaptiveDopParam _adaptive_dop_param;

    size_t _expired_log_count = 0;

    std::atomic<int64_t> _last_report_exec_state_ns = MonotonicNanos();
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
