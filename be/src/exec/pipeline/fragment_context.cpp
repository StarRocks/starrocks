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

#include "exec/pipeline/fragment_context.h"

#include <memory>

#include "exec/data_sink.h"
#include "exec/pipeline/group_execution/execution_group.h"
#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/pipeline/stream_pipeline_driver.h"
#include "exec/workgroup/work_group.h"
#include "runtime/client_cache.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/exec_env.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/stream_load/transaction_mgr.h"
#include "util/threadpool.h"
#include "util/thrift_rpc_helper.h"
#include "util/time.h"

namespace starrocks::pipeline {

FragmentContext::FragmentContext() : _data_sink(nullptr) {}

FragmentContext::~FragmentContext() {
    _data_sink.reset();
    _runtime_filter_hub.close_all_in_filters(_runtime_state.get());
    close_all_execution_groups();
    if (_plan != nullptr) {
        _plan->close(_runtime_state.get());
    }
}

size_t FragmentContext::total_dop() const {
    size_t total = 0;
    for (const auto& group : _execution_groups) {
        total += group->total_logical_dop();
    }
    return total;
}

void FragmentContext::close_all_execution_groups() {
    for (auto& group : _execution_groups) {
        group->close(_runtime_state.get());
    }
    _execution_groups.clear();
}

void FragmentContext::move_tplan(TPlan& tplan) {
    swap(_tplan, tplan);
}
void FragmentContext::set_data_sink(std::unique_ptr<DataSink> data_sink) {
    _data_sink = std::move(data_sink);
}

void FragmentContext::count_down_execution_group(size_t val) {
    // Note that _pipelines may be destructed after fetch_add
    // memory_order_seq_cst semantics ensure that previous code does not reorder after fetch_add
    size_t total_execution_groups = _execution_groups.size();
    bool all_groups_finished = _num_finished_execution_groups.fetch_add(val) + val == total_execution_groups;
    if (!all_groups_finished) {
        return;
    }

    // dump profile if necessary
    auto* state = runtime_state();
    auto* query_ctx = state->query_ctx();
    state->runtime_profile()->reverse_childs();
    if (config::pipeline_print_profile) {
        std::stringstream ss;
        // Print profile for this fragment
        state->runtime_profile()->compute_time_in_profile();
        state->runtime_profile()->pretty_print(&ss);
        LOG(INFO) << ss.str();
    }

    finish();
    auto status = final_status();
    _workgroup->executors()->driver_executor()->report_exec_state(query_ctx, this, status, true, true);

    if (_report_when_finish) {
        /// TODO: report fragment finish to BE coordinator
        TReportFragmentFinishResponse res;
        TReportFragmentFinishParams params;
        params.__set_query_id(query_id());
        params.__set_fragment_instance_id(fragment_instance_id());
        // params.query_id = query_id();
        // params.fragment_instance_id = fragment_instance_id();
        const auto& fe_addr = state->fragment_ctx()->fe_addr();

        class RpcRunnable : public Runnable {
        public:
            RpcRunnable(const TNetworkAddress& fe_addr, const TReportFragmentFinishResponse& res,
                        const TReportFragmentFinishParams& params)
                    : fe_addr(fe_addr), res(res), params(params) {}
            const TNetworkAddress fe_addr;
            TReportFragmentFinishResponse res;
            const TReportFragmentFinishParams params;

            void run() override {
                (void)ThriftRpcHelper::rpc<FrontendServiceClient>(
                        fe_addr.hostname, fe_addr.port,
                        [&](FrontendServiceConnection& client) { client->reportFragmentFinish(res, params); });
            }
        };
        //
        std::shared_ptr<Runnable> runnable;
        runnable = std::make_shared<RpcRunnable>(fe_addr, res, params);
        (void)state->exec_env()->streaming_load_thread_pool()->submit(runnable);
    }

    destroy_pass_through_chunk_buffer();

    query_ctx->count_down_fragments();
}

bool FragmentContext::need_report_exec_state() {
    auto* state = runtime_state();
    auto* query_ctx = state->query_ctx();
    if (!query_ctx->enable_profile()) {
        return false;
    }
    const auto now = MonotonicNanos();
    const auto interval_ns = query_ctx->get_runtime_profile_report_interval_ns();
    auto last_report_ns = _last_report_exec_state_ns.load();
    return now - last_report_ns >= interval_ns;
}

void FragmentContext::report_exec_state_if_necessary() {
    auto* state = runtime_state();
    auto* query_ctx = state->query_ctx();
    if (!query_ctx->enable_profile()) {
        return;
    }
    const auto now = MonotonicNanos();
    const auto interval_ns = query_ctx->get_runtime_profile_report_interval_ns();
    auto last_report_ns = _last_report_exec_state_ns.load();
    if (now - last_report_ns < interval_ns) {
        return;
    }

    int64_t normalized_report_ns;
    if (now - last_report_ns > 2 * interval_ns) {
        // Maybe the first time, then initialized it.
        normalized_report_ns = now;
    } else {
        // Fix the report interval regardless the noise.
        normalized_report_ns = last_report_ns + interval_ns;
    }
    if (_last_report_exec_state_ns.compare_exchange_strong(last_report_ns, normalized_report_ns)) {
        iterate_pipeline([](const Pipeline* pipeline) {
            for (const auto& driver : pipeline->drivers()) {
                driver->runtime_report_action();
            }
        });
        _workgroup->executors()->driver_executor()->report_exec_state(query_ctx, this, Status::OK(), false, true);
    }
}

void FragmentContext::set_final_status(const Status& status) {
    if (_final_status.load() != nullptr) {
        return;
    }
    Status* old_status = nullptr;
    if (_final_status.compare_exchange_strong(old_status, &_s_status)) {
        _s_status = status;

        _driver_token.reset();

        if (_s_status.is_cancelled()) {
            auto detailed_message = _s_status.detailed_message();
            std::string cancel_msg =
                    fmt::format("[Driver] Canceled, query_id={}, instance_id={}, reason={}", print_id(_query_id),
                                print_id(_fragment_instance_id), detailed_message);
            if (detailed_message == "QueryFinished" || detailed_message == "LimitReach" ||
                detailed_message == "UserCancel" || detailed_message == "TimeOut") {
                LOG(INFO) << cancel_msg;
            } else {
                LOG(WARNING) << cancel_msg;
            }

            const auto* executors = _workgroup != nullptr
                                            ? _workgroup->executors()
                                            : _runtime_state->exec_env()->workgroup_manager()->shared_executors();
            auto* executor = executors->driver_executor();
            iterate_drivers([executor](const DriverPtr& driver) { executor->cancel(driver.get()); });
        }
    }
}

void FragmentContext::set_pipelines(ExecutionGroups&& exec_groups, Pipelines&& pipelines) {
    for (auto& group : exec_groups) {
        if (!group->is_empty()) {
            _execution_groups.emplace_back(std::move(group));
        }
    }
    _pipelines = std::move(pipelines);
}

Status FragmentContext::prepare_all_pipelines() {
    for (auto& group : _execution_groups) {
        RETURN_IF_ERROR(group->prepare_pipelines(_runtime_state.get()));
    }
    return Status::OK();
}

void FragmentContext::set_stream_load_contexts(const std::vector<StreamLoadContext*>& contexts) {
    _stream_load_contexts = std::move(contexts);
    _channel_stream_load = true;
}

void FragmentContext::cancel(const Status& status) {
    if (!status.ok() && _runtime_state != nullptr && _runtime_state->query_ctx() != nullptr) {
        _runtime_state->query_ctx()->release_workgroup_token_once();
    }

    _runtime_state->set_is_cancelled(true);
    set_final_status(status);

    const TQueryOptions& query_options = _runtime_state->query_options();
    if (query_options.query_type == TQueryType::LOAD && (query_options.load_job_type == TLoadJobType::BROKER ||
                                                         query_options.load_job_type == TLoadJobType::INSERT_QUERY ||
                                                         query_options.load_job_type == TLoadJobType::INSERT_VALUES)) {
        ExecEnv::GetInstance()->profile_report_worker()->unregister_pipeline_load(_query_id, _fragment_instance_id);
    }

    if (_stream_load_contexts.size() > 0) {
        for (const auto& stream_load_context : _stream_load_contexts) {
            if (stream_load_context->body_sink) {
                Status st;
                stream_load_context->body_sink->cancel(st);
            }
            if (_channel_stream_load) {
                _runtime_state->exec_env()->stream_context_mgr()->remove_channel_context(stream_load_context);
            }
        }
        _stream_load_contexts.resize(0);
    }
}

FragmentContext* FragmentContextManager::get_or_register(const TUniqueId& fragment_id) {
    std::lock_guard<std::mutex> lock(_lock);
    auto it = _fragment_contexts.find(fragment_id);
    if (it != _fragment_contexts.end()) {
        return it->second.get();
    } else {
        auto&& ctx = std::make_unique<FragmentContext>();
        auto* raw_ctx = ctx.get();
        _fragment_contexts.emplace(fragment_id, std::move(ctx));
        raw_ctx->set_workgroup(ExecEnv::GetInstance()->workgroup_manager()->get_default_workgroup());
        return raw_ctx;
    }
}

Status FragmentContextManager::register_ctx(const TUniqueId& fragment_id, FragmentContextPtr fragment_ctx) {
    std::lock_guard<std::mutex> lock(_lock);

    if (_fragment_contexts.find(fragment_id) != _fragment_contexts.end()) {
        std::stringstream msg;
        msg << "Fragment " << fragment_id << " has been registered";
        LOG(WARNING) << msg.str();
        return Status::InternalError(msg.str());
    }

    // Only register profile report worker for broker load and insert into here,
    // for stream load and routine load, currently we don't need BE to report their progress regularly.
    const TQueryOptions& query_options = fragment_ctx->runtime_state()->query_options();
    if (query_options.query_type == TQueryType::LOAD && (query_options.load_job_type == TLoadJobType::BROKER ||
                                                         query_options.load_job_type == TLoadJobType::INSERT_QUERY ||
                                                         query_options.load_job_type == TLoadJobType::INSERT_VALUES)) {
        RETURN_IF_ERROR(ExecEnv::GetInstance()->profile_report_worker()->register_pipeline_load(
                fragment_ctx->query_id(), fragment_id));
    }
    _fragment_contexts.emplace(fragment_id, std::move(fragment_ctx));
    return Status::OK();
}

FragmentContextPtr FragmentContextManager::get(const TUniqueId& fragment_id) {
    std::lock_guard<std::mutex> lock(_lock);
    auto it = _fragment_contexts.find(fragment_id);
    if (it != _fragment_contexts.end()) {
        return it->second;
    } else {
        return nullptr;
    }
}

void FragmentContextManager::unregister(const TUniqueId& fragment_id) {
    std::lock_guard<std::mutex> lock(_lock);
    auto it = _fragment_contexts.find(fragment_id);
    if (it != _fragment_contexts.end()) {
        it->second->_finish_promise.set_value();

        const TQueryOptions& query_options = it->second->runtime_state()->query_options();
        if (query_options.query_type == TQueryType::LOAD &&
            (query_options.load_job_type == TLoadJobType::BROKER ||
             query_options.load_job_type == TLoadJobType::INSERT_QUERY ||
             query_options.load_job_type == TLoadJobType::INSERT_VALUES) &&
            !it->second->runtime_state()->is_cancelled()) {
            ExecEnv::GetInstance()->profile_report_worker()->unregister_pipeline_load(it->second->query_id(),
                                                                                      fragment_id);
        }
        const auto& stream_load_contexts = it->second->_stream_load_contexts;

        if (stream_load_contexts.size() > 0) {
            for (const auto& stream_load_context : stream_load_contexts) {
                if (stream_load_context->body_sink) {
                    Status st;
                    stream_load_context->body_sink->cancel(st);
                }
                if (it->second->_channel_stream_load) {
                    it->second->_runtime_state->exec_env()->stream_context_mgr()->remove_channel_context(
                            stream_load_context);
                }
            }
            it->second->_stream_load_contexts.resize(0);
        }
        _fragment_contexts.erase(it);
    }
}

void FragmentContextManager::cancel(const Status& status) {
    std::lock_guard<std::mutex> lock(_lock);
    for (auto& _fragment_context : _fragment_contexts) {
        _fragment_context.second->cancel(status);
    }
}
void FragmentContext::prepare_pass_through_chunk_buffer() {
    _runtime_state->exec_env()->stream_mgr()->prepare_pass_through_chunk_buffer(_query_id);
}
void FragmentContext::destroy_pass_through_chunk_buffer() {
    if (_runtime_state) {
        _runtime_state->exec_env()->stream_mgr()->destroy_pass_through_chunk_buffer(_query_id);
    }
}

Status FragmentContext::reset_epoch() {
    _num_finished_epoch_pipelines = 0;
    const std::function<Status(Pipeline*)> caller = [this](Pipeline* pipeline) {
        RETURN_IF_ERROR(_runtime_state->reset_epoch());
        RETURN_IF_ERROR(pipeline->reset_epoch(_runtime_state.get()));
        return Status::OK();
    };
    return iterate_pipeline(caller);
}

void FragmentContext::count_down_epoch_pipeline(RuntimeState* state, size_t val) {
    size_t total_execution_groups = _execution_groups.size();
    bool all_groups_finished = _num_finished_epoch_pipelines.fetch_add(val) + val == total_execution_groups;
    if (!all_groups_finished) {
        return;
    }

    state->query_ctx()->stream_epoch_manager()->count_down_fragment_ctx(state, this);
}

void FragmentContext::init_jit_profile() {
    if (runtime_state() && runtime_state()->is_jit_enabled() && runtime_state()->runtime_profile()) {
        _jit_timer = ADD_TIMER(_runtime_state->runtime_profile(), "JITTotalCostTime");
        _jit_counter = ADD_COUNTER(_runtime_state->runtime_profile(), "JITCounter", TUnit::UNIT);
    }
}

void FragmentContext::update_jit_profile(int64_t time_ns) {
    if (_jit_counter != nullptr) {
        COUNTER_UPDATE(_jit_counter, 1);
    }

    if (_jit_timer != nullptr) {
        COUNTER_UPDATE(_jit_timer, time_ns);
    }
}
void FragmentContext::iterate_pipeline(const std::function<void(Pipeline*)>& call) {
    for (auto& group : _execution_groups) {
        group->for_each_pipeline(call);
    }
}

Status FragmentContext::iterate_pipeline(const std::function<Status(Pipeline*)>& call) {
    for (auto& group : _execution_groups) {
        RETURN_IF_ERROR(group->for_each_pipeline(call));
    }
    return Status::OK();
}

Status FragmentContext::prepare_active_drivers() {
    for (auto& group : _execution_groups) {
        RETURN_IF_ERROR(group->prepare_drivers(_runtime_state.get()));
    }
    return Status::OK();
}

Status FragmentContext::submit_active_drivers(DriverExecutor* executor) {
    for (auto& group : _execution_groups) {
        group->attach_driver_executor(executor);
        group->submit_active_drivers();
    }
    return Status::OK();
}

} // namespace starrocks::pipeline
