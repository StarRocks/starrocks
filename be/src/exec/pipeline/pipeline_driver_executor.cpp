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

#include "exec/pipeline/pipeline_driver_executor.h"

#include <memory>

#include "agent/master_info.h"
#include "exec/pipeline/pipeline_metrics.h"
#include "exec/pipeline/stream_pipeline_driver.h"
#include "exec/workgroup/work_group.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "util/debug/query_trace.h"
#include "util/defer_op.h"
#include "util/failpoint/fail_point.h"
#include "util/stack_util.h"
#include "util/starrocks_metrics.h"

namespace starrocks::pipeline {

GlobalDriverExecutor::GlobalDriverExecutor(const std::string& name, std::unique_ptr<ThreadPool> thread_pool,
                                           bool enable_resource_group, const CpuUtil::CpuIds& cpuids,
                                           PipelineExecutorMetrics* metrics)
        : Base("pip_exec_" + name),
          _driver_queue(enable_resource_group
                                ? std::unique_ptr<DriverQueue>(
                                          std::make_unique<WorkGroupDriverQueue>(metrics->get_driver_queue_metrics()))
                                : std::make_unique<QuerySharedDriverQueue>(metrics->get_driver_queue_metrics())),
          _thread_pool(std::move(thread_pool)),
          _blocked_driver_poller(
                  new PipelineDriverPoller(name, _driver_queue.get(), cpuids, metrics->get_poller_metrics())),
          _exec_state_reporter(new ExecStateReporter(cpuids)),
          _audit_statistics_reporter(new AuditStatisticsReporter()),
          _metrics(metrics->get_driver_executor_metrics()) {}

void GlobalDriverExecutor::close() {
    _driver_queue->close();
    _thread_pool->wait();
    _blocked_driver_poller->shutdown();
}

void GlobalDriverExecutor::initialize(int num_threads) {
    _blocked_driver_poller->start();
    _num_threads_setter.set_actual_num(num_threads);
    for (auto i = 0; i < num_threads; ++i) {
        (void)_thread_pool->submit_func([this]() { this->_worker_thread(); });
    }
}

void GlobalDriverExecutor::change_num_threads(int32_t num_threads) {
    int32_t old_num_threads = 0;
    if (!_num_threads_setter.adjust_expect_num(num_threads, &old_num_threads)) {
        return;
    }
    for (int i = old_num_threads; i < num_threads; ++i) {
        if (_num_threads_setter.should_expand()) {
            (void)_thread_pool->submit_func([this]() { this->_worker_thread(); });
        }
    }
}

void GlobalDriverExecutor::_finalize_driver(DriverRawPtr driver, RuntimeState* runtime_state, DriverState state) {
    DCHECK(driver);
    driver->finalize(runtime_state, state);
}

void GlobalDriverExecutor::_worker_thread() {
    auto current_thread = Thread::current_thread();
    const int worker_id = _next_id++;
    std::queue<DriverRawPtr> local_driver_queue;
    while (true) {
        if (local_driver_queue.empty() && _num_threads_setter.should_shrink()) {
            break;
        }
        // Reset TLS state
        CurrentThread::current().set_query_id({});
        CurrentThread::current().set_fragment_instance_id({});
        CurrentThread::current().set_pipeline_driver_id(0);

        if (current_thread != nullptr) {
            current_thread->set_idle(true);
        }

        auto maybe_driver = _get_next_driver(local_driver_queue);
        if (maybe_driver.status().is_cancelled()) {
            return;
        }
        auto* driver = maybe_driver.value();
        if (driver == nullptr) {
            continue;
        }
        DCHECK(!driver->is_in_ready());
        DCHECK(!driver->is_in_blocked());

        if (current_thread != nullptr) {
            current_thread->set_idle(false);
        }
        auto* query_ctx = driver->query_ctx();
        auto* fragment_ctx = driver->fragment_ctx();

        driver->increment_schedule_times();
        _metrics->driver_schedule_count.increment(1);

        SCOPED_SET_TRACE_INFO(driver->driver_id(), query_ctx->query_id(), fragment_ctx->fragment_instance_id());

        SET_THREAD_LOCAL_QUERY_TRACE_CONTEXT(query_ctx->query_trace(), fragment_ctx->fragment_instance_id(), driver);

        // TODO(trueeyu): This writing is to ensure that MemTracker will not be destructed before the thread ends.
        //  This writing method is a bit tricky, and when there is a better way, replace it
        auto runtime_state_ptr = fragment_ctx->runtime_state_ptr();
        auto* runtime_state = runtime_state_ptr.get();
        {
            SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(runtime_state->instance_mem_tracker());
#if !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && !defined(THREAD_SANITIZER)
            FAIL_POINT_SCOPE(mem_alloc_error);
#endif
            if (fragment_ctx->is_canceled()) {
                driver->cancel_operators(runtime_state);
                if (driver->is_still_pending_finish()) {
                    driver->set_driver_state(DriverState::PENDING_FINISH);
                    _blocked_driver_poller->add_blocked_driver(driver);
                } else if (driver->is_still_epoch_finishing()) {
                    driver->set_driver_state(DriverState::EPOCH_PENDING_FINISH);
                    _blocked_driver_poller->add_blocked_driver(driver);
                } else {
                    _finalize_driver(driver, runtime_state, DriverState::CANCELED);
                }
                continue;
            } else if (driver->is_finished()) {
                // a blocked driver is canceled because of fragment cancellation or query expiration.
                _finalize_driver(driver, runtime_state, driver->driver_state());
                continue;
            } else if (!driver->is_ready()) {
                // Offer blocked driver a chance to trigger profile report.
                driver->report_exec_state_if_necessary();
                _blocked_driver_poller->add_blocked_driver(driver);
                continue;
            }

            StatusOr<DriverState> maybe_state;
            int64_t start_time = driver->get_active_time();
            _metrics->exec_running_tasks.increment(1);
#ifdef NDEBUG
            TRY_CATCH_ALL(maybe_state, driver->process(runtime_state, worker_id));
#else
            maybe_state = driver->process(runtime_state, worker_id);
#endif
            if (current_thread != nullptr) {
                current_thread->inc_finished_tasks();
            }
            Status status = maybe_state.status();
            this->_driver_queue->update_statistics(driver);
            int64_t end_time = driver->get_active_time();
            _metrics->driver_execution_time.increment(end_time - start_time);
            _metrics->exec_running_tasks.increment(-1);
            _metrics->exec_finished_tasks.increment(1);

            // Check big query
            if (!driver->is_query_never_expired() && status.ok() && driver->workgroup()) {
                status = driver->workgroup()->check_big_query(*query_ctx);
            }

            if (!status.ok()) {
                auto o_id = get_backend_id();
                int64_t be_id = o_id.has_value() ? o_id.value() : -1;
                status = status.clone_and_append(fmt::format("BE:{}", be_id));
                LOG(WARNING) << "[Driver] Process error, query_id=" << print_id(driver->query_ctx()->query_id())
                             << ", instance_id=" << print_id(driver->fragment_ctx()->fragment_instance_id())
                             << ", status=" << status;
                driver->runtime_profile()->add_info_string("ErrorMsg", std::string(status.message()));
                query_ctx->cancel(status);
                driver->cancel_operators(runtime_state);
                if (driver->is_still_pending_finish()) {
                    driver->set_driver_state(DriverState::PENDING_FINISH);
                    _blocked_driver_poller->add_blocked_driver(driver);
                } else {
                    _finalize_driver(driver, runtime_state, DriverState::INTERNAL_ERROR);
                }
                continue;
            }

            driver->report_exec_state_if_necessary();

            auto driver_state = maybe_state.value();
            switch (driver_state) {
            case READY:
            case RUNNING: {
                driver->driver_acct().clean_local_queue_infos();
                this->_driver_queue->put_back_from_executor(driver);
                break;
            }
            case LOCAL_WAITING: {
                driver->driver_acct().update_enter_local_queue_timestamp();
                local_driver_queue.push(driver);
                break;
            }
            case FINISH:
            case CANCELED:
            case INTERNAL_ERROR: {
                _finalize_driver(driver, runtime_state, driver_state);
                break;
            }
            case EPOCH_FINISH: {
                _finalize_epoch(driver, runtime_state, driver_state);
                _blocked_driver_poller->park_driver(driver);
                break;
            }
            case INPUT_EMPTY:
            case OUTPUT_FULL:
            case PENDING_FINISH:
            case EPOCH_PENDING_FINISH:
            case PRECONDITION_BLOCK: {
                _blocked_driver_poller->add_blocked_driver(driver);
                break;
            }
            default:
                DCHECK(false);
            }
        }
    }
}

StatusOr<DriverRawPtr> GlobalDriverExecutor::_get_next_driver(std::queue<DriverRawPtr>& local_driver_queue) {
    DriverRawPtr driver = nullptr;
    if (!local_driver_queue.empty()) {
        const size_t local_driver_num = local_driver_queue.size();
        for (size_t i = 0; i < local_driver_num; i++) {
            driver = local_driver_queue.front();
            local_driver_queue.pop();
            if (driver->source_operator()->has_output()) {
                return driver;
            } else {
                if (driver->driver_acct().get_local_queue_time_spent() > LOCAL_MAX_WAIT_TIME_SPENT_NS) {
                    driver->set_driver_state(DriverState::INPUT_EMPTY);
                    _blocked_driver_poller->add_blocked_driver(driver);
                } else {
                    local_driver_queue.push(driver);
                }
                driver = nullptr;
            }
        }
    }

    // If local driver queue is not empty, we cannot block here. Otherwise these local drivers may not be scheduled until
    // ready queue is not empty.
    const bool need_block = local_driver_queue.empty();
    return this->_driver_queue->take(need_block);
}

void GlobalDriverExecutor::submit(DriverRawPtr driver) {
    driver->start_timers();
    if (driver->fragment_ctx()->enable_event_scheduler()) {
        driver->fragment_ctx()->event_scheduler()->attach_queue(_driver_queue.get());
    }

    if (driver->is_precondition_block()) {
        driver->set_driver_state(DriverState::PRECONDITION_BLOCK);
        driver->mark_precondition_not_ready();
        this->_blocked_driver_poller->add_blocked_driver(driver);
    } else {
        if (driver->has_precondition() && !driver->precondition_prepared()) driver->mark_precondition_ready();

        driver->submit_operators();

        // Try to add the driver to poller first.
        if (!driver->source_operator()->is_finished() && !driver->source_operator()->has_output()) {
            if (typeid(*driver) == typeid(StreamPipelineDriver)) {
                driver->set_driver_state(DriverState::EPOCH_FINISH);
                this->_blocked_driver_poller->park_driver(driver);
            } else {
                driver->set_driver_state(DriverState::INPUT_EMPTY);
                this->_blocked_driver_poller->add_blocked_driver(driver);
            }
        } else {
            this->_driver_queue->put_back(driver);
        }
    }
}

void GlobalDriverExecutor::cancel(DriverRawPtr driver) {
    // if driver is already in ready queue, we should cancel it
    // otherwise, just ignore it and wait for the poller to schedule
    if (driver->is_in_ready()) {
        this->_driver_queue->cancel(driver);
    }
}

void GlobalDriverExecutor::report_exec_state(QueryContext* query_ctx, FragmentContext* fragment_ctx,
                                             const Status& status, bool done, bool attach_profile) {
    auto* profile = fragment_ctx->runtime_state()->runtime_profile();
    ObjectPool obj_pool;
    if (attach_profile) {
        profile = _build_merged_instance_profile(query_ctx, fragment_ctx, &obj_pool);

        // Add counters for query level memory and cpu usage, these two metrics will be specially handled at the frontend
        auto* query_peak_memory = profile->add_counter(
                "QueryPeakMemoryUsage", TUnit::BYTES,
                RuntimeProfile::Counter::create_strategy(TUnit::BYTES, TCounterMergeType::SKIP_FIRST_MERGE));
        query_peak_memory->set(query_ctx->mem_cost_bytes());
        auto* query_cumulative_cpu = profile->add_counter(
                "QueryCumulativeCpuTime", TUnit::TIME_NS,
                RuntimeProfile::Counter::create_strategy(TUnit::TIME_NS, TCounterMergeType::SKIP_FIRST_MERGE));
        query_cumulative_cpu->set(query_ctx->cpu_cost());
        auto* query_spill_bytes = profile->add_counter(
                "QuerySpillBytes", TUnit::BYTES,
                RuntimeProfile::Counter::create_strategy(TUnit::BYTES, TCounterMergeType::SKIP_FIRST_MERGE));
        query_spill_bytes->set(query_ctx->get_spill_bytes());
        // Add execution wall time
        auto* query_exec_wall_time = profile->add_counter(
                "QueryExecutionWallTime", TUnit::TIME_NS,
                RuntimeProfile::Counter::create_strategy(TUnit::TIME_NS, TCounterMergeType::SKIP_FIRST_MERGE));
        query_exec_wall_time->set(query_ctx->lifetime());
    }

    const auto& fe_addr = fragment_ctx->fe_addr();
    if (fe_addr.hostname.empty()) {
        // query executed by external connectors, like spark and flink connector,
        // does not need to report exec state to FE, so return if fe addr is empty.
        return;
    }

    // Load channel profile will be merged on FE
    auto* load_channel_profile = fragment_ctx->runtime_state()->load_channel_profile();
    std::shared_ptr<TReportExecStatusParams> params;
    {
        // move profile memory to process, similar with SinkBuffer. the params will be released in ExecStateReporter
        int64_t before_bytes = CurrentThread::current().get_consumed_bytes();
        params = ExecStateReporter::create_report_exec_status_params(query_ctx, fragment_ctx, profile,
                                                                     load_channel_profile, status, done);
        int64_t delta = CurrentThread::current().get_consumed_bytes() - before_bytes;

        CurrentThread::current().mem_release(delta);
        GlobalEnv::GetInstance()->process_mem_tracker()->consume(delta);
    }

    auto exec_env = fragment_ctx->runtime_state()->exec_env();
    auto fragment_id = fragment_ctx->fragment_instance_id();

    auto report_task = [params, exec_env, fe_addr, fragment_id]() {
        int retry_times = 0;
        while (retry_times++ < 3) {
            auto status = ExecStateReporter::report_exec_status(*params, exec_env, fe_addr);
            if (!status.ok()) {
                if (status.is_not_found()) {
                    VLOG(1) << "[Driver] Fail to report exec state due to query not found: fragment_instance_id="
                            << print_id(fragment_id);
                } else {
                    LOG(WARNING) << "[Driver] Fail to report exec state: fragment_instance_id=" << print_id(fragment_id)
                                 << ", status: " << status.to_string() << ", retry_times=" << retry_times;
                    // if it is done exec state report, we should retry
                    if (params->__isset.done && params->done) {
                        continue;
                    }
                }
            } else {
                VLOG(1) << "[Driver] Succeed to report exec state: fragment_instance_id=" << print_id(fragment_id)
                        << ", is_done=" << params->done;
            }
            break;
        }
    };

    // if it is done exec state report, We need to ensure that this report is executed with priority
    // and is retried as much as possible to ensure success.
    // Otherwise, it may result in the query or ingestion status getting stuck.
    this->_exec_state_reporter->submit(std::move(report_task), done);
    VLOG(2) << "[Driver] Submit exec state report task: fragment_instance_id=" << print_id(fragment_id)
            << ", is_done=" << done;
}

void GlobalDriverExecutor::report_audit_statistics(QueryContext* query_ctx, FragmentContext* fragment_ctx) {
    auto query_statistics = query_ctx->final_query_statistic();

    TReportAuditStatisticsParams params;
    params.__set_query_id(fragment_ctx->query_id());
    params.__set_fragment_instance_id(fragment_ctx->fragment_instance_id());
    params.__set_audit_statistics({});
    query_statistics->to_params(&params.audit_statistics);

    auto fe_addr = fragment_ctx->fe_addr();
    if (fe_addr.hostname.empty()) {
        // query executed by external connectors, like spark and flink connector,
        // does not need to report exec state to FE, so return if fe addr is empty.
        return;
    }

    auto exec_env = fragment_ctx->runtime_state()->exec_env();
    auto fragment_id = fragment_ctx->fragment_instance_id();

    auto report_task = [=]() {
        auto status = AuditStatisticsReporter::report_audit_statistics(params, exec_env, fe_addr);
        if (!status.ok()) {
            if (status.is_not_found()) {
                LOG(INFO) << "[Driver] Fail to report audit statistics due to query not found: fragment_instance_id="
                          << print_id(fragment_id);
            } else {
                LOG(WARNING) << "[Driver] Fail to report audit statistics fragment_instance_id="
                             << print_id(fragment_id) << ", status: " << status.to_string();
            }
        } else {
            VLOG(1) << "[Driver] Succeed to report audit statistics: fragment_instance_id=" << print_id(fragment_id);
        }
    };
    auto st = this->_audit_statistics_reporter->submit(std::move(report_task));
    if (!st.ok()) {
        LOG(ERROR) << "submit audit statistics report fail, " << st.to_string();
    }
}

size_t GlobalDriverExecutor::activate_parked_driver(const ConstDriverPredicator& predicate_func) {
    return _blocked_driver_poller->activate_parked_driver(predicate_func);
}

size_t GlobalDriverExecutor::calculate_parked_driver(const ConstDriverPredicator& predicate_func) const {
    return _blocked_driver_poller->calculate_parked_driver(predicate_func);
}

void GlobalDriverExecutor::_finalize_epoch(DriverRawPtr driver, RuntimeState* runtime_state, DriverState state) {
    DCHECK(driver);
    DCHECK(down_cast<StreamPipelineDriver*>(driver));
    auto* stream_driver = down_cast<StreamPipelineDriver*>(driver);
    stream_driver->epoch_finalize(runtime_state, state);
}

void GlobalDriverExecutor::report_epoch(ExecEnv* exec_env, QueryContext* query_ctx,
                                        std::vector<FragmentContext*> fragment_ctxs) {
    DCHECK_LT(0, fragment_ctxs.size());
    auto params = ExecStateReporter::create_report_epoch_params(query_ctx, fragment_ctxs);
    // TODO(lism): Check all fragment_ctx's fe_addr are the same.
    auto fe_addr = fragment_ctxs[0]->fe_addr();
    auto query_id = query_ctx->query_id();
    auto report_task = [=]() {
        auto status = ExecStateReporter::report_epoch(params, exec_env, fe_addr);
        if (!status.ok()) {
            if (status.is_not_found()) {
                LOG(INFO) << "[Driver] Fail to report epoch exec state due to query not found: query_id="
                          << print_id(query_id);
            } else {
                LOG(WARNING) << "[Driver] Fail to report epoch exec state: query_id=" << print_id(query_id)
                             << ", status: " << status.to_string();
            }
        } else {
            VLOG(1) << "[Driver] Succeed to report epoch exec state: query_id=" << print_id(query_id);
        }
    };

    this->_exec_state_reporter->submit(std::move(report_task));
}

void GlobalDriverExecutor::iterate_immutable_blocking_driver(const ConstDriverConsumer& call) const {
    _blocked_driver_poller->for_each_driver(call);
}

RuntimeProfile* GlobalDriverExecutor::_build_merged_instance_profile(QueryContext* query_ctx,
                                                                     FragmentContext* fragment_ctx,
                                                                     ObjectPool* obj_pool) {
    auto* instance_profile = fragment_ctx->runtime_state()->runtime_profile();
    if (!query_ctx->enable_profile()) {
        return instance_profile;
    }

    if (query_ctx->profile_level() >= TPipelineProfileLevel::type::DETAIL) {
        return instance_profile;
    }

    RuntimeProfile* new_instance_profile = nullptr;
    int64_t process_raw_timer = 0;
    DeferOp defer([&new_instance_profile, &process_raw_timer]() {
        if (new_instance_profile != nullptr) {
            auto* process_timer = ADD_TIMER(new_instance_profile, "BackendProfileMergeTime");
            COUNTER_SET(process_timer, process_raw_timer);
        }
    });

    SCOPED_RAW_TIMER(&process_raw_timer);
    std::vector<RuntimeProfile*> pipeline_profiles;
    instance_profile->get_children(&pipeline_profiles);

    std::vector<RuntimeProfile*> merged_driver_profiles;
    for (auto* pipeline_profile : pipeline_profiles) {
        std::vector<RuntimeProfile*> driver_profiles;
        pipeline_profile->get_children(&driver_profiles);

        if (driver_profiles.empty()) {
            continue;
        }

        auto* merged_driver_profile = RuntimeProfile::merge_isomorphic_profiles(obj_pool, driver_profiles);

        // use the name of pipeline' profile as pipeline driver's
        merged_driver_profile->set_name(pipeline_profile->name());

        // add all the info string and counters of the pipeline's profile
        // to the pipeline driver's profile
        merged_driver_profile->copy_all_info_strings_from(pipeline_profile);
        merged_driver_profile->copy_all_counters_from(pipeline_profile);

        merged_driver_profiles.push_back(merged_driver_profile);
    }

    new_instance_profile = obj_pool->add(new RuntimeProfile(instance_profile->name()));
    new_instance_profile->copy_all_info_strings_from(instance_profile);
    new_instance_profile->copy_all_counters_from(instance_profile);
    for (auto* merged_driver_profile : merged_driver_profiles) {
        merged_driver_profile->reset_parent();
        new_instance_profile->add_child(merged_driver_profile, true, nullptr);
    }

    return new_instance_profile;
}

void GlobalDriverExecutor::bind_cpus(const CpuUtil::CpuIds& cpuids,
                                     const std::vector<CpuUtil::CpuIds>& borrowed_cpuids) {
    _thread_pool->bind_cpus(cpuids, borrowed_cpuids);
    _blocked_driver_poller->bind_cpus(cpuids);
    _exec_state_reporter->bind_cpus(cpuids);
}

} // namespace starrocks::pipeline
