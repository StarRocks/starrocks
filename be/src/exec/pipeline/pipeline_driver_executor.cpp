// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/pipeline_driver_executor.h"

#include <memory>

#include "exec/workgroup/work_group.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "util/debug/query_trace.h"
#include "util/defer_op.h"
#include "util/stack_util.h"
#include "util/starrocks_metrics.h"

namespace starrocks::pipeline {

GlobalDriverExecutor::GlobalDriverExecutor(const std::string& name, std::unique_ptr<ThreadPool> thread_pool,
                                           bool enable_resource_group)
        : Base(name),
          _driver_queue(enable_resource_group ? std::unique_ptr<DriverQueue>(std::make_unique<WorkGroupDriverQueue>())
                                              : std::make_unique<QuerySharedDriverQueue>()),
          _thread_pool(std::move(thread_pool)),
          _blocked_driver_poller(new PipelineDriverPoller(_driver_queue.get())),
          _exec_state_reporter(new ExecStateReporter()),
          _audit_statistics_reporter(new AuditStatisticsReporter()) {
    REGISTER_GAUGE_STARROCKS_METRIC(pipe_driver_schedule_count, [this]() { return _schedule_count.load(); });
    REGISTER_GAUGE_STARROCKS_METRIC(pipe_driver_execution_time, [this]() { return _driver_execution_ns.load(); });
    REGISTER_GAUGE_STARROCKS_METRIC(pipe_driver_queue_len, [this]() { return _driver_queue->size(); });
    REGISTER_GAUGE_STARROCKS_METRIC(pipe_poller_block_queue_len,
                                    [this]() { return _blocked_driver_poller->blocked_driver_queue_len(); });
}

GlobalDriverExecutor::~GlobalDriverExecutor() {
    _driver_queue->close();
}

void GlobalDriverExecutor::initialize(int num_threads) {
    _blocked_driver_poller->start();
    _num_threads_setter.set_actual_num(num_threads);
    for (auto i = 0; i < num_threads; ++i) {
        _thread_pool->submit_func([this]() { this->_worker_thread(); });
    }
}

void GlobalDriverExecutor::change_num_threads(int32_t num_threads) {
    int32_t old_num_threads = 0;
    if (!_num_threads_setter.adjust_expect_num(num_threads, &old_num_threads)) {
        return;
    }
    for (int i = old_num_threads; i < num_threads; ++i) {
        _thread_pool->submit_func([this]() { this->_worker_thread(); });
    }
}

void GlobalDriverExecutor::_finalize_driver(DriverRawPtr driver, RuntimeState* runtime_state, DriverState state) {
    DCHECK(driver);
    driver->finalize(runtime_state, state, _schedule_count, _driver_execution_ns);
}

void GlobalDriverExecutor::_worker_thread() {
    auto current_thread = Thread::current_thread();
    const int worker_id = _next_id++;
    while (true) {
        if (_num_threads_setter.should_shrink()) {
            break;
        }
        // Reset TLS state
        CurrentThread::current().set_query_id({});
        CurrentThread::current().set_fragment_instance_id({});
        CurrentThread::current().set_pipeline_driver_id(0);

        if (current_thread != nullptr) {
            current_thread->set_idle(true);
        }
        auto maybe_driver = this->_driver_queue->take();
        if (current_thread != nullptr) {
            current_thread->set_idle(false);
        }
        if (maybe_driver.status().is_cancelled()) {
            return;
        }
        auto driver = maybe_driver.value();
        DCHECK(driver != nullptr);

        auto* query_ctx = driver->query_ctx();
        auto* fragment_ctx = driver->fragment_ctx();

        driver->increment_schedule_times();
        _schedule_count++;

        SCOPED_SET_TRACE_INFO(driver->driver_id(), query_ctx->query_id(), fragment_ctx->fragment_instance_id());

        SET_THREAD_LOCAL_QUERY_TRACE_CONTEXT(query_ctx->query_trace(), fragment_ctx->fragment_instance_id(), driver);

        // TODO(trueeyu): This writing is to ensure that MemTracker will not be destructed before the thread ends.
        //  This writing method is a bit tricky, and when there is a better way, replace it
        auto runtime_state_ptr = fragment_ctx->runtime_state_ptr();
        auto* runtime_state = runtime_state_ptr.get();
        {
            SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(runtime_state->instance_mem_tracker());

            if (fragment_ctx->is_canceled()) {
                driver->cancel_operators(runtime_state);
                if (driver->is_still_pending_finish()) {
                    driver->set_driver_state(DriverState::PENDING_FINISH);
                    _blocked_driver_poller->add_blocked_driver(driver);
                } else {
                    _finalize_driver(driver, runtime_state, DriverState::CANCELED);
                }
                continue;
            }
            // a blocked driver is canceled because of fragment cancellation or query expiration.
            if (driver->is_finished()) {
                _finalize_driver(driver, runtime_state, driver->driver_state());
                continue;
            }
            StatusOr<DriverState> maybe_state;
            int64_t start_time = driver->get_active_time();
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
            _driver_execution_ns += end_time - start_time;

            // Check big query
            if (status.ok() && driver->workgroup()) {
                status = driver->workgroup()->check_big_query(*query_ctx);
            }

            if (!status.ok()) {
                LOG(WARNING) << "[Driver] Process error, query_id=" << print_id(driver->query_ctx()->query_id())
                             << ", instance_id=" << print_id(driver->fragment_ctx()->fragment_instance_id())
                             << ", status=" << status;
                driver->runtime_profile()->add_info_string("ErrorMsg", status.get_error_msg());
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
            auto driver_state = maybe_state.value();
            switch (driver_state) {
            case READY:
            case RUNNING: {
                this->_driver_queue->put_back_from_executor(driver);
                break;
            }
            case FINISH:
            case CANCELED:
            case INTERNAL_ERROR: {
                _finalize_driver(driver, runtime_state, driver_state);
                break;
            }
            case INPUT_EMPTY:
            case OUTPUT_FULL:
            case PENDING_FINISH:
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

void GlobalDriverExecutor::submit(DriverRawPtr driver) {
    driver->start_schedule(_schedule_count, _driver_execution_ns);

    if (driver->is_precondition_block()) {
        driver->set_driver_state(DriverState::PRECONDITION_BLOCK);
        driver->mark_precondition_not_ready();
        this->_blocked_driver_poller->add_blocked_driver(driver);
    } else {
        driver->submit_operators();

        // Try to add the driver to poller first.
        if (!driver->source_operator()->is_finished() && !driver->source_operator()->has_output()) {
            driver->set_driver_state(DriverState::INPUT_EMPTY);
            this->_blocked_driver_poller->add_blocked_driver(driver);
        } else {
            this->_driver_queue->put_back(driver);
        }
    }
}

void GlobalDriverExecutor::cancel(DriverRawPtr driver) {
    // if driver is already in ready queue, we should cancel it
    // otherwise, just ignore it and wait for the poller to schedule
    if (driver->is_in_ready_queue()) {
        this->_driver_queue->cancel(driver);
    }
}

void GlobalDriverExecutor::report_exec_state(QueryContext* query_ctx, FragmentContext* fragment_ctx,
                                             const Status& status, bool done) {
    _update_profile_by_level(query_ctx, fragment_ctx, done);
    auto params = ExecStateReporter::create_report_exec_status_params(query_ctx, fragment_ctx, status, done);
    auto fe_addr = fragment_ctx->fe_addr();
    if (fe_addr.hostname.empty()) {
        // query executed by external connectors, like spark and flink connector,
        // does not need to report exec state to FE, so return if fe addr is empty.
        return;
    }

    auto exec_env = fragment_ctx->runtime_state()->exec_env();
    auto fragment_id = fragment_ctx->fragment_instance_id();

    auto report_task = [=]() {
        int retry_times = 0;
        while (retry_times++ < 3) {
            auto status = ExecStateReporter::report_exec_status(params, exec_env, fe_addr);
            if (!status.ok()) {
                if (status.is_not_found()) {
                    LOG(INFO) << "[Driver] Fail to report exec state due to query not found: fragment_instance_id="
                              << print_id(fragment_id);
                } else {
                    LOG(WARNING) << "[Driver] Fail to report exec state: fragment_instance_id=" << print_id(fragment_id)
                                 << ", status: " << status.to_string() << ", retry_times=" << retry_times;
                    // if it is done exec state report, we should retry
                    if (params.__isset.done && params.done) {
                        continue;
                    }
                }
            } else {
                LOG(INFO) << "[Driver] Succeed to report exec state: fragment_instance_id=" << print_id(fragment_id)
                          << ", is_done=" << params.done;
            }
            break;
        }
    };

    // if it is done exec state report, We need to ensure that this report is executed with priority
    // and is retried as much as possible to ensure success.
    // Otherwise, it may result in the query or ingestion status getting stuck.
    this->_exec_state_reporter->submit(std::move(report_task), done);
    VLOG(1) << "[Driver] Submit exec state report task: fragment_instance_id=" << print_id(fragment_id)
            << ", is_done=" << done;
}

void GlobalDriverExecutor::report_audit_statistics(QueryContext* query_ctx, FragmentContext* fragment_ctx, bool* done) {
    // It should be guaranteed that the done flag must be set to true in any cases.
    // If the async task is submitted successfully, the done flag will be set to true in the lambda function.
    // Otherwise, the done flag will be set to true in the defer object.
    bool submit_success = false;
    DeferOp defer([&]() {
        if (!submit_success) {
            *done = true;
        }
    });
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
        *done = true;
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
            LOG(INFO) << "[Driver] Succeed to report audit statistics: fragment_instance_id=" << print_id(fragment_id);
        }
    };
    auto st = this->_audit_statistics_reporter->submit(std::move(report_task));
    if (st.ok()) {
        submit_success = true;
    }
}

void GlobalDriverExecutor::iterate_immutable_blocking_driver(const IterateImmutableDriverFunc& call) const {
    _blocked_driver_poller->iterate_immutable_driver(call);
}

void GlobalDriverExecutor::_update_profile_by_level(QueryContext* query_ctx, FragmentContext* fragment_ctx, bool done) {
    if (!done) {
        return;
    }

    if (!query_ctx->is_report_profile()) {
        return;
    }

    if (query_ctx->profile_level() >= TPipelineProfileLevel::type::DETAIL) {
        return;
    }

    auto* profile = fragment_ctx->runtime_state()->runtime_profile();

    std::vector<RuntimeProfile*> pipeline_profiles;
    profile->get_children(&pipeline_profiles);

    std::vector<RuntimeProfile*> merged_driver_profiles;
    for (auto* pipeline_profile : pipeline_profiles) {
        std::vector<RuntimeProfile*> driver_profiles;
        pipeline_profile->get_children(&driver_profiles);

        if (driver_profiles.empty()) {
            continue;
        }

        _remove_non_core_metrics(query_ctx, driver_profiles);

        RuntimeProfile::merge_isomorphic_profiles(driver_profiles);
        // all the isomorphic profiles will merged into the first profile
        auto* merged_driver_profile = driver_profiles[0];

        // use the name of pipeline' profile as pipeline driver's
        merged_driver_profile->set_name(pipeline_profile->name());

        // add all the info string and counters of the pipeline's profile
        // to the pipeline driver's profile
        merged_driver_profile->copy_all_info_strings_from(pipeline_profile);
        merged_driver_profile->copy_all_counters_from(pipeline_profile);

        merged_driver_profiles.push_back(merged_driver_profile);
    }

    // remove pipeline's profile from the hierarchy
    profile->remove_childs();
    for (auto* merged_driver_profile : merged_driver_profiles) {
        merged_driver_profile->reset_parent();
        profile->add_child(merged_driver_profile, true, nullptr);
    }
}

void GlobalDriverExecutor::_remove_non_core_metrics(QueryContext* query_ctx,
                                                    std::vector<RuntimeProfile*>& driver_profiles) {
    if (query_ctx->profile_level() > TPipelineProfileLevel::CORE_METRICS) {
        return;
    }

    for (auto* driver_profile : driver_profiles) {
        driver_profile->remove_counters(std::set<std::string>{"DriverTotalTime", "ActiveTime", "PendingTime"});

        std::vector<RuntimeProfile*> operator_profiles;
        driver_profile->get_children(&operator_profiles);

        for (auto* operator_profile : operator_profiles) {
            RuntimeProfile* common_metrics = operator_profile->get_child("CommonMetrics");
            DCHECK(common_metrics != nullptr);
            common_metrics->remove_counters(std::set<std::string>{"OperatorTotalTime"});

            RuntimeProfile* unique_metrics = operator_profile->get_child("UniqueMetrics");
            DCHECK(unique_metrics != nullptr);
            unique_metrics->remove_counters(std::set<std::string>{"ScanTime", "WaitTime"});
        }
    }
}

} // namespace starrocks::pipeline
