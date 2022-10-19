// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/pipeline_driver_executor.h"

#include <memory>

#include "exec/workgroup/work_group.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "util/debug/query_trace.h"
#include "util/defer_op.h"

namespace starrocks::pipeline {

GlobalDriverExecutor::GlobalDriverExecutor(std::string name, std::unique_ptr<ThreadPool> thread_pool,
                                           bool enable_resource_group)
        : Base(name),
          _enable_resource_group(enable_resource_group),
          _driver_queue(enable_resource_group ? std::unique_ptr<DriverQueue>(std::make_unique<WorkGroupDriverQueue>())
                                              : std::make_unique<QuerySharedDriverQueue>()),
          _thread_pool(std::move(thread_pool)),
          _blocked_driver_poller(new PipelineDriverPoller(_driver_queue.get())),
          _exec_state_reporter(new ExecStateReporter()) {}

GlobalDriverExecutor::~GlobalDriverExecutor() {
    {
        // unregist hook
        auto metrics = StarRocksMetrics::instance()->metrics();
        metrics->deregister_hook("driver_queue_len");
        metrics->deregister_hook("poller_block_queue_len");
        _driver_queue_len.reset();
        _driver_poller_block_queue_len.reset();
    }
    _driver_queue->close();
}

void GlobalDriverExecutor::initialize(int num_threads) {
    {
        // regist pipeline metrics
        auto metrics = StarRocksMetrics::instance()->metrics();
        auto regist_metric = [this, metrics](const std::string& metric_name, std::unique_ptr<UIntGauge>& metric,
                                             const std::function<unsigned long()> provider) {
            std::string full_name = _name + "_" + metric_name;
            metric = std::make_unique<UIntGauge>(MetricUnit::NOUNIT);
            metrics->register_metric(full_name, metric.get());
            metrics->register_hook(full_name, [&metric, provider]() { metric->set_value(provider()); });
        };
        regist_metric("driver_queue_len", _driver_queue_len, [this]() { return _driver_queue->size(); });
        regist_metric("poller_block_queue_len", _driver_poller_block_queue_len,
                      [this]() { return _blocked_driver_poller->blocked_driver_queue_len(); });
    }

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
    driver->finalize(runtime_state, state);
}

void GlobalDriverExecutor::_worker_thread() {
    const int worker_id = _next_id++;
    while (true) {
        if (_num_threads_setter.should_shrink()) {
            break;
        }
        // Reset TLS state
        CurrentThread::current().set_query_id({});
        CurrentThread::current().set_fragment_instance_id({});
        CurrentThread::current().set_pipeline_driver_id(0);

        auto maybe_driver = this->_driver_queue->take();
        if (maybe_driver.status().is_cancelled()) {
            return;
        }
        auto driver = maybe_driver.value();
        DCHECK(driver != nullptr);

        auto* query_ctx = driver->query_ctx();
        auto* fragment_ctx = driver->fragment_ctx();

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
            TRY_CATCH_ALL(maybe_state, driver->process(runtime_state, worker_id));
            Status status = maybe_state.status();
            this->_driver_queue->update_statistics(driver);

            // Check big query
            if (status.ok() && driver->workgroup()) {
                status = driver->workgroup()->check_big_query(*query_ctx);
            }

            if (!status.ok()) {
                LOG(WARNING) << "[Driver] Process error, query_id=" << print_id(driver->query_ctx()->query_id())
                             << ", instance_id=" << print_id(driver->fragment_ctx()->fragment_instance_id())
                             << ", status=" << status;
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

void GlobalDriverExecutor::report_exec_state(FragmentContext* fragment_ctx, const Status& status, bool done) {
    _update_profile_by_level(fragment_ctx, done);
    auto params = ExecStateReporter::create_report_exec_status_params(fragment_ctx, status, done);
    auto fe_addr = fragment_ctx->fe_addr();
    auto exec_env = fragment_ctx->runtime_state()->exec_env();
    auto fragment_id = fragment_ctx->fragment_instance_id();

    auto report_task = [=]() {
        auto status = ExecStateReporter::report_exec_status(params, exec_env, fe_addr);
        if (!status.ok()) {
            if (status.is_not_found()) {
                LOG(INFO) << "[Driver] Fail to report exec state due to query not found: fragment_instance_id="
                          << print_id(fragment_id);
            } else {
                LOG(WARNING) << "[Driver] Fail to report exec state: fragment_instance_id=" << print_id(fragment_id)
                             << ", status: " << status.to_string();
            }
        } else {
            LOG(INFO) << "[Driver] Succeed to report exec state: fragment_instance_id=" << print_id(fragment_id);
        }
    };

    this->_exec_state_reporter->submit(std::move(report_task));
}

void GlobalDriverExecutor::_update_profile_by_level(FragmentContext* fragment_ctx, bool done) {
    if (!done) {
        return;
    }

    if (!fragment_ctx->is_report_profile()) {
        return;
    }

    if (fragment_ctx->profile_level() >= TPipelineProfileLevel::type::DETAIL) {
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

        _remove_non_core_metrics(fragment_ctx, driver_profiles);

        RuntimeProfile::merge_isomorphic_profiles(driver_profiles);
        // all the isomorphic profiles will merged into the first profile
        auto* merged_driver_profile = driver_profiles[0];

        // use the name of pipeline' profile as pipeline driver's
        merged_driver_profile->set_name(pipeline_profile->name());

        // add all the info string and counters of the pipeline's profile
        // to the pipeline driver's profile
        merged_driver_profile->copy_all_info_strings_from(pipeline_profile);
        merged_driver_profile->copy_all_counters_from(pipeline_profile);

        _simplify_common_metrics(merged_driver_profile);

        merged_driver_profiles.push_back(merged_driver_profile);
    }

    // remove pipeline's profile from the hierarchy
    profile->remove_childs();
    for (auto* merged_driver_profile : merged_driver_profiles) {
        merged_driver_profile->reset_parent();
        profile->add_child(merged_driver_profile, true, nullptr);
    }
}

void GlobalDriverExecutor::_remove_non_core_metrics(FragmentContext* fragment_ctx,
                                                    std::vector<RuntimeProfile*>& driver_profiles) {
    if (fragment_ctx->profile_level() > TPipelineProfileLevel::CORE_METRICS) {
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

void GlobalDriverExecutor::_simplify_common_metrics(RuntimeProfile* driver_profile) {
    std::vector<RuntimeProfile*> operator_profiles;
    driver_profile->get_children(&operator_profiles);
    for (auto* operator_profile : operator_profiles) {
        RuntimeProfile* common_metrics = operator_profile->get_child("CommonMetrics");
        DCHECK(common_metrics != nullptr);

        // Remove runtime filter related counters if it's value is 0
        static std::string counter_names[] = {"RuntimeInFilterNum",         "RuntimeBloomFilterNum",
                                              "JoinRuntimeFilterInputRows", "JoinRuntimeFilterOutputRows",
                                              "JoinRuntimeFilterEvaluate",  "JoinRuntimeFilterTime",
                                              "ConjunctsInputRows",         "ConjunctsOutputRows",
                                              "ConjunctsEvaluate",          "ConjunctsTime",
                                              "JoinRuntimeFilterHashTime"};
        for (auto& name : counter_names) {
            auto* counter = common_metrics->get_counter(name);
            if (counter != nullptr && counter->value() == 0) {
                common_metrics->remove_counter(name);
            }
        }
    }
}
} // namespace starrocks::pipeline
