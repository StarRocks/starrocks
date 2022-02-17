// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/pipeline_driver_dispatcher.h"

#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "util/defer_op.h"

namespace starrocks::pipeline {

GlobalDriverDispatcher::GlobalDriverDispatcher(std::unique_ptr<ThreadPool> thread_pool, bool enable_resource_group)
        : _enable_resource_group(enable_resource_group),
          _driver_queue(enable_resource_group
                                ? std::unique_ptr<DriverQueue>(std::make_unique<DriverQueueWithWorkGroup>())
                                : std::make_unique<QuerySharedDriverQueue>()),
          _thread_pool(std::move(thread_pool)),
          _blocked_driver_poller(new PipelineDriverPoller(_driver_queue.get())),
          _exec_state_reporter(new ExecStateReporter()) {}

GlobalDriverDispatcher::~GlobalDriverDispatcher() {
    _driver_queue->close();
}

void GlobalDriverDispatcher::initialize(int num_threads) {
    _blocked_driver_poller->start();
    _num_threads_setter.set_actual_num(num_threads);
    for (auto i = 0; i < num_threads; ++i) {
        _thread_pool->submit_func([this]() { this->run(); });
    }
}

void GlobalDriverDispatcher::change_num_threads(int32_t num_threads) {
    int32_t old_num_threads = 0;
    if (!_num_threads_setter.adjust_expect_num(num_threads, &old_num_threads)) {
        return;
    }
    for (int i = old_num_threads; i < num_threads; ++i) {
        _thread_pool->submit_func([this]() { this->run(); });
    }
}

void GlobalDriverDispatcher::finalize_driver(DriverRawPtr driver, RuntimeState* runtime_state, DriverState state) {
    DCHECK(driver);
    driver->finalize(runtime_state, state);
    if (driver->query_ctx()->is_finished()) {
        auto query_id = driver->query_ctx()->query_id();
        DCHECK(!driver->is_still_pending_finish());
        QueryContextManager::instance()->remove(query_id);
    }
}

void GlobalDriverDispatcher::run() {
    const int dispatcher_id = _next_id++;
    while (true) {
        if (_num_threads_setter.should_shrink()) {
            break;
        }

        auto maybe_driver = this->_driver_queue->take(dispatcher_id);
        if (maybe_driver.status().is_cancelled()) {
            return;
        }
        auto driver = maybe_driver.value();
        DCHECK(driver != nullptr);

        auto* query_ctx = driver->query_ctx();
        auto* fragment_ctx = driver->fragment_ctx();
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
                    finalize_driver(driver, runtime_state, DriverState::CANCELED);
                }
                continue;
            }
            // a blocked driver is canceled because of fragment cancellation or query expiration.
            if (driver->is_finished()) {
                finalize_driver(driver, runtime_state, driver->driver_state());
                continue;
            }

            // query context has ready drivers to run, so extend its lifetime.
            query_ctx->extend_lifetime();
            auto status = driver->process(runtime_state, dispatcher_id);
            this->_driver_queue->update_statistics(driver);

            if (!status.ok()) {
                LOG(WARNING) << "[Driver] Process error, query_id=" << print_id(driver->query_ctx()->query_id())
                             << ", instance_id=" << print_id(driver->fragment_ctx()->fragment_instance_id())
                             << ", error=" << status.status().to_string();
                query_ctx->cancel(status.status());
                driver->cancel_operators(runtime_state);
                if (driver->is_still_pending_finish()) {
                    driver->set_driver_state(DriverState::PENDING_FINISH);
                    _blocked_driver_poller->add_blocked_driver(driver);
                } else {
                    finalize_driver(driver, runtime_state, DriverState::INTERNAL_ERROR);
                }
                continue;
            }
            auto driver_state = status.value();
            switch (driver_state) {
            case READY:
            case RUNNING: {
                this->_driver_queue->put_back_from_dispatcher(driver);
                break;
            }
            case FINISH:
            case CANCELED:
            case INTERNAL_ERROR: {
                finalize_driver(driver, runtime_state, driver_state);
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

void GlobalDriverDispatcher::dispatch(DriverRawPtr driver) {
    if (driver->is_precondition_block()) {
        driver->set_driver_state(DriverState::PRECONDITION_BLOCK);
        driver->mark_precondition_not_ready();
        this->_blocked_driver_poller->add_blocked_driver(driver);
    } else {
        driver->dispatch_operators();

        // Try to add the driver to poller first.
        if (!driver->source_operator()->is_finished() && !driver->source_operator()->has_output()) {
            driver->set_driver_state(DriverState::INPUT_EMPTY);
            this->_blocked_driver_poller->add_blocked_driver(driver);
        } else {
            this->_driver_queue->put_back(driver);
        }
    }
}

void GlobalDriverDispatcher::report_exec_state(FragmentContext* fragment_ctx, const Status& status, bool done) {
    if (done) {
        update_profile_by_mode(fragment_ctx, done);
    }
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

void GlobalDriverDispatcher::update_profile_by_mode(FragmentContext* fragment_ctx, bool done) {
    if (!done) {
        return;
    }

    if (fragment_ctx->profile_mode() != TPipelineProfileMode::type::BRIEF) {
        return;
    }

    auto* profile = fragment_ctx->runtime_state()->runtime_profile();

    std::vector<RuntimeProfile*> pipeline_profiles;
    profile->get_children(&pipeline_profiles);
    for (auto* pipeline_profile : pipeline_profiles) {
        std::vector<RuntimeProfile*> pipeline_driver_profiles;
        pipeline_profile->get_children(&pipeline_driver_profiles);

        // Find the most time-consuming profile in all driver profiles
        int64_t max_active_time = 0;
        RuntimeProfile* pipeline_driver_profile_with_max_active_time = nullptr;
        for (auto* pipeline_driver_profile : pipeline_driver_profiles) {
            auto* active_timer = pipeline_driver_profile->get_counter("ActiveTime");
            if (active_timer == nullptr) {
                continue;
            }
            if (active_timer->value() > max_active_time) {
                max_active_time = active_timer->value();
                pipeline_driver_profile_with_max_active_time = pipeline_driver_profile;
            }
        }
        if (pipeline_driver_profile_with_max_active_time == nullptr) {
            continue;
        }
        pipeline_profile->remove_childs();
        pipeline_driver_profile_with_max_active_time->reset_parent();
        pipeline_profile->add_child(pipeline_driver_profile_with_max_active_time, true, nullptr);
        pipeline_profile->add_info_string("ContainsAllPipelineDrivers", "false");
    }
}

} // namespace starrocks::pipeline
