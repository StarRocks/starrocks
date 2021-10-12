// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/pipeline_driver_dispatcher.h"

#include "gutil/strings/substitute.h"
namespace starrocks::pipeline {
GlobalDriverDispatcher::GlobalDriverDispatcher(std::unique_ptr<ThreadPool> thread_pool)
        : _driver_queue(new QuerySharedDriverQueue()),
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
        DCHECK(!driver->source_operator()->pending_finish());
        QueryContextManager::instance()->remove(query_id);
    }
}

void GlobalDriverDispatcher::run() {
    while (true) {
        if (_num_threads_setter.should_shrink()) {
            break;
        }

        size_t queue_index;
        auto maybe_driver = this->_driver_queue->take(&queue_index);
        if (maybe_driver.status().is_cancelled()) {
            return;
        }
        auto driver = maybe_driver.value();
        DCHECK(driver != nullptr);

        auto* query_ctx = driver->query_ctx();
        auto* fragment_ctx = driver->fragment_ctx();
        auto* runtime_state = fragment_ctx->runtime_state();

        if (fragment_ctx->is_canceled()) {
            VLOG_ROW << "[Driver] Canceled: driver=" << driver
                     << ", error=" << fragment_ctx->final_status().to_string();
            driver->cancel(runtime_state);
            if (driver->source_operator()->pending_finish()) {
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
        auto status = driver->process(runtime_state);
        this->_driver_queue->get_sub_queue(queue_index)->update_accu_time(driver);

        if (!status.ok()) {
            VLOG_ROW << "[Driver] Process error: error=" << status.status().to_string();
            query_ctx->cancel(status.status());
            driver->cancel(runtime_state);
            if (driver->source_operator()->pending_finish()) {
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
            VLOG_ROW << strings::Substitute("[Driver] Push back again, source=$0, state=$1",
                                            driver->source_operator()->get_name(), ds_to_string(driver_state));
            this->_driver_queue->put_back(driver);
            break;
        }
        case FINISH:
        case CANCELED:
        case INTERNAL_ERROR: {
            VLOG_ROW << strings::Substitute("[Driver] Finished, source=$0, state=$1, status=$2",
                                            driver->source_operator()->get_name(), ds_to_string(driver_state),
                                            fragment_ctx->final_status().to_string());
            finalize_driver(driver, runtime_state, driver_state);
            break;
        }
        case INPUT_EMPTY:
        case OUTPUT_FULL:
        case PENDING_FINISH: {
            VLOG_ROW << strings::Substitute("[Driver] Blocked, source=$0, state=$1",
                                            driver->source_operator()->get_name(), ds_to_string(driver_state));
            _blocked_driver_poller->add_blocked_driver(driver);
            break;
        }
        default:
            DCHECK(false);
        }
    }
}

void GlobalDriverDispatcher::dispatch(DriverRawPtr driver) {
    this->_driver_queue->put_back(driver);
}

void GlobalDriverDispatcher::report_exec_state(FragmentContext* fragment_ctx, const Status& status, bool done) {
    auto params = ExecStateReporter::create_report_exec_status_params(fragment_ctx, status, done);
    auto fe_addr = fragment_ctx->fe_addr();
    auto exec_env = fragment_ctx->runtime_state()->exec_env();
    auto fragment_id = fragment_ctx->fragment_instance_id();

    auto report_task = [=]() {
        auto status = ExecStateReporter::report_exec_status(params, exec_env, fe_addr);
        if (!status.ok()) {
            LOG(WARNING) << "[Driver] Fail to report exec state: fragment_instance_id=" << print_id(fragment_id);
        } else {
            LOG(INFO) << "[Driver] Succeed to report exec state: fragment_instance_id=" << print_id(fragment_id);
        }
    };

    this->_exec_state_reporter->submit(std::move(report_task));
}
} // namespace starrocks::pipeline
