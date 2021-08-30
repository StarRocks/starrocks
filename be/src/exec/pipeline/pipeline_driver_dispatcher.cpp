// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/pipeline_driver_dispatcher.h"

#include "gutil/strings/substitute.h"
namespace starrocks {
namespace pipeline {
GlobalDriverDispatcher::GlobalDriverDispatcher(std::unique_ptr<ThreadPool> thread_pool)
        : _driver_queue(new QuerySharedDriverQueue()),
          _thread_pool(std::move(thread_pool)),
          _blocked_driver_poller(new PipelineDriverPoller(_driver_queue.get())),
          _exec_state_reporter(new ExecStateReporter()) {}

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

void GlobalDriverDispatcher::run() {
    while (true) {
        if (_num_threads_setter.should_shrink()) {
            break;
        }

        size_t queue_index;
        auto driver = this->_driver_queue->take(&queue_index);
        DCHECK(driver != nullptr);
        auto* fragment_ctx = driver->fragment_ctx();
        auto* runtime_state = fragment_ctx->runtime_state();

        if (fragment_ctx->is_canceled()) {
            VLOG_ROW << "[Driver] Canceled: error=" << fragment_ctx->final_status().to_string();
            if (driver->source_operator()->async_pending()) {
                driver->set_driver_state(DriverState::ASYNC_PENDING);
                _blocked_driver_poller->add_blocked_driver(driver);
            } else {
                driver->finalize(runtime_state, DriverState::CANCELED);
            }
            continue;
        }

        auto status = driver->process(runtime_state);
        this->_driver_queue->get_sub_queue(queue_index)->update_accu_time(driver);

        if (!status.ok()) {
            VLOG_ROW << "[Driver] Process error: error=" << status.status().to_string();
            fragment_ctx->cancel(status.status());
            if (driver->source_operator()->async_pending()) {
                driver->set_driver_state(DriverState::ASYNC_PENDING);
                _blocked_driver_poller->add_blocked_driver(driver);
            } else {
                driver->finalize(runtime_state, DriverState::INTERNAL_ERROR);
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
            driver->finalize(runtime_state, driver_state);
            break;
        }
        case INPUT_EMPTY:
        case OUTPUT_FULL:
        case ASYNC_PENDING: {
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

void GlobalDriverDispatcher::dispatch(DriverPtr driver) {
    this->_driver_queue->put_back(driver);
}

void GlobalDriverDispatcher::report_exec_state(FragmentContext* fragment_ctx, const Status& status, bool done,
                                               bool clean) {
    this->_exec_state_reporter->submit(fragment_ctx, status, done, clean);
}
} // namespace pipeline
} // namespace starrocks
