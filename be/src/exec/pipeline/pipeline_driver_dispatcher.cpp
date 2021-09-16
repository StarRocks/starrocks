// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/pipeline_driver_dispatcher.h"

#include "gutil/strings/substitute.h"
namespace starrocks {
namespace pipeline {
GlobalDriverDispatcher::GlobalDriverDispatcher(std::unique_ptr<ThreadPool> thread_pool, size_t reaper_slot_size)
        : _driver_queue(new QuerySharedDriverQueue()),
          _thread_pool(std::move(thread_pool)),
          _blocked_driver_poller(new PipelineDriverPoller(_driver_queue.get())),
          _exec_state_reporter(new ExecStateReporter()),
          _query_context_reaper(new DyingQueryContextReaper(reaper_slot_size)) {}

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
    // execution threads choose different strides to compute next slots to probe.
    // different strides can reduce contention.
    const auto slot_stride = _query_context_reaper->next_slot_stride();
    const auto slot_size = _query_context_reaper->slot_size();
    int slot_idx = 0;
    while (true) {
        if (_num_threads_setter.should_shrink()) {
            break;
        }
        // reap in-active dying QueryContext
        reap_inactive_dying_query_contexts(slot_idx, slot_stride, slot_size);
        // dispatcher should not wait for drivers for a long time, it should spend time on dying QueryContexts'
        // reaping, so driver_queue's take function would return a nullptr in case of timeout waiting.
        size_t queue_index;
        auto driver = this->_driver_queue->take(&queue_index);
        if (driver == nullptr) {
            continue;
        }

        auto* query_ctx = driver->query_ctx();
        auto* fragment_ctx = driver->fragment_ctx();
        auto* runtime_state = fragment_ctx->runtime_state();

        if (fragment_ctx->is_canceled()) {
            VLOG_ROW << "[Driver] Canceled: error=" << fragment_ctx->final_status().to_string();
            if (driver->source_operator()->pending_finish()) {
                driver->set_driver_state(DriverState::PENDING_FINISH);
                _blocked_driver_poller->add_blocked_driver(driver);
            } else {
                driver->finalize(runtime_state, DriverState::CANCELED);
            }
            continue;
        }
        // a blocked driver is canceled because of fragment cancellation or query expiration.
        if (driver->is_finished()) {
            driver->finalize(runtime_state, driver->driver_state());
            continue;
        }
        // query context has ready drivers to run, so extend its lifetime.
        query_ctx->extend_lifetime();
        auto status = driver->process(runtime_state);
        this->_driver_queue->get_sub_queue(queue_index)->update_accu_time(driver);

        if (!status.ok()) {
            VLOG_ROW << "[Driver] Process error: error=" << status.status().to_string();
            query_ctx->cancel(status.status());
            if (driver->source_operator()->pending_finish()) {
                driver->set_driver_state(DriverState::PENDING_FINISH);
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

void GlobalDriverDispatcher::reap_inactive_dying_query_contexts(int& slot_idx, int slot_stride, int slot_size) {
    slot_idx += slot_stride;
    slot_idx %= slot_size;
    auto&& dying_query_ctx_list = _query_context_reaper->get(slot_idx);
    auto query_ctx_it = dying_query_ctx_list.begin();
    while (query_ctx_it != dying_query_ctx_list.end()) {
        if ((*query_ctx_it)->should_be_reaped()) {
            query_ctx_it->reset();
            dying_query_ctx_list.erase(query_ctx_it++);
        } else {
            ++query_ctx_it;
        }
    }
    if (!dying_query_ctx_list.empty()) {
        _query_context_reaper->add(slot_idx, std::move(dying_query_ctx_list));
    }
}

void GlobalDriverDispatcher::dispatch(DriverPtr driver) {
    this->_driver_queue->put_back(driver);
}

void GlobalDriverDispatcher::report_exec_state(FragmentContext* fragment_ctx, const Status& status, bool done,
                                               bool clean) {
    auto report_func = [=]() {
        auto params = ExecStateReporter::create_report_exec_status_params(fragment_ctx, status, done);
        auto status = ExecStateReporter::report_exec_status(params, fragment_ctx->runtime_state()->exec_env(),
                                                            fragment_ctx->fe_addr());
        if (!status.ok()) {
            LOG(WARNING) << "[Driver] Fail to report exec state: fragment_instance_id="
                         << fragment_ctx->fragment_instance_id();
        } else {
            LOG(INFO) << "[Driver] Succeed to report exec state: fragment_instance_id="
                      << fragment_ctx->fragment_instance_id();
        }
        if (clean) {
            auto query_id = fragment_ctx->query_id();
            auto&& query_ctx = QueryContextManager::instance()->get(query_id);
            DCHECK(query_ctx);
            query_ctx->fragment_mgr()->unregister(fragment_ctx->fragment_instance_id());
            if (query_ctx->count_down_fragment()) {
                auto&& managed_query_ctx = QueryContextManager::instance()->remove(query_id);
                auto* dispatcher = query_ctx->runtime_state()->exec_env()->driver_dispatcher();
                dispatcher->add_dying_query_ctx(std::move(managed_query_ctx));
            }
        }
    };
    fragment_ctx->token()->submit_func(report_func);
}
std::unique_ptr<ThreadPoolToken> GlobalDriverDispatcher::new_report_token() {
    return _exec_state_reporter->new_token();
}
void GlobalDriverDispatcher::add_dying_query_ctx(QueryContextPtr&& query_ctx) {
    _query_context_reaper->add(std::move(query_ctx));
}
} // namespace pipeline
} // namespace starrocks
