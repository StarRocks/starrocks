// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/fragment_context.h"

#include "exec/pipeline/pipeline_driver_executor.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/exec_env.h"
namespace starrocks::pipeline {

void FragmentContext::set_final_status(const Status& status) {
    if (_final_status.load() != nullptr) {
        return;
    }
    Status* old_status = nullptr;
    if (_final_status.compare_exchange_strong(old_status, &_s_status)) {
        _s_status = status;
        if (_s_status.is_cancelled()) {
            auto detailed_message = _s_status.detailed_message();
            std::stringstream ss;
            ss << "[Driver] Canceled, query_id=" << print_id(_query_id)
               << ", instance_id=" << print_id(_fragment_instance_id) << ", reason=" << detailed_message;
            if (detailed_message == "LimitReach" || detailed_message == "UserCancel" || detailed_message == "TimeOut") {
                LOG(INFO) << ss.str();
            } else {
                LOG(WARNING) << ss.str();
            }
            DriverExecutor* executor = enable_resource_group() ? _runtime_state->exec_env()->wg_driver_executor()
                                                               : _runtime_state->exec_env()->driver_executor();
            for (auto& driver : _drivers) {
                executor->cancel(driver.get());
            }
        }
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
        return raw_ctx;
    }
}

void FragmentContextManager::register_ctx(const TUniqueId& fragment_id, FragmentContextPtr fragment_ctx) {
    std::lock_guard<std::mutex> lock(_lock);

    if (_fragment_contexts.find(fragment_id) != _fragment_contexts.end()) {
        return;
    }

    _fragment_contexts.emplace(fragment_id, std::move(fragment_ctx));
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

} // namespace starrocks::pipeline
