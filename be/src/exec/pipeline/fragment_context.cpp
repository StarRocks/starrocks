// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/fragment_context.h"

#include "exec/pipeline/pipeline_driver_executor.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/exec_env.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/stream_load/transaction_mgr.h"

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

void FragmentContext::set_stream_load_contexts(const std::vector<StreamLoadContext*>& contexts) {
    _stream_load_contexts = std::move(contexts);
    _channel_stream_load = true;
}

void FragmentContext::cancel(const Status& status) {
<<<<<<< HEAD
    if (_runtime_state != nullptr && _runtime_state->query_ctx() != nullptr) {
=======
    if (!status.ok() && _runtime_state != nullptr && _runtime_state->query_ctx() != nullptr) {
>>>>>>> 2.5.18
        _runtime_state->query_ctx()->release_workgroup_token_once();
    }

    _runtime_state->set_is_cancelled(true);
    set_final_status(status);

    const TQueryOptions& query_options = _runtime_state->query_options();
    if (query_options.query_type == TQueryType::LOAD && (query_options.load_job_type == TLoadJobType::BROKER ||
                                                         query_options.load_job_type == TLoadJobType::INSERT_QUERY ||
                                                         query_options.load_job_type == TLoadJobType::INSERT_VALUES)) {
        starrocks::ExecEnv::GetInstance()->profile_report_worker()->unregister_pipeline_load(_query_id,
                                                                                             _fragment_instance_id);
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
        RETURN_IF_ERROR(starrocks::ExecEnv::GetInstance()->profile_report_worker()->register_pipeline_load(
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
            starrocks::ExecEnv::GetInstance()->profile_report_worker()->unregister_pipeline_load(it->second->query_id(),
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

} // namespace starrocks::pipeline
