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

#include "exec/pipeline/fragment_context_manager.h"

#include <memory>
#include <mutex>
#include <sstream>
#include <utility>

#include "common/logging.h"
#include "compute_env/profile_report_worker.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/fragment_context_cancel.h"
#include "runtime/runtime_state.h"
#include "runtime/service_contexts.h"

namespace starrocks::pipeline {

namespace {

const QueryExecutionServices& query_execution_services(const RuntimeState* state) {
    DCHECK(state != nullptr);
    return *state->query_execution_services();
}

const RuntimeServices& runtime_services(const RuntimeState* state) {
    return *query_execution_services(state).runtime;
}

} // namespace

FragmentContextManager::FragmentContextManager(FragmentLifecycleWeakPtr fragment_lifecycle)
        : _fragment_lifecycle(std::move(fragment_lifecycle)) {}

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
        RETURN_IF_ERROR(runtime_services(fragment_ctx->runtime_state())
                                .profile_report_worker->register_pipeline_load(fragment_ctx->query_id(), fragment_id));
    }
    if (!_fragment_lifecycle.expired()) {
        fragment_ctx->set_fragment_lifecycle(_fragment_lifecycle);
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
            runtime_services(it->second->runtime_state())
                    .profile_report_worker->unregister_pipeline_load(it->second->query_id(), fragment_id);
        }
        _fragment_contexts.erase(it);
    }
}

void FragmentContextManager::cancel(const Status& status) {
    std::lock_guard<std::mutex> lock(_lock);
    for (auto& _fragment_context : _fragment_contexts) {
        cancel_fragment_context(_fragment_context.second.get(), status);
    }
}

void FragmentContextManager::for_each_fragment(const std::function<void(const FragmentContextPtr&)>& caller) {
    std::lock_guard guard(_lock);
    for (auto& [_, fragment] : _fragment_contexts) {
        caller(fragment);
    }
}

} // namespace starrocks::pipeline
