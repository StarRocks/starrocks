// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
#include "exec/pipeline/query_context.h"

#include "exec/pipeline/fragment_context.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/exec_env.h"

namespace starrocks::pipeline {
QueryContext::QueryContext()
        : _fragment_mgr(new FragmentContextManager()),
          _total_fragments(0),
          _num_fragments(0),
          _num_active_fragments(0),
          _deadline(0) {}

QueryContext::~QueryContext() {
    if (_exec_env != nullptr) {
        if (_is_runtime_filter_coordinator) {
            _exec_env->runtime_filter_worker()->close_query(_query_id);
        }
        _exec_env->stream_mgr()->destroy_pass_through_chunk_buffer(_query_id);
    }
}
FragmentContextManager* QueryContext::fragment_mgr() {
    return _fragment_mgr.get();
}

void QueryContext::cancel(const Status& status) {
    _fragment_mgr->cancel(status);
}

QueryContextManager::QueryContextManager() = default;
QueryContextManager::~QueryContextManager() = default;
QueryContext* QueryContextManager::get_or_register(const TUniqueId& query_id) {
    std::lock_guard lock(_lock);
    auto iter = _contexts.find(query_id);
    if (iter != _contexts.end()) {
        iter->second->increment_num_fragments();
        return iter->second.get();
    }

    auto&& ctx = std::make_shared<QueryContext>();
    auto* ctx_raw_ptr = ctx.get();
    ctx_raw_ptr->set_query_id(query_id);
    ctx_raw_ptr->increment_num_fragments();
    _contexts.emplace(query_id, std::move(ctx));
    return ctx_raw_ptr;
}

QueryContextPtr QueryContextManager::get(const TUniqueId& query_id) {
    std::lock_guard lock(_lock);
    auto it = _contexts.find(query_id);
    if (it != _contexts.end()) {
        return it->second;
    } else {
        return nullptr;
    }
}

QueryContextPtr QueryContextManager::remove(const TUniqueId& query_id) {
    std::lock_guard lock(_lock);
    auto it = _contexts.find(query_id);
    if (it != _contexts.end()) {
        auto ctx = std::move(it->second);
        _contexts.erase(it);
        return ctx;
    } else {
        return nullptr;
    }
}

} // namespace starrocks::pipeline
