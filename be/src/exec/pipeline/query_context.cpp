// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
#include "exec/pipeline/query_context.h"

#include "exec/pipeline/fragment_context.h"

namespace starrocks {
namespace pipeline {
QueryContext::QueryContext()
        : _fragment_mgr(new FragmentContextManager()), _num_fragments(0), _num_active_fragments(0) {}

FragmentContextManager* QueryContext::fragment_mgr() {
    return _fragment_mgr.get();
}

void QueryContext::cancel(const Status& status) {
    _fragment_mgr->cancel(status);
}

QueryContextManager::QueryContextManager() {}
QueryContextManager::~QueryContextManager() {}
QueryContext* QueryContextManager::get_or_register(const TUniqueId& query_id) {
    std::lock_guard lock(_lock);
    auto iter = _contexts.find(query_id);
    if (iter != _contexts.end()) {
        iter->second->increment_num_fragments();
        return iter->second.get();
    }

    auto&& ctx = std::make_unique<QueryContext>();
    auto* ctx_raw_ptr = ctx.get();
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

void QueryContextManager::unregister(const TUniqueId& query_id) {
    std::lock_guard lock(_lock);
    _contexts.erase(query_id);
}

} // namespace pipeline
} // namespace starrocks
