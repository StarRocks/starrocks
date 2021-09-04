// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/fragment_context.h"
namespace starrocks {
namespace pipeline {

FragmentContextManager::FragmentContextManager() {}
FragmentContextManager::~FragmentContextManager() {}
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

FragmentContext* FragmentContextManager::get(const TUniqueId& fragment_id) {
    std::lock_guard<std::mutex> lock(_lock);
    auto it = _fragment_contexts.find(fragment_id);
    if (it != _fragment_contexts.end()) {
        return it->second.get();
    } else {
        return nullptr;
    }
}

void FragmentContextManager::unregister(const TUniqueId& fragment_id) {
    std::lock_guard<std::mutex> lock(_lock);
    _fragment_contexts.erase(fragment_id);
}

} // namespace pipeline
} // namespace starrocks
