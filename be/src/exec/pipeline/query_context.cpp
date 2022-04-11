// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
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
    }
}

void QueryContext::init_mem_tracker(MemTracker* parent, int64_t limit) {
    std::string name = print_id(_query_id);
    _mem_tracker = std::make_shared<starrocks::MemTracker>(limit, name, parent);
}

FragmentContextManager* QueryContext::fragment_mgr() {
    return _fragment_mgr.get();
}

void QueryContext::cancel(const Status& status) {
    _fragment_mgr->cancel(status);
}

static constexpr size_t QUERY_CONTEXT_MAP_SLOT_NUM = 64;
static constexpr size_t QUERY_CONTEXT_MAP_SLOT_NUM_MASK = (1 << 6) - 1;

QueryContextManager::QueryContextManager()
        : _mutexes(QUERY_CONTEXT_MAP_SLOT_NUM),
          _context_maps(QUERY_CONTEXT_MAP_SLOT_NUM),
          _second_chance_maps(QUERY_CONTEXT_MAP_SLOT_NUM) {}

#ifdef BE_TEST
QueryContextManager::QueryContextManager(int) : QueryContextManager() {}
#endif

QueryContextManager::~QueryContextManager() = default;
QueryContext* QueryContextManager::get_or_register(const TUniqueId& query_id) {
    size_t i = std::hash<size_t>()(query_id.lo) & QUERY_CONTEXT_MAP_SLOT_NUM_MASK;
    auto& mutex = _mutexes[i];
    auto& context_map = _context_maps[i];
    auto& sc_map = _second_chance_maps[i];

    {
        std::shared_lock<std::shared_mutex> read_lock(mutex);
        // lookup query context in context_map
        auto it = context_map.find(query_id);
        if (it != context_map.end()) {
            it->second->increment_num_fragments();
            return it->second.get();
        }
    }
    {
        std::unique_lock<std::shared_mutex> write_lock(mutex);
        // lookup query context in context_map at first
        auto it = context_map.find(query_id);
        auto sc_it = sc_map.find(query_id);
        if (it != context_map.end()) {
            it->second->increment_num_fragments();
            return it->second.get();
        } else {
            // lookup query context for the second chance in sc_map
            if (sc_it != sc_map.end()) {
                auto ctx = std::move(sc_it->second);
                ctx->increment_num_fragments();
                sc_map.erase(sc_it);
                auto* raw_ctx_ptr = ctx.get();
                context_map.emplace(query_id, std::move(ctx));
                return raw_ctx_ptr;
            }
        }

        // finally, find no query contexts, so create a new one
        auto&& ctx = std::make_shared<QueryContext>();
        auto* ctx_raw_ptr = ctx.get();
        ctx_raw_ptr->set_query_id(query_id);
        ctx_raw_ptr->increment_num_fragments();
        context_map.emplace(query_id, std::move(ctx));
        return ctx_raw_ptr;
    }
}

QueryContextPtr QueryContextManager::get(const TUniqueId& query_id) {
    size_t i = std::hash<size_t>()(query_id.lo) & QUERY_CONTEXT_MAP_SLOT_NUM_MASK;
    auto& mutex = _mutexes[i];
    auto& context_map = _context_maps[i];
    auto& sc_map = _second_chance_maps[i];
    std::shared_lock<std::shared_mutex> read_lock(mutex);
    // lookup query context in context_map for the first chance
    auto it = context_map.find(query_id);
    if (it != context_map.end()) {
        return it->second;
    } else {
        // lookup query context in context_map for the second chance
        auto sc_it = sc_map.find(query_id);
        if (sc_it != sc_map.end()) {
            return sc_it->second;
        } else {
            return nullptr;
        }
    }
}

void QueryContextManager::remove(const TUniqueId& query_id) {
    size_t i = std::hash<size_t>()(query_id.lo) & QUERY_CONTEXT_MAP_SLOT_NUM_MASK;
    auto& mutex = _mutexes[i];
    auto& context_map = _context_maps[i];
    auto& sc_map = _second_chance_maps[i];

    std::unique_lock<std::shared_mutex> write_lock(mutex);

    // clean expired query contexts in sc_map
    auto sc_it = sc_map.begin();
    while (sc_it != sc_map.end()) {
        if (sc_it->second->is_expired()) {
            sc_it = sc_map.erase(sc_it);
        } else {
            ++sc_it;
        }
    }
    // return directly if query_ctx is absent
    auto it = context_map.find(query_id);
    if (it == context_map.end()) {
        return;
    }

    // the query context is really dead, so just cleanup
    if (it->second->is_dead()) {
        context_map.erase(it);
    } else if (it->second->is_finished()) {
        // although all of active fragments of the query context terminates, but some fragments maybe comes too late
        // in the future, so extend the lifetime of query context and wait for some time till fragments on wire have
        // vanished
        auto ctx = std::move(it->second);
        ctx->extend_lifetime();
        context_map.erase(it);
        sc_map.emplace(query_id, std::move(ctx));
    }
}

void QueryContextManager::clear() {
    std::vector<std::unique_lock<std::shared_mutex>> locks;
    locks.reserve(_mutexes.size());
    for (int i = 0; i < _mutexes.size(); ++i) {
        locks.emplace_back(_mutexes[i]);
    }
    _second_chance_maps.clear();
    _context_maps.clear();
}

} // namespace starrocks::pipeline
