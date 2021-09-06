// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <atomic>
#include <mutex>
#include <unordered_map>

#include "exec/pipeline/pipeline_fwd.h"
#include "gen_cpp/InternalService_types.h" // for TQueryOptions
#include "gen_cpp/Types_types.h"           // for TUniqueId
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "util/hash_util.hpp"

namespace starrocks {
class MemTracker;
namespace pipeline {

// The context for all fragment of one query in one BE
class QueryContext {
public:
    QueryContext() : _num_fragments_initialized(false), _num_fragments(0) {}
    RuntimeState* get_runtime_state() { return _runtime_state.get(); }
    void set_num_fragments(size_t num_fragments) {
        bool old_value = false;
        if (_num_fragments_initialized.compare_exchange_strong(old_value, true)) {
            _num_fragments.store(num_fragments);
        }
    }
    bool count_down_fragment() { return _num_fragments.fetch_sub(1) == 1; }

private:
    std::unique_ptr<RuntimeState> _runtime_state;
    std::shared_ptr<RuntimeProfile> _runtime_profile;
    std::unique_ptr<MemTracker> _mem_tracker;
    TQueryOptions _query_options;
    TUniqueId _query_id;
    std::atomic<bool> _num_fragments_initialized;
    std::atomic<size_t> _num_fragments;
};

class QueryContextManager {
    DECLARE_SINGLETON(QueryContextManager);

public:
    QueryContext* get_or_register(const TUniqueId& query_id);

    QueryContext* get_raw(const TUniqueId& query_id);
    QueryContextPtr get(const TUniqueId& query_id);
    void unregister(const TUniqueId& query_id);

private:
    //TODO(by satanson)
    // A multi-shard map may be more efficient
    std::mutex _lock;
    std::unordered_map<TUniqueId, QueryContextPtr> _contexts;
};
} // namespace pipeline
} // namespace starrocks