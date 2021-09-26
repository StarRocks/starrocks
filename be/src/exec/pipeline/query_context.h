// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <atomic>
#include <chrono>
#include <mutex>
#include <unordered_map>

#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "gen_cpp/InternalService_types.h" // for TQueryOptions
#include "gen_cpp/Types_types.h"           // for TUniqueId
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "util/hash_util.hpp"

namespace starrocks {
class MemTracker;
namespace pipeline {

using std::chrono::seconds;
using std::chrono::milliseconds;
using std::chrono::steady_clock;
using std::chrono::duration_cast;
// The context for all fragment of one query in one BE
class QueryContext {
public:
    QueryContext();
    void set_query_id(const TUniqueId& query_id) { _query_id = query_id; }
    TUniqueId query_id() { return _query_id; }
    RuntimeState* runtime_state() { return _runtime_state.get(); }
    void set_total_fragments(size_t total_fragments) { _total_fragments = total_fragments; }

    void increment_num_fragments() {
        _num_fragments.fetch_add(1);
        _num_active_fragments.fetch_add(1);
    }

    bool count_down_fragments() { return _num_active_fragments.fetch_sub(1) == 1; }

    bool is_finished() { return _num_active_fragments.load() == 0; }

    void set_expire_seconds(int expire_seconds) { _expire_seconds = seconds(expire_seconds); }

    // now time point pass by deadline point.
    bool is_expired() {
        auto now = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
        return now > _deadline;
    }

    // add expired seconds to deadline
    void extend_lifetime() {
        _deadline = duration_cast<milliseconds>(steady_clock::now().time_since_epoch() + _expire_seconds).count();
    }

    FragmentContextManager* fragment_mgr();

    void cancel(const Status& status);

private:
    std::unique_ptr<RuntimeState> _runtime_state;
    std::shared_ptr<RuntimeProfile> _runtime_profile;
    std::unique_ptr<MemTracker> _mem_tracker;
    TQueryOptions _query_options;
    TUniqueId _query_id;
    std::unique_ptr<FragmentContextManager> _fragment_mgr;
    size_t _total_fragments;
    std::atomic<size_t> _num_fragments;
    std::atomic<size_t> _num_active_fragments;
    int64_t _deadline;
    seconds _expire_seconds;
};

class QueryContextManager {
    DECLARE_SINGLETON(QueryContextManager);

public:
    QueryContext* get_or_register(const TUniqueId& query_id);
    QueryContextPtr get(const TUniqueId& query_id);
    QueryContextPtr remove(const TUniqueId& query_id);

private:
    //TODO(by satanson)
    // A multi-shard map may be more efficient
    std::mutex _lock;
    std::unordered_map<TUniqueId, QueryContextPtr> _contexts;
};

} // namespace pipeline
} // namespace starrocks