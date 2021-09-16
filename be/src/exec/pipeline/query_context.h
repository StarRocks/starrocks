// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <atomic>
#include <chrono>
#include <list>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "gen_cpp/InternalService_types.h" // for TQueryOptions
#include "gen_cpp/Types_types.h"           // for TUniqueId
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "util/hash_util.hpp"
#include "util/threadpool.h"

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
    void set_query_id(const TUniqueId& query_id) { this->_query_id = _query_id; }
    TUniqueId query_id() { return _query_id; }
    RuntimeState* runtime_state() { return _runtime_state.get(); }
    void set_total_fragments(size_t total_fragments) { _total_fragments = total_fragments; }

    void increment_num_fragments() {
        _num_fragments.fetch_add(1);
        _num_active_fragments.fetch_add(1);
    }

    bool count_down_fragment() { return _num_active_fragments.fetch_sub(1) == 1; }

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

    void add_token(std::unique_ptr<ThreadPoolToken>&& token) {
        std::lock_guard lock(_tokens_mutex);
        _tokens.emplace_back(std::move(token));
    }

    bool should_be_reaped() {
        if (UNLIKELY(_num_active_fragments != 0)) {
            return false;
        }
        std::lock_guard lock(_tokens_mutex);
        auto token_it = _tokens.begin();
        while (token_it != _tokens.end()) {
            if ((*token_it)->is_active()) {
                return false;
            } else {
                _tokens.erase(token_it++);
            }
        }
        return true;
    }

private:
    std::unique_ptr<RuntimeState> _runtime_state;
    std::shared_ptr<RuntimeProfile> _runtime_profile;
    std::unique_ptr<MemTracker> _mem_tracker;
    TQueryOptions _query_options;
    TUniqueId _query_id;
    std::unique_ptr<FragmentContextManager> _fragment_mgr;
    std::mutex _tokens_mutex;
    std::list<std::unique_ptr<ThreadPoolToken>> _tokens;
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
    void unregister(const TUniqueId& query_id);

private:
    //TODO(by satanson)
    // A multi-shard map may be more efficient
    std::mutex _lock;
    std::unordered_map<TUniqueId, QueryContextPtr> _contexts;
};

class DyingQueryContextReaper {
public:
    explicit DyingQueryContextReaper(const int& size);
    void add(QueryContextPtr&& query_ctx);
    void add(size_t i, QueryContextList&& query_ctx_list);
    std::list<QueryContextPtr> get(size_t i);
    size_t slot_size() { return _size; }
    // the number among 1, 2, ..., p-1 is co-prime with p, so the it can be used as a stride to
    // iterate all numbers from (0,1,2,...,p-1), that is a property of cyclic groups.
    size_t next_slot_stride() {
        auto i = _slot_gap.fetch_add(1);
        if (_size <= 2) {
            return 1;
        } else {
            return 1 + i % (_size - 1);
        }
    }

private:
    std::atomic<uint32_t> _slot_gap{0};
    // _size is a prime number
    const size_t _size;
    std::vector<std::unique_ptr<std::mutex>> _mutexes;
    std::vector<QueryContextList> _slots;
};
} // namespace pipeline
} // namespace starrocks