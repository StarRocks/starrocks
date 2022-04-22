// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once
#include <atomic>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "exprs/vectorized/runtime_filter.h"
#include "gen_cpp/Types_types.h" // for TUniqueId

namespace starrocks {

using JoinRuntimeFilterPtr = std::shared_ptr<vectorized::JoinRuntimeFilter>;
class RfCacheValue;
using RfCacheValueRawPtr = RfCacheValue*;
using RfCacheValuePtr = std::shared_ptr<RfCacheValue>;
class RfEventValue;
using RfEventValueRawPtr = RfEventValue*;
using RfEventValuePtr = std::shared_ptr<RfEventValue>;

struct RfTracePoint {
    struct UnifiedQueryId {
        UnifiedQueryId(const TUniqueId& tquery_id) : query_id(tquery_id) {}
        UnifiedQueryId(const PUniqueId& pquery_id) {
            query_id.lo = pquery_id.lo();
            query_id.hi = pquery_id.hi();
        }
        operator TUniqueId() const { return query_id; }

    private:
        TUniqueId query_id;
    } query_id;

    int filter_id;
    std::string network;
    std::string msg;
};

class RuntimeFilterCache {
public:
    explicit RuntimeFilterCache(size_t log2_num_slots);
    ~RuntimeFilterCache();
    Status init();
    void put_if_absent(const TUniqueId& query_id, int filter_id, const JoinRuntimeFilterPtr& filter);
    JoinRuntimeFilterPtr get(const TUniqueId& query_id, int filter_id);
    void remove(const TUniqueId& query_id);
    size_t cache_times() const { return _cache_times; }
    size_t use_times() const { return _use_times; }
    void add_rf_event(const TUniqueId& query_id, int filter_id, std::string&& msg);
    std::unordered_map<std::string, std::list<std::string>> get_events();
    void set_enable_trace(bool enable) {
        _enable_trace.store(enable);
        if (!enable) {
            _clean_events(true);
        }
    }
    bool enable_trace() const { return _enable_trace; }
    void stop_clean_thread() { _stop.store(true); }
    bool is_stopped() { return _stop; }
    std::thread& clean_thread() { return *_clean_thread.get(); }

private:
    static void _clean_thread_func(RuntimeFilterCache* cache);
    void _clean_events(bool force);
    void _clean_filters();
    size_t _slot_idx(const TUniqueId& query_id);
    const size_t _num_slots;
    const size_t _slot_mask;
    std::vector<std::shared_mutex> _mutexes;
    std::vector<std::unordered_map<TUniqueId, RfCacheValuePtr>> _filter_maps;
    std::vector<std::shared_mutex> _event_mutexes;
    std::vector<std::unordered_map<TUniqueId, RfEventValuePtr>> _event_maps;
    mutable std::atomic<bool> _enable_trace{false};
    std::atomic<size_t> _cache_times{0};
    std::atomic<size_t> _use_times{0};
    std::atomic<bool> _stop{false};
    std::shared_ptr<std::thread> _clean_thread;
};
} // namespace starrocks