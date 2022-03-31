// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once
#include <atomic>
#include <shared_mutex>
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

class RuntimeFilterCache {
public:
    explicit RuntimeFilterCache(size_t log2_num_slots);
    ~RuntimeFilterCache() = default;
    void put_if_absent(const TUniqueId& query_id, int filter_id, const JoinRuntimeFilterPtr& filter);
    JoinRuntimeFilterPtr get(const TUniqueId& query_id, int filter_id);
    void remove(const TUniqueId& query_id);
    size_t cache_times() const { return _cache_times; }
    size_t use_times() const { return _use_times; }
    void add_event(const TUniqueId& query_id, int filter_id, std::string&& msg);
    std::unordered_map<std::string, std::list<std::string>> get_events();

private:
    const size_t _num_slots;
    const size_t _slot_mask;
    std::vector<std::shared_mutex> _mutexes;
    std::vector<std::unordered_map<TUniqueId, RfCacheValuePtr>> _maps;
    std::vector<std::shared_mutex> _event_mutexes;
    std::vector<std::unordered_map<TUniqueId, RfEventValuePtr>> _event_maps;
    std::atomic<size_t> _cache_times{0};
    std::atomic<size_t> _use_times{0};
};
} // namespace starrocks