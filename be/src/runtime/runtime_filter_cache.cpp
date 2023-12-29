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

#include "runtime/runtime_filter_cache.h"

#include <chrono>

#include "gutil/strings/substitute.h"
#include "util/thread.h"

namespace starrocks {

using std::chrono::seconds;
using std::chrono::milliseconds;
using std::chrono::steady_clock;
using std::chrono::duration_cast;
class RfCacheValue {
public:
    RfCacheValue() { _extend_lifetime(); }
    ~RfCacheValue() = default;
    void put_if_absent(int filter_id, const JoinRuntimeFilterPtr& filter) {
        _extend_lifetime();
        if (!_filters.count(filter_id)) {
            _filters[filter_id] = filter;
        }
    }

    JoinRuntimeFilterPtr get(int filter_id) const {
        _extend_lifetime();
        auto it = _filters.find(filter_id);
        if (it != _filters.end()) {
            return it->second;
        } else {
            return nullptr;
        }
    }

    bool is_expired() const {
        auto now = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
        return now > _deadline;
    }

private:
    void _extend_lifetime() const {
        _deadline = duration_cast<milliseconds>(steady_clock::now().time_since_epoch() + EXPIRE_SECONDS).count();
    }
    std::unordered_map<int, JoinRuntimeFilterPtr> _filters;
    mutable int64_t _deadline;
    static constexpr seconds EXPIRE_SECONDS = seconds(60);
};

class RfEvent {
public:
    RfEvent(const TUniqueId& query_id, int filter_id, std::string&& msg)
            : query_id(query_id),
              filter_id(filter_id),
              ts(duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count()),
              msg(std::move(msg)) {}
    ~RfEvent() = default;
    std::string to_string() { return strings::Substitute("[$0][$1] $2: $3", print_id(query_id), filter_id, ts, msg); }

private:
    TUniqueId query_id;
    int filter_id;
    long ts;
    std::string msg;
};
using RfEventPtr = std::shared_ptr<RfEvent>;
using RfEvents = std::list<RfEventPtr>;
class RfEventValue {
public:
    RfEventValue() = default;
    ~RfEventValue() = default;
    void add_rf_event(const TUniqueId& query_id, int filter_id, std::string&& msg) {
        _extend_lifetime();
        _events.push_back(std::make_shared<RfEvent>(query_id, filter_id, std::move(msg)));
    }

    std::list<std::string> get_events() {
        _extend_lifetime();
        std::list<std::string> event_strings;
        for (auto& _event : _events) {
            event_strings.push_back(_event->to_string());
        }
        return event_strings;
    }
    bool is_expired() const {
        auto now = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
        return now > _deadline;
    }

private:
    void _extend_lifetime() const {
        _deadline = duration_cast<milliseconds>(steady_clock::now().time_since_epoch() + EXPIRE_SECONDS).count();
    }
    RfEvents _events;
    mutable int64_t _deadline;
    static constexpr seconds EXPIRE_SECONDS = seconds(600);
};

RuntimeFilterCache::RuntimeFilterCache(size_t log2_num_slots)
        : _num_slots(1L << log2_num_slots),
          _slot_mask(_num_slots - 1),
          _mutexes(_num_slots),
          _filter_maps(_num_slots),
          _event_mutexes(_num_slots),
          _event_maps(_num_slots) {}
RuntimeFilterCache::~RuntimeFilterCache() {
    if (_clean_thread) {
        this->stop_clean_thread();
        _clean_thread->join();
    }
}
Status RuntimeFilterCache::init() {
    try {
        _clean_thread = std::make_shared<std::thread>(_clean_thread_func, this);
        Thread::set_thread_name(*_clean_thread, "rf_cache_clr");
        return Status::OK();
    } catch (...) {
        return Status::InternalError("Fail to create clean_thread of RuntimeFilterCache");
    }
}
void RuntimeFilterCache::_clean_thread_func(RuntimeFilterCache* cache) {
    while (!cache->is_stopped()) {
        cache->_clean_filters();
        cache->_clean_events(false);
        std::this_thread::sleep_for(milliseconds(100));
    }
}
void RuntimeFilterCache::_clean_events(bool force) {
    for (auto i = 0; i < _num_slots; ++i) {
        auto& mutex = _event_mutexes[i];
        auto& map = _event_maps[i];
        std::unique_lock write_lock(mutex);
        for (auto it = map.begin(); it != map.end();) {
            if (force || it->second->is_expired()) {
                it = map.erase(it);
            } else {
                ++it;
            }
        }
    }
}
void RuntimeFilterCache::_clean_filters() {
    for (auto i = 0; i < _num_slots; ++i) {
        auto& mutex = _mutexes[i];
        auto& map = _filter_maps[i];
        std::unique_lock write_lock(mutex);
        for (auto it = map.begin(); it != map.end();) {
            if (it->second->is_expired()) {
                it = map.erase(it);
            } else {
                ++it;
            }
        }
    }
}
size_t RuntimeFilterCache::_slot_idx(const TUniqueId& query_id) {
    return std::hash<size_t>()(query_id.lo) & _slot_mask;
}
void RuntimeFilterCache::put_if_absent(const TUniqueId& query_id, int filter_id, const JoinRuntimeFilterPtr& filter) {
    const auto slot_idx = _slot_idx(query_id);
    auto& mutex = _mutexes[slot_idx];
    auto& map = _filter_maps[slot_idx];
    _cache_times.fetch_add(1);
    std::unique_lock write_lock(mutex);
    if (!map.count(query_id)) {
        map[query_id] = std::make_shared<RfCacheValue>();
    }
    auto& cached = map[query_id];
    cached->put_if_absent(filter_id, filter);
}

JoinRuntimeFilterPtr RuntimeFilterCache::get(const TUniqueId& query_id, int filter_id) {
    const auto slot_idx = _slot_idx(query_id);
    auto& mutex = _mutexes[slot_idx];
    auto& map = _filter_maps[slot_idx];
    std::shared_lock read_lock(mutex);
    auto it = map.find(query_id);
    if (it == map.end()) {
        return nullptr;
    } else {
        auto filter = it->second->get(filter_id);
        _use_times.fetch_add(filter != nullptr);
        return filter;
    }
}

void RuntimeFilterCache::add_rf_event(const TUniqueId& query_id, int filter_id, std::string&& msg) {
    if (!_enable_trace) return;
    const auto slot_idx = _slot_idx(query_id);
    auto& mutex = _event_mutexes[slot_idx];
    auto& map = _event_maps[slot_idx];
    std::unique_lock write_lock(mutex);
    if (!map.count(query_id)) {
        map[query_id] = std::make_shared<RfEventValue>();
    }
    map[query_id]->add_rf_event(query_id, filter_id, std::move(msg));
}

std::unordered_map<std::string, std::list<std::string>> RuntimeFilterCache::get_events() {
    std::unordered_map<std::string, std::list<std::string>> events;
    for (auto i = 0; i < _num_slots; ++i) {
        auto& mutex = _event_mutexes[i];
        auto& map = _event_maps[i];
        std::shared_lock read_lock(mutex);
        for (auto& it : map) {
            events[print_id(it.first)] = it.second->get_events();
        }
    }
    return events;
}

void RuntimeFilterCache::remove(const TUniqueId& query_id) {
    const auto slot_idx = _slot_idx(query_id);
    auto& mutex = _mutexes[slot_idx];
    auto& map = _filter_maps[slot_idx];
    std::unique_lock write_lock(mutex);
    map.erase(query_id);
}

} // namespace starrocks
