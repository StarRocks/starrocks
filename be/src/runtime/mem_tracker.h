// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/mem_tracker.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "common/status.h"
#include "util/metrics.h"
#include "util/runtime_profile.h"
#include "util/spinlock.h"

namespace starrocks {

class MemTracker;
class RuntimeState;

/// A MemTracker tracks memory consumption; it contains an optional limit
/// and can be arranged into a tree structure such that the consumption tracked
/// by a MemTracker is also tracked by its ancestors.
///
/// We use a five-level hierarchy of mem trackers: process, pool, query, fragment
/// instance. Specific parts of the fragment (exec nodes, sinks, etc) will add a
/// fifth level when they are initialized. This function also initializes a user
/// function mem tracker (in the fifth level).
///
/// By default, memory consumption is tracked via calls to Consume()/Release(), either to
/// the tracker itself or to one of its descendents. Alternatively, a consumption metric
/// can specified, and then the metric's value is used as the consumption rather than the
/// tally maintained by Consume() and Release(). A tcmalloc metric is used to track
/// process memory consumption, since the process memory usage may be higher than the
/// computed total memory (tcmalloc does not release deallocated memory immediately).
//
/// GcFunctions can be attached to a MemTracker in order to free up memory if the limit is
/// reached. If LimitExceeded() is called and the limit is exceeded, it will first call
/// the GcFunctions to try to free memory and recheck the limit. For example, the process
/// tracker has a GcFunction that releases any unused memory still held by tcmalloc, so
/// this will be called before the process limit is reported as exceeded. GcFunctions are
/// called in the order they are added, so expensive functions should be added last.
/// GcFunctions are called with a global lock held, so should be non-blocking and not
/// call back into MemTrackers, except to release memory.
//
/// This class is thread-safe.
class MemTracker {
public:
    // I want to get a snapshot of the mem_tracker, but don't want to copy all the field of MemTracker.
    // SimpleItem contains the most important field of MemTracker.
    // Current this is only used for list_mem_usage
    // TODO: use a better name?
    struct SimpleItem {
        std::string label;
        std::string parent_label;
        size_t level = 0;
        int64_t limit = 0;
        int64_t cur_consumption = 0;
        int64_t peak_consumption = 0;
    };

    enum Type {
        NO_SET,
        PROCESS,
        QUERY_POOL,
        QUERY,
        LOAD,
        CONSISTENCY,
        COMPACTION,
        SCHEMA_CHANGE_TASK,
        RESOURCE_GROUP,
        RESOURCE_GROUP_BIG_QUERY
    };

    /// 'byte_limit' < 0 means no limit
    /// 'label' is the label used in the usage string (LogUsage())
    /// If 'auto_unregister' is true, never call unregister_from_parent().
    /// If 'log_usage_if_zero' is false, this tracker (and its children) will not be included
    /// in LogUsage() output if consumption is 0.
    explicit MemTracker(int64_t byte_limit = -1, std::string label = std::string(), MemTracker* parent = nullptr,
                        bool auto_unregister = true);

    explicit MemTracker(Type type, int64_t byte_limit = -1, std::string label = std::string(),
                        MemTracker* parent = nullptr, bool auto_unregister = true);

    /// C'tor for tracker for which consumption counter is created as part of a profile.
    /// The counter is created with name COUNTER_NAME.
    MemTracker(RuntimeProfile* profile, int64_t byte_limit, std::string label = std::string(),
               MemTracker* parent = nullptr, bool auto_unregister = true);

    ~MemTracker();

    // Removes this tracker from _parent->_child_trackers.
    void unregister_from_parent() {
        DCHECK(_parent != nullptr);
        std::lock_guard<std::mutex> l(_parent->_child_trackers_lock);
        _parent->_child_trackers.erase(_child_tracker_it);
        _child_tracker_it = _parent->_child_trackers.end();
    }

    // used for single mem_tracker
    void set(int64_t bytes) { _consumption->set(bytes); }

    void consume(int64_t bytes) {
        if (bytes <= 0) {
            if (bytes < 0) release(-bytes);
            return;
        }
        for (auto* tracker : _all_trackers) {
            tracker->_consumption->add(bytes);
        }
    }

    void release_without_root() {
        int64_t bytes = consumption();
        if (bytes != 0) {
            for (size_t i = 0; i < _all_trackers.size() - 1; i++) {
                _all_trackers[i]->_consumption->add(-bytes);
            }
        }
    }

    void list_mem_usage(std::vector<SimpleItem>* items, size_t cur_level, size_t upper_level) const {
        SimpleItem item;
        item.label = _label;
        if (_parent != nullptr) {
            item.parent_label = _parent->label();
        } else {
            item.parent_label = "";
        }
        item.level = cur_level;
        item.limit = _limit;
        item.cur_consumption = _consumption->current_value();
        item.peak_consumption = _consumption->value();

        (*items).emplace_back(item);

        if (cur_level < upper_level) {
            std::lock_guard<std::mutex> l(_child_trackers_lock);
            for (const auto& child : _child_trackers) {
                child->list_mem_usage(items, cur_level + 1, upper_level);
            }
        }
    }

    /// Increases consumption of this tracker and its ancestors by 'bytes' only if
    /// they can all consume 'bytes'. If this brings any of them over, none of them
    /// are updated.
    /// Returns true if the try succeeded.
    WARN_UNUSED_RESULT
    MemTracker* try_consume(int64_t bytes) {
        if (UNLIKELY(bytes <= 0)) return nullptr;
        int64_t i;
        // Walk the tracker tree top-down.
        for (i = _all_trackers.size() - 1; i >= 0; --i) {
            MemTracker* tracker = _all_trackers[i];
            const int64_t limit = tracker->limit();
            if (limit < 0) {
                tracker->_consumption->add(bytes); // No limit at this tracker.
            } else {
                if (LIKELY(tracker->_consumption->try_add(bytes, limit))) {
                    continue;
                } else {
                    // Failed for this mem tracker. Roll back the ones that succeeded.
                    for (int64_t j = _all_trackers.size() - 1; j > i; --j) {
                        _all_trackers[j]->_consumption->add(-bytes);
                    }
                    return tracker;
                }
            }
        }
        // Everyone succeeded, return.
        DCHECK_EQ(i, -1);
        return nullptr;
    }

    /// Decreases consumption of this tracker and its ancestors by 'bytes'.
    void release(int64_t bytes) {
        if (bytes <= 0) {
            if (bytes < 0) consume(-bytes);
            return;
        }
        for (auto* tracker : _all_trackers) {
            tracker->_consumption->add(-bytes);
        }
    }

    // Returns true if a valid limit of this tracker or one of its ancestors is exceeded.
    bool any_limit_exceeded() {
        for (auto& _limit_tracker : _limit_trackers) {
            if (_limit_tracker->limit_exceeded()) {
                return true;
            }
        }
        return false;
    }

    // Return limit exceeded tracker or null
    MemTracker* find_limit_exceeded_tracker() const {
        for (auto& _limit_tracker : _limit_trackers) {
            if (_limit_tracker->limit_exceeded()) {
                return _limit_tracker;
            }
        }
        return nullptr;
    }

    // Returns the maximum consumption that can be made without exceeding the limit on
    // this tracker or any of its parents. Returns int64_t::max() if there are no
    // limits and a negative value if any limit is already exceeded.
    int64_t spare_capacity() const {
        int64_t result = std::numeric_limits<int64_t>::max();
        for (auto _limit_tracker : _limit_trackers) {
            int64_t mem_left = _limit_tracker->limit() - _limit_tracker->consumption();
            result = std::min(result, mem_left);
        }
        return result;
    }

    bool limit_exceeded() const { return _limit >= 0 && _limit < consumption(); }

    void set_limit(int64_t limit) { _limit = limit; }

    int64_t limit() const { return _limit; }

    bool has_limit() const { return _limit >= 0; }

    const std::string& label() const { return _label; }

    /// Returns the lowest limit for this tracker and its ancestors. Returns
    /// -1 if there is no limit.
    int64_t lowest_limit() const {
        if (_limit_trackers.empty()) return -1;
        int64_t v = std::numeric_limits<int64_t>::max();
        for (auto _limit_tracker : _limit_trackers) {
            DCHECK(_limit_tracker->has_limit());
            v = std::min(v, _limit_tracker->limit());
        }
        return v;
    }

    int64_t consumption() const { return _consumption->current_value(); }

    int64_t peak_consumption() const { return _consumption->value(); }

    MemTracker* parent() const { return _parent; }

    Status check_mem_limit(const std::string& msg) const;

    std::string err_msg(const std::string& msg) const;

    static const std::string COUNTER_NAME;

    std::string debug_string() {
        std::stringstream msg;
        msg << "limit: " << _limit << "; "
            << "consumption: " << _consumption->current_value() << "; "
            << "label: " << _label << "; "
            << "all tracker size: " << _all_trackers.size() << "; "
            << "limit trackers size: " << _limit_trackers.size() << "; "
            << "parent is null: " << ((_parent == nullptr) ? "true" : "false") << "; ";
        return msg.str();
    }

    Type type() const { return _type; }

private:
    // Walks the MemTracker hierarchy and populates _all_trackers and _limit_trackers
    void Init();

    // Adds tracker to _child_trackers
    void add_child_tracker(MemTracker* tracker) {
        std::lock_guard<std::mutex> l(_child_trackers_lock);
        tracker->_child_tracker_it = _child_trackers.insert(_child_trackers.end(), tracker);
    }

    Type _type{NO_SET};

    int64_t _limit; // in bytes

    std::string _label;
    MemTracker* _parent;

    /// in bytes; not owned
    RuntimeProfile::HighWaterMarkCounter* _consumption;

    /// holds _consumption counter if not tied to a profile
    RuntimeProfile::HighWaterMarkCounter _local_counter;

    std::vector<MemTracker*> _all_trackers;   // this tracker plus all of its ancestors
    std::vector<MemTracker*> _limit_trackers; // _all_trackers with valid limits

    // All the child trackers of this tracker. Used for error reporting only.
    // i.e., Updating a parent tracker does not update the children.
    mutable std::mutex _child_trackers_lock;
    std::list<MemTracker*> _child_trackers;
    // Iterator into _parent->_child_trackers for this object. Stored to have O(1)
    // remove.
    std::list<MemTracker*>::iterator _child_tracker_it;

    // If true, calls unregister_from_parent() in the dtor. This is only used for
    // the query wide trackers to remove it from the process mem tracker. The
    // process tracker never gets deleted so it is safe to reference it in the dtor.
    // The query tracker has lifetime shared by multiple plan fragments so it's hard
    // to do cleanup another way.
    bool _auto_unregister = false;
};

#define MEM_TRACKER_SAFE_CONSUME(mem_tracker, mem_bytes) \
    if (LIKELY((mem_tracker) != nullptr)) {              \
        (mem_tracker)->consume(mem_bytes);               \
    }

#define MEM_TRACKER_SAFE_RELEASE(mem_tracker, mem_bytes) \
    if (LIKELY((mem_tracker) != nullptr)) {              \
        (mem_tracker)->release(mem_bytes);               \
    }

template <typename T>
class DeleterWithMemTracker {
public:
    DeleterWithMemTracker(MemTracker* mem_tracker) : _mem_tracker(mem_tracker) {}

    void operator()(T* ptr) const {
        _mem_tracker->release(ptr->mem_usage());
        delete ptr;
    }

private:
    MemTracker* _mem_tracker = nullptr;
};

} // namespace starrocks
