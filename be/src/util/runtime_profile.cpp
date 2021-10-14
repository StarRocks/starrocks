// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/runtime_profile.cpp

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

#include "util/runtime_profile.h"

#include <boost/thread/thread.hpp>
#include <iomanip>
#include <iostream>
#include <memory>
#include <utility>

#include "common/config.h"
#include "common/object_pool.h"
#include "gutil/map_util.h"
#include "util/cpu_info.h"
#include "util/debug_util.h"
#include "util/monotime.h"
#include "util/pretty_printer.h"

namespace starrocks {

// Thread counters name
// NOLINTNEXTLINE
static const std::string THREAD_TOTAL_TIME = "TotalWallClockTime";
// NOLINTNEXTLINE
static const std::string THREAD_USER_TIME = "UserTime";
// NOLINTNEXTLINE
static const std::string THREAD_SYS_TIME = "SysTime";
// NOLINTNEXTLINE
static const std::string THREAD_VOLUNTARY_CONTEXT_SWITCHES = "VoluntaryContextSwitches";
// NOLINTNEXTLINE
static const std::string THREAD_INVOLUNTARY_CONTEXT_SWITCHES = "InvoluntaryContextSwitches";

// The root counter name for all top level counters.
static const std::string ROOT_COUNTER = ""; // NOLINT

// NOLINTNEXTLINE
RuntimeProfile::PeriodicCounterUpdateState RuntimeProfile::_s_periodic_counter_update_state;

RuntimeProfile::RuntimeProfile(std::string name, bool is_averaged_profile)
        : _parent(nullptr),
          _pool(new ObjectPool()),
          _own_pool(false),
          _name(std::move(name)),
          _metadata(-1),
          _is_averaged_profile(is_averaged_profile),
          _counter_total_time(TUnit::TIME_NS, 0),
          _local_time_percent(0) {
    _counter_map["TotalTime"] = &_counter_total_time;
}

RuntimeProfile::~RuntimeProfile() {
    std::map<std::string, Counter*>::const_iterator iter;

    for (iter = _counter_map.begin(); iter != _counter_map.end(); ++iter) {
        stop_rate_counters_updates(iter->second);
        stop_sampling_counters_updates(iter->second);
    }

    std::set<std::vector<Counter*>*>::const_iterator buckets_iter;

    for (buckets_iter = _bucketing_counters.begin(); buckets_iter != _bucketing_counters.end(); ++buckets_iter) {
        // This is just a clean up. No need to perform conversion. Also, the underlying
        // counters might be gone already.
        stop_bucketing_counters_updates(*buckets_iter, false);
    }
}

void RuntimeProfile::merge(RuntimeProfile* other) {
    DCHECK(other != nullptr);

    // Merge this level
    {
        CounterMap::iterator dst_iter;
        CounterMap::const_iterator src_iter;
        std::lock_guard<std::mutex> l(_counter_map_lock);
        std::lock_guard<std::mutex> m(other->_counter_map_lock);

        for (src_iter = other->_counter_map.begin(); src_iter != other->_counter_map.end(); ++src_iter) {
            dst_iter = _counter_map.find(src_iter->first);

            if (dst_iter == _counter_map.end()) {
                _counter_map[src_iter->first] =
                        _pool->add(new Counter(src_iter->second->type(), src_iter->second->value()));
            } else {
                DCHECK(dst_iter->second->type() == src_iter->second->type());

                if (dst_iter->second->type() == TUnit::DOUBLE_VALUE) {
                    double new_val = dst_iter->second->double_value() + src_iter->second->double_value();
                    dst_iter->second->set(new_val);
                } else {
                    dst_iter->second->update(src_iter->second->value());
                }
            }
        }

        ChildCounterMap::const_iterator child_counter_src_itr;

        for (child_counter_src_itr = other->_child_counter_map.begin();
             child_counter_src_itr != other->_child_counter_map.end(); ++child_counter_src_itr) {
            auto& child_counters =
                    LookupOrInsert(&_child_counter_map, child_counter_src_itr->first, std::set<std::string>());
            child_counters.insert(child_counter_src_itr->second.begin(), child_counter_src_itr->second.end());
        }
    }

    {
        std::lock_guard<std::mutex> l(_children_lock);
        std::lock_guard<std::mutex> m(other->_children_lock);

        // Recursively merge children with matching names
        for (auto& i : other->_children) {
            RuntimeProfile* other_child = i.first;
            ChildMap::iterator j = _child_map.find(other_child->_name);
            RuntimeProfile* child = nullptr;

            if (j != _child_map.end()) {
                child = j->second;
            } else {
                child = _pool->add(new RuntimeProfile(other_child->_name));
                child->_local_time_percent = other_child->_local_time_percent;
                child->_metadata = other_child->_metadata;
                bool indent_other_child = i.second;
                _child_map[child->_name] = child;
                _children.push_back(std::make_pair(child, indent_other_child));
            }

            child->merge(other_child);
        }
    }
}

void RuntimeProfile::update(const TRuntimeProfileTree& thrift_profile) {
    int idx = 0;
    update(thrift_profile.nodes, &idx);
    DCHECK_EQ(idx, thrift_profile.nodes.size());
}

void RuntimeProfile::update(const std::vector<TRuntimeProfileNode>& nodes, int* idx) {
    DCHECK_LT(*idx, nodes.size());
    const TRuntimeProfileNode& node = nodes[*idx];
    {
        std::lock_guard<std::mutex> l(_counter_map_lock);
        // update this level
        std::map<std::string, Counter*>::iterator dst_iter;

        for (const auto& tcounter : node.counters) {
            CounterMap::iterator j = _counter_map.find(tcounter.name);

            if (j == _counter_map.end()) {
                _counter_map[tcounter.name] = _pool->add(new Counter(tcounter.type, tcounter.value));
            } else {
                if (j->second->type() != tcounter.type) {
                    LOG(ERROR) << "Cannot update counters with the same name (" << j->first << ") but different types.";
                } else {
                    j->second->set(tcounter.value);
                }
            }
        }

        ChildCounterMap::const_iterator child_counter_src_itr;

        for (child_counter_src_itr = node.child_counters_map.begin();
             child_counter_src_itr != node.child_counters_map.end(); ++child_counter_src_itr) {
            auto& child_counters =
                    LookupOrInsert(&_child_counter_map, child_counter_src_itr->first, std::set<std::string>());
            child_counters.insert(child_counter_src_itr->second.begin(), child_counter_src_itr->second.end());
        }
    }

    {
        std::lock_guard<std::mutex> l(_info_strings_lock);
        const InfoStrings& info_strings = node.info_strings;
        for (const std::string& key : node.info_strings_display_order) {
            // Look for existing info strings and update in place. If there
            // are new strings, add them to the end of the display order.
            // TODO: Is nodes.info_strings always a superset of
            // _info_strings? If so, can just copy the display order.
            InfoStrings::const_iterator it = info_strings.find(key);
            DCHECK(it != info_strings.end());
            InfoStrings::iterator existing = _info_strings.find(key);

            if (existing == _info_strings.end()) {
                _info_strings.insert(std::make_pair(key, it->second));
                _info_strings_display_order.push_back(key);
            } else {
                _info_strings[key] = it->second;
            }
        }
    }

    ++*idx;
    {
        std::lock_guard<std::mutex> l(_children_lock);

        // update children with matching names; create new ones if they don't match
        for (int i = 0; i < node.num_children; ++i) {
            const TRuntimeProfileNode& tchild = nodes[*idx];
            ChildMap::iterator j = _child_map.find(tchild.name);
            RuntimeProfile* child = nullptr;

            if (j != _child_map.end()) {
                child = j->second;
            } else {
                child = _pool->add(new RuntimeProfile(tchild.name));
                child->_metadata = tchild.metadata;
                _child_map[tchild.name] = child;
                _children.push_back(std::make_pair(child, tchild.indent));
            }

            child->update(nodes, idx);
        }
    }
}

void RuntimeProfile::divide(int n) {
    DCHECK_GT(n, 0);
    std::map<std::string, Counter*>::iterator iter;
    {
        std::lock_guard<std::mutex> l(_counter_map_lock);

        for (iter = _counter_map.begin(); iter != _counter_map.end(); ++iter) {
            if (iter->second->type() == TUnit::DOUBLE_VALUE) {
                iter->second->set(iter->second->double_value() / n);
            } else {
                int64_t value = iter->second->_value.load();
                value = value / n;
                iter->second->_value.store(value);
            }
        }
    }
    {
        std::lock_guard<std::mutex> l(_children_lock);

        for (auto& i : _child_map) {
            i.second->divide(n);
        }
    }
}

void RuntimeProfile::compute_time_in_profile() {
    compute_time_in_profile(total_time_counter()->value());
}

void RuntimeProfile::compute_time_in_profile(int64_t total) {
    if (total == 0) {
        return;
    }

    // Add all the total times in all the children
    int64_t total_child_time = 0;
    std::lock_guard<std::mutex> l(_children_lock);

    for (auto& i : _children) {
        total_child_time += i.first->total_time_counter()->value();
    }

    int64_t local_time = total_time_counter()->value() - total_child_time;
    // Counters have some margin, set to 0 if it was negative.
    local_time = std::max<int64_t>(0, local_time);
    _local_time_percent = static_cast<double>(local_time) / total;
    _local_time_percent = std::min(1.0, _local_time_percent) * 100;

    // Recurse on children
    for (auto& i : _children) {
        i.first->compute_time_in_profile(total);
    }
}

RuntimeProfile* RuntimeProfile::create_child(const std::string& name, bool indent, bool prepend) {
    std::lock_guard<std::mutex> l(_children_lock);
    auto iter = _child_map.find(name);
    if (iter != _child_map.end()) {
        return iter->second;
    }
    RuntimeProfile* child = _pool->add(new RuntimeProfile(name));
    ChildVector::iterator pos = prepend ? _children.begin() : _children.end();
    add_child_unlock(child, indent, pos);
    return child;
}

void RuntimeProfile::add_child_unlock(RuntimeProfile* child, bool indent, ChildVector::iterator pos) {
    DCHECK(child != nullptr);
    DCHECK(child->_parent == nullptr);
    _child_map[child->_name] = child;
    _children.insert(pos, std::make_pair(child, indent));
    child->_parent = this;
}

void RuntimeProfile::add_child(RuntimeProfile* child, bool indent, RuntimeProfile* loc) {
    std::lock_guard<std::mutex> l(_children_lock);
    if (loc == nullptr || _children.empty()) {
        add_child_unlock(child, indent, _children.end());
    } else {
        auto f = [loc](const std::pair<RuntimeProfile*, bool>& pair) { return pair.first == loc; };
        ChildVector::iterator pos = std::find_if(_children.begin(), _children.end(), f);
        DCHECK(pos != _children.end());
        add_child_unlock(child, indent, ++pos);
    }
}

void RuntimeProfile::get_children(std::vector<RuntimeProfile*>* children) {
    children->clear();
    std::lock_guard<std::mutex> l(_children_lock);

    for (auto& i : _child_map) {
        children->push_back(i.second);
    }
}

void RuntimeProfile::get_all_children(std::vector<RuntimeProfile*>* children) {
    std::lock_guard<std::mutex> l(_children_lock);

    for (auto& i : _child_map) {
        children->push_back(i.second);
        i.second->get_all_children(children);
    }
}

void RuntimeProfile::add_info_string(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> l(_info_strings_lock);
    InfoStrings::iterator it = _info_strings.find(key);

    if (it == _info_strings.end()) {
        _info_strings.insert(std::make_pair(key, value));
        _info_strings_display_order.push_back(key);
    } else {
        it->second = value;
    }
}

const std::string* RuntimeProfile::get_info_string(const std::string& key) {
    std::lock_guard<std::mutex> l(_info_strings_lock);
    InfoStrings::const_iterator it = _info_strings.find(key);

    if (it == _info_strings.end()) {
        return nullptr;
    }

    return &it->second;
}

#define ADD_COUNTER_IMPL(NAME, T)                                                                                    \
    RuntimeProfile::T* RuntimeProfile::NAME(const std::string& name, TUnit::type unit,                               \
                                            const std::string& parent_counter_name) {                                \
        DCHECK_EQ(_is_averaged_profile, false);                                                                      \
        std::lock_guard<std::mutex> l(_counter_map_lock);                                                            \
        if (_counter_map.find(name) != _counter_map.end()) {                                                         \
            return reinterpret_cast<T*>(_counter_map[name]);                                                         \
        }                                                                                                            \
        DCHECK(parent_counter_name == ROOT_COUNTER || _counter_map.find(parent_counter_name) != _counter_map.end()); \
        T* counter = _pool->add(new T(unit));                                                                        \
        _counter_map[name] = counter;                                                                                \
        auto& child_counters = LookupOrInsert(&_child_counter_map, parent_counter_name, std::set<std::string>());    \
        child_counters.insert(name);                                                                                 \
        return counter;                                                                                              \
    }

//ADD_COUNTER_IMPL(AddCounter, Counter);
ADD_COUNTER_IMPL(AddHighWaterMarkCounter, HighWaterMarkCounter)
//ADD_COUNTER_IMPL(AddConcurrentTimerCounter, ConcurrentTimerCounter);

RuntimeProfile::Counter* RuntimeProfile::add_counter(const std::string& name, TUnit::type type,
                                                     const std::string& parent_counter_name) {
    std::lock_guard<std::mutex> l(_counter_map_lock);
    if (auto iter = _counter_map.find(name); iter != _counter_map.end()) {
        return iter->second;
    }

    DCHECK(parent_counter_name == ROOT_COUNTER || _counter_map.find(parent_counter_name) != _counter_map.end());
    Counter* counter = _pool->add(new Counter(type, 0));
    _counter_map[name] = counter;
    _child_counter_map[parent_counter_name].insert(name);
    return counter;
}

RuntimeProfile::DerivedCounter* RuntimeProfile::add_derived_counter(const std::string& name, TUnit::type type,
                                                                    const DerivedCounterFunction& counter_fn,
                                                                    const std::string& parent_counter_name) {
    std::lock_guard<std::mutex> l(_counter_map_lock);

    if (_counter_map.find(name) != _counter_map.end()) {
        return nullptr;
    }

    DerivedCounter* counter = _pool->add(new DerivedCounter(type, counter_fn));
    _counter_map[name] = counter;
    auto& child_counters = LookupOrInsert(&_child_counter_map, parent_counter_name, std::set<std::string>());
    child_counters.insert(name);
    return counter;
}

RuntimeProfile::ThreadCounters* RuntimeProfile::add_thread_counters(const std::string& prefix) {
    ThreadCounters* counter = _pool->add(new ThreadCounters());
    counter->_total_time = add_counter(prefix + THREAD_TOTAL_TIME, TUnit::TIME_NS);
    counter->_user_time = add_counter(prefix + THREAD_USER_TIME, TUnit::TIME_NS, prefix + THREAD_TOTAL_TIME);
    counter->_sys_time = add_counter(prefix + THREAD_SYS_TIME, TUnit::TIME_NS, prefix + THREAD_TOTAL_TIME);
    counter->_voluntary_context_switches = add_counter(prefix + THREAD_VOLUNTARY_CONTEXT_SWITCHES, TUnit::UNIT);
    counter->_involuntary_context_switches = add_counter(prefix + THREAD_INVOLUNTARY_CONTEXT_SWITCHES, TUnit::UNIT);
    return counter;
}

RuntimeProfile::Counter* RuntimeProfile::get_counter(const std::string& name) {
    std::lock_guard<std::mutex> l(_counter_map_lock);

    if (_counter_map.find(name) != _counter_map.end()) {
        return _counter_map[name];
    }

    return nullptr;
}

void RuntimeProfile::get_counters(const std::string& name, std::vector<Counter*>* counters) {
    Counter* c = get_counter(name);

    if (c != nullptr) {
        counters->push_back(c);
    }

    std::lock_guard<std::mutex> l(_children_lock);

    for (auto& i : _children) {
        i.first->get_counters(name, counters);
    }
}

// Print the profile:
//  1. Profile Name
//  2. Info Strings
//  3. Counters
//  4. Children
void RuntimeProfile::pretty_print(std::ostream* s, const std::string& prefix) const {
    std::ostream& stream = *s;

    // create copy of _counter_map and _child_counter_map so we don't need to hold lock
    // while we call value() on the counters (some of those might be DerivedCounters)
    CounterMap counter_map;
    ChildCounterMap child_counter_map;
    {
        std::lock_guard<std::mutex> l(_counter_map_lock);
        counter_map = _counter_map;
        child_counter_map = _child_counter_map;
    }

    std::map<std::string, Counter*>::const_iterator total_time = counter_map.find("TotalTime");
    DCHECK(total_time != counter_map.end());

    stream.flags(std::ios::fixed);
    stream << prefix << _name << ":";

    if (total_time->second->value() != 0) {
        stream << "(Active: " << PrettyPrinter::print(total_time->second->value(), total_time->second->type())
               << ", non-child: " << std::setprecision(2) << _local_time_percent << "%)";
    }

    stream << std::endl;

    {
        std::lock_guard<std::mutex> l(_info_strings_lock);
        for (const std::string& key : _info_strings_display_order) {
            stream << prefix << "   - " << key << ": " << _info_strings.find(key)->second << std::endl;
        }
    }

    {
        // Print all the event timers as the following:
        // <EventKey> Timeline: 2s719ms
        //     - Event 1: 6.522us (6.522us)
        //     - Event 2: 2s288ms (2s288ms)
        //     - Event 3: 2s410ms (121.138ms)
        // The times in parentheses are the time elapsed since the last event.
        std::lock_guard<std::mutex> l(_event_sequences_lock);
        for (const EventSequenceMap::value_type& event_sequence : _event_sequence_map) {
            stream << prefix << "  " << event_sequence.first << ": "
                   << PrettyPrinter::print(event_sequence.second->elapsed_time(), TUnit::TIME_NS) << std::endl;

            int64_t last = 0L;
            for (const EventSequence::Event& event : event_sequence.second->events()) {
                stream << prefix << "     - " << event.first << ": "
                       << PrettyPrinter::print(event.second, TUnit::TIME_NS) << " ("
                       << PrettyPrinter::print(event.second - last, TUnit::TIME_NS) << ")" << std::endl;
                last = event.second;
            }
        }
    }

    RuntimeProfile::print_child_counters(prefix, ROOT_COUNTER, counter_map, child_counter_map, s);

    // create copy of _children so we don't need to hold lock while we call
    // pretty_print() on the children
    ChildVector children;
    {
        std::lock_guard<std::mutex> l(_children_lock);
        children = _children;
    }

    for (auto& i : children) {
        RuntimeProfile* profile = i.first;
        bool indent = i.second;
        profile->pretty_print(s, prefix + (indent ? "  " : ""));
    }
}

void RuntimeProfile::to_thrift(TRuntimeProfileTree* tree) {
    tree->nodes.clear();
    to_thrift(&tree->nodes);
}

void RuntimeProfile::to_thrift(std::vector<TRuntimeProfileNode>* nodes) {
    nodes->reserve(nodes->size() + _children.size());

    int index = nodes->size();
    nodes->push_back(TRuntimeProfileNode());
    TRuntimeProfileNode& node = (*nodes)[index];
    node.name = _name;
    node.num_children = _children.size();
    node.metadata = _metadata;
    node.indent = true;

    CounterMap counter_map;
    {
        std::lock_guard<std::mutex> l(_counter_map_lock);
        counter_map = _counter_map;
        node.child_counters_map = _child_counter_map;
    }

    for (std::map<std::string, Counter*>::const_iterator iter = counter_map.begin(); iter != counter_map.end();
         ++iter) {
        TCounter counter;
        counter.name = iter->first;
        counter.value = iter->second->value();
        counter.type = iter->second->type();
        node.counters.push_back(counter);
    }

    {
        std::lock_guard<std::mutex> l(_info_strings_lock);
        node.info_strings = _info_strings;
        node.info_strings_display_order = _info_strings_display_order;
    }

    ChildVector children;
    {
        std::lock_guard<std::mutex> l(_children_lock);
        children = _children;
    }

    for (auto& i : children) {
        int child_idx = nodes->size();
        i.first->to_thrift(nodes);
        // fix up indentation flag
        (*nodes)[child_idx].indent = i.second;
    }
}

int64_t RuntimeProfile::units_per_second(const RuntimeProfile::Counter* total_counter,
                                         const RuntimeProfile::Counter* timer) {
    DCHECK(total_counter->type() == TUnit::BYTES || total_counter->type() == TUnit::UNIT);
    DCHECK(timer->type() == TUnit::TIME_NS);

    if (timer->value() == 0) {
        return 0;
    }

    double secs = static_cast<double>(timer->value()) / 1000.0 / 1000.0 / 1000.0;
    return total_counter->value() / secs;
}

[[maybe_unused]] int64_t RuntimeProfile::counter_sum(const std::vector<Counter*>* counters) {
    int64_t value = 0;
    for (auto counter : *counters) {
        value += counter->value();
    }
    return value;
}

RuntimeProfile::Counter* RuntimeProfile::add_rate_counter(const std::string& name, Counter* src_counter) {
    TUnit::type dst_type;

    switch (src_counter->type()) {
    case TUnit::BYTES:
        dst_type = TUnit::BYTES_PER_SECOND;
        break;

    case TUnit::UNIT:
        dst_type = TUnit::UNIT_PER_SECOND;
        break;

    default:
        DCHECK(false) << "Unsupported src counter type: " << src_counter->type();
        return nullptr;
    }

    Counter* dst_counter = add_counter(name, dst_type);
    register_periodic_counter(src_counter, nullptr, dst_counter, RATE_COUNTER);
    return dst_counter;
}

RuntimeProfile::Counter* RuntimeProfile::add_rate_counter(const std::string& name, SampleFn fn, TUnit::type dst_type) {
    Counter* dst_counter = add_counter(name, dst_type);
    register_periodic_counter(nullptr, std::move(fn), dst_counter, RATE_COUNTER);
    return dst_counter;
}

RuntimeProfile::Counter* RuntimeProfile::add_sampling_counter(const std::string& name, Counter* src_counter) {
    DCHECK(src_counter->type() == TUnit::UNIT);
    Counter* dst_counter = add_counter(name, TUnit::DOUBLE_VALUE);
    register_periodic_counter(src_counter, nullptr, dst_counter, SAMPLING_COUNTER);
    return dst_counter;
}

RuntimeProfile::Counter* RuntimeProfile::add_sampling_counter(const std::string& name, SampleFn sample_fn) {
    Counter* dst_counter = add_counter(name, TUnit::DOUBLE_VALUE);
    register_periodic_counter(nullptr, std::move(sample_fn), dst_counter, SAMPLING_COUNTER);
    return dst_counter;
}

void RuntimeProfile::add_bucketing_counters(const std::string& name, const std::string& parent_counter_name,
                                            Counter* src_counter, int num_buckets, std::vector<Counter*>* buckets) {
    {
        std::lock_guard<std::mutex> l(_counter_map_lock);
        _bucketing_counters.insert(buckets);
    }

    for (int i = 0; i < num_buckets; ++i) {
        std::stringstream counter_name;
        counter_name << name << "=" << i;
        buckets->push_back(add_counter(counter_name.str(), TUnit::DOUBLE_VALUE, parent_counter_name));
    }

    std::lock_guard<std::mutex> l(_s_periodic_counter_update_state.lock);

    if (_s_periodic_counter_update_state.update_thread == nullptr) {
        _s_periodic_counter_update_state.update_thread =
                std::make_unique<boost::thread>(&RuntimeProfile::periodic_counter_update_loop);
    }

    BucketCountersInfo info{.src_counter = src_counter, .num_sampled = 0};
    _s_periodic_counter_update_state.bucketing_counters[buckets] = info;
}

RuntimeProfile::EventSequence* RuntimeProfile::add_event_sequence(const std::string& name) {
    std::lock_guard<std::mutex> l(_event_sequences_lock);
    EventSequenceMap::iterator timer_it = _event_sequence_map.find(name);

    if (timer_it != _event_sequence_map.end()) {
        return timer_it->second;
    }

    EventSequence* timer = _pool->add(new EventSequence());
    _event_sequence_map[name] = timer;
    return timer;
}

void RuntimeProfile::register_periodic_counter(Counter* src_counter, SampleFn sample_fn, Counter* dst_counter,
                                               PeriodicCounterType type) {
    DCHECK(src_counter == nullptr || sample_fn == nullptr);

    std::lock_guard<std::mutex> l(_s_periodic_counter_update_state.lock);

    if (_s_periodic_counter_update_state.update_thread == nullptr) {
        _s_periodic_counter_update_state.update_thread =
                std::make_unique<boost::thread>(&RuntimeProfile::periodic_counter_update_loop);
    }

    switch (type) {
    case RATE_COUNTER: {
        RateCounterInfo counter;
        counter.src_counter = src_counter;
        counter.sample_fn = std::move(sample_fn);
        counter.elapsed_ms = 0;
        _s_periodic_counter_update_state.rate_counters[dst_counter] = counter;
        break;
    }

    case SAMPLING_COUNTER: {
        SamplingCounterInfo counter;
        counter.src_counter = src_counter;
        counter.sample_fn = std::move(sample_fn);
        counter.num_sampled = 0;
        counter.total_sampled_value = 0;
        _s_periodic_counter_update_state.sampling_counters[dst_counter] = counter;
        break;
    }

    default:
        DCHECK(false) << "Unsupported PeriodicCounterType:" << type;
    }
}

void RuntimeProfile::stop_rate_counters_updates(Counter* rate_counter) {
    std::lock_guard<std::mutex> l(_s_periodic_counter_update_state.lock);
    _s_periodic_counter_update_state.rate_counters.erase(rate_counter);
}

void RuntimeProfile::stop_sampling_counters_updates(Counter* sampling_counter) {
    std::lock_guard<std::mutex> l(_s_periodic_counter_update_state.lock);
    _s_periodic_counter_update_state.sampling_counters.erase(sampling_counter);
}

void RuntimeProfile::stop_bucketing_counters_updates(std::vector<Counter*>* buckets, bool convert) {
    int64_t num_sampled = 0;
    {
        std::lock_guard<std::mutex> l(_s_periodic_counter_update_state.lock);
        PeriodicCounterUpdateState::BucketCountersMap::const_iterator itr =
                _s_periodic_counter_update_state.bucketing_counters.find(buckets);

        if (itr != _s_periodic_counter_update_state.bucketing_counters.end()) {
            num_sampled = itr->second.num_sampled;
            _s_periodic_counter_update_state.bucketing_counters.erase(buckets);
        }
    }

    if (convert && num_sampled > 0) {
        for (Counter* counter : *buckets) {
            double perc = 100.0 * counter->value() / (double)num_sampled;
            counter->set(perc);
        }
    }
}

RuntimeProfile::PeriodicCounterUpdateState::PeriodicCounterUpdateState() {}

RuntimeProfile::PeriodicCounterUpdateState::~PeriodicCounterUpdateState() {
    if (_s_periodic_counter_update_state.update_thread != nullptr) {
        {
            // Lock to ensure the update thread will see the update to _done
            std::lock_guard<std::mutex> l(_s_periodic_counter_update_state.lock);
            _done = true;
        }
        _s_periodic_counter_update_state.update_thread->join();
    }
}

void RuntimeProfile::periodic_counter_update_loop() {
    while (!_s_periodic_counter_update_state._done) {
        boost::system_time before_time = boost::get_system_time();
        SleepFor(MonoDelta::FromMilliseconds(config::periodic_counter_update_period_ms));
        boost::posix_time::time_duration elapsed = boost::get_system_time() - before_time;
        int elapsed_ms = elapsed.total_milliseconds();

        std::lock_guard<std::mutex> l(_s_periodic_counter_update_state.lock);

        for (PeriodicCounterUpdateState::RateCounterMap::iterator it =
                     _s_periodic_counter_update_state.rate_counters.begin();
             it != _s_periodic_counter_update_state.rate_counters.end(); ++it) {
            it->second.elapsed_ms += elapsed_ms;
            int64_t value;

            if (it->second.src_counter != nullptr) {
                value = it->second.src_counter->value();
            } else {
                DCHECK(it->second.sample_fn != nullptr);
                value = it->second.sample_fn();
            }

            int64_t rate = value * 1000 / (it->second.elapsed_ms);
            it->first->set(rate);
        }

        for (PeriodicCounterUpdateState::SamplingCounterMap::iterator it =
                     _s_periodic_counter_update_state.sampling_counters.begin();
             it != _s_periodic_counter_update_state.sampling_counters.end(); ++it) {
            ++it->second.num_sampled;
            int64_t value;

            if (it->second.src_counter != nullptr) {
                value = it->second.src_counter->value();
            } else {
                DCHECK(it->second.sample_fn != nullptr);
                value = it->second.sample_fn();
            }

            it->second.total_sampled_value += value;
            double average = static_cast<double>(it->second.total_sampled_value) / it->second.num_sampled;
            it->first->set(average);
        }

        for (auto& bucketing_counter : _s_periodic_counter_update_state.bucketing_counters) {
            int64_t val = bucketing_counter.second.src_counter->value();

            if (val >= bucketing_counter.first->size()) {
                val = bucketing_counter.first->size() - 1;
            }

            bucketing_counter.first->at(val)->update(1);
            ++bucketing_counter.second.num_sampled;
        }
    }
}

void RuntimeProfile::print_child_counters(const std::string& prefix, const std::string& counter_name,
                                          const CounterMap& counter_map, const ChildCounterMap& child_counter_map,
                                          std::ostream* s) {
    std::ostream& stream = *s;
    ChildCounterMap::const_iterator itr = child_counter_map.find(counter_name);

    if (itr != child_counter_map.end()) {
        const std::set<std::string>& child_counters = itr->second;
        for (const std::string& child_counter : child_counters) {
            CounterMap::const_iterator iter = counter_map.find(child_counter);
            DCHECK(iter != counter_map.end());
            stream << prefix << "   - " << iter->first << ": "
                   << PrettyPrinter::print(iter->second->value(), iter->second->type()) << std::endl;
            RuntimeProfile::print_child_counters(prefix + "  ", child_counter, counter_map, child_counter_map, s);
        }
    }
}

} // namespace starrocks
