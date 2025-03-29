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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/mem_tracker.cpp

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

#include "runtime/mem_tracker.h"

#include <utility>

#include "runtime/runtime_state.h"
#include "service/backend_options.h"

namespace starrocks {

const std::string MemTracker::PEAK_MEMORY_USAGE = "PeakMemoryUsage";
const std::string MemTracker::ALLOCATED_MEMORY_USAGE = "AllocatedMemoryUsage";
const std::string MemTracker::DEALLOCATED_MEMORY_USAGE = "DeallocatedMemoryUsage";

static std::vector<std::pair<MemTrackerType, std::string>> s_mem_types = {
        {MemTrackerType::PROCESS, "process"},
        {MemTrackerType::QUERY_POOL, "query_pool"},
        {MemTrackerType::LOAD, "load"},
        {MemTrackerType::CONSISTENCY, "consistency"},
        {MemTrackerType::COMPACTION, "compaction"},
        {MemTrackerType::SCHEMA_CHANGE, "schema_change"},
        {MemTrackerType::JEMALLOC, "jemalloc_metadata"},
        {MemTrackerType::PASSTHROUGH, "passthrough"},
        {MemTrackerType::CONNECTOR_SCAN, "connector_scan"},
        {MemTrackerType::METADATA, "metadata"},
        {MemTrackerType::TABLET_METADATA, "tablet_metadata"},
        {MemTrackerType::ROWSET_METADATA, "rowset_metadata"},
        {MemTrackerType::SEGMENT_METADATA, "segment_metadata"},
        {MemTrackerType::COLUMN_METADATA, "column_metadata"},
        {MemTrackerType::TABLET_SCHEMA, "tablet_schema"},
        {MemTrackerType::SEGMENT_ZONEMAP, "segment_zonemap"},
        {MemTrackerType::SHORT_KEY_INDEX, "short_key_index"},
        {MemTrackerType::COLUMN_ZONEMAP_INDEX, "column_zonemap_index"},
        {MemTrackerType::ORDINAL_INDEX, "ordinal_index"},
        {MemTrackerType::BITMAP_INDEX, "bitmap_index"},
        {MemTrackerType::BLOOM_FILTER_INDEX, "bloom_filter_index"},
        {MemTrackerType::PAGE_CACHE, "page_cache"},
        {MemTrackerType::JIT_CACHE, "jit_cache"},
        {MemTrackerType::UPDATE, "update"},
        {MemTrackerType::CHUNK_ALLOCATOR, "chunk_allocator"},
        {MemTrackerType::CLONE, "clone"},
        {MemTrackerType::DATACACHE, "datacache"},
        {MemTrackerType::POCO_CONNECTION_POOL, "poco_connection_pool"},
        {MemTrackerType::REPLICATION, "replication"},
        {MemTrackerType::ROWSET_UPDATE_STATE, "rowset_update_state"},
        {MemTrackerType::INDEX_CACHE, "index_cache"},
        {MemTrackerType::DEL_VEC_CACHE, "del_vec_cache"},
        {MemTrackerType::COMPACTION_STATE, "compaction_state"},
};

static std::map<MemTrackerType, std::string> s_type_to_label_map;
static std::map<std::string, MemTrackerType> s_label_to_type_map;

std::vector<std::pair<MemTrackerType, std::string>>& MemTracker::mem_types() {
    return s_mem_types;
}

void MemTracker::init_type_label_map() {
    for (const auto& item : s_mem_types) {
        s_type_to_label_map[item.first] = item.second;
        s_label_to_type_map[item.second] = item.first;
    }
}

std::string MemTracker::type_to_label(MemTrackerType type) {
    auto iter = s_type_to_label_map.find(type);
    if (iter != s_type_to_label_map.end()) {
        return iter->second;
    } else {
        return "";
    }
}

MemTrackerType MemTracker::label_to_type(const std::string& label) {
    auto iter = s_label_to_type_map.find(label);
    if (iter != s_label_to_type_map.end()) {
        return iter->second;
    } else {
        return MemTrackerType::NO_SET;
    }
}

MemTracker::MemTracker(int64_t byte_limit, std::string label, MemTracker* parent)
        : _limit(byte_limit),
          _label(std::move(label)),
          _parent(parent),
          _consumption(&_local_consumption_counter),
          _local_consumption_counter(TUnit::BYTES,
                                     RuntimeProfile::Counter::create_strategy(TCounterAggregateType::AVG)),
          _allocation(&_local_allocation_counter),
          _local_allocation_counter(TUnit::BYTES),
          _deallocation(&_local_deallocation_counter),
          _local_deallocation_counter(TUnit::BYTES) {
    if (parent != nullptr) {
        _parent->add_child_tracker(this);
        _level = _parent->_level + 1;
    }
    Init();
}

MemTracker::MemTracker(MemTrackerType type, int64_t byte_limit, std::string label, MemTracker* parent)
        : _type(type),
          _limit(byte_limit),
          _label(std::move(label)),
          _parent(parent),
          _consumption(&_local_consumption_counter),
          _local_consumption_counter(TUnit::BYTES,
                                     RuntimeProfile::Counter::create_strategy(TCounterAggregateType::AVG)),
          _allocation(&_local_allocation_counter),
          _local_allocation_counter(TUnit::BYTES),
          _deallocation(&_local_deallocation_counter),
          _local_deallocation_counter(TUnit::BYTES) {
    if (parent != nullptr) {
        _parent->add_child_tracker(this);
        _level = _parent->_level + 1;
    }
    Init();
}

MemTracker::MemTracker(RuntimeProfile* profile, std::tuple<bool, bool, bool> attaching_info,
                       const std::string& counter_name_prefix, int64_t byte_limit, std::string label,
                       MemTracker* parent)
        : _limit(byte_limit),
          _label(std::move(label)),
          _parent(parent),
          _consumption(std::get<0>(attaching_info)
                               ? profile->AddHighWaterMarkCounter(
                                         counter_name_prefix + PEAK_MEMORY_USAGE, TUnit::BYTES,
                                         RuntimeProfile::Counter::create_strategy(TCounterAggregateType::AVG))
                               : &_local_consumption_counter),
          _local_consumption_counter(TUnit::BYTES,
                                     RuntimeProfile::Counter::create_strategy(TCounterAggregateType::AVG)),
          _allocation(std::get<1>(attaching_info)
                              ? profile->add_counter(counter_name_prefix + ALLOCATED_MEMORY_USAGE, TUnit::BYTES,
                                                     RuntimeProfile::Counter::create_strategy(TUnit::BYTES))
                              : &_local_allocation_counter),
          _local_allocation_counter(TUnit::BYTES),
          _deallocation(std::get<2>(attaching_info)
                                ? profile->add_counter(counter_name_prefix + DEALLOCATED_MEMORY_USAGE, TUnit::BYTES,
                                                       RuntimeProfile::Counter::create_strategy(TUnit::BYTES))
                                : &_local_deallocation_counter),
          _local_deallocation_counter(TUnit::BYTES) {
    if (parent != nullptr) {
        _parent->add_child_tracker(this);
        _level = _parent->_level + 1;
    }
    Init();
}

void MemTracker::Init() {
    DCHECK_GE(_limit, -1);
    // populate _all_trackers and _limit_trackers
    MemTracker* tracker = this;
    while (tracker != nullptr) {
        _all_trackers.push_back(tracker);
        if (tracker->has_limit()) _limit_trackers.push_back(tracker);
        tracker = tracker->_parent;
    }
    DCHECK_GT(_all_trackers.size(), 0);
    DCHECK_EQ(_all_trackers[0], this);
}

MemTracker::~MemTracker() {
    // return memory to root mem_tracker
    release_without_root();

    if (parent()) {
        unregister_from_parent();
    }
}

Status MemTracker::check_mem_limit(const std::string& msg) const {
    MemTracker* tracker = find_limit_exceeded_tracker();
    if (LIKELY(tracker == nullptr)) {
        return Status::OK();
    }

    return Status::MemoryLimitExceeded(tracker->err_msg(msg));
}

MemTracker::SimpleItem* MemTracker::_get_snapshot_internal(ObjectPool* pool, SimpleItem* parent,
                                                           size_t upper_level) const {
    SimpleItem* item = pool->add(new SimpleItem());
    item->label = _label;
    item->parent = parent;
    item->level = _level;
    item->limit = _limit;
    item->cur_consumption = _consumption->current_value();
    item->peak_consumption = _consumption->value();

    if (_level < upper_level) {
        std::lock_guard<std::mutex> l(_child_trackers_lock);
        item->childs.reserve(_child_trackers.size());
        for (const auto& child : _child_trackers) {
            item->childs.emplace_back(child->_get_snapshot_internal(pool, item, upper_level));
        }
    }

    return item;
}

std::string MemTracker::err_msg(const std::string& msg, RuntimeState* state) const {
    std::stringstream str;
    str << "Memory of " << label() << " exceed limit. " << msg << " ";
    str << "Backend: " << BackendOptions::get_localhost() << ", ";
    if (state != nullptr) {
        str << "fragment: " << print_id(state->fragment_instance_id()) << " ";
    }
    str << "Used: " << consumption() << ", Limit: " << limit() << ". ";
    switch (type()) {
    case MemTrackerType::NO_SET:
        break;
    case MemTrackerType::QUERY:
        str << "Mem usage has exceed the limit of single query, You can change the limit by "
               "set session variable query_mem_limit.";
        break;
    case MemTrackerType::PROCESS:
        str << "Mem usage has exceed the limit of BE";
        break;
    case MemTrackerType::QUERY_POOL:
        str << "Mem usage has exceed the limit of query pool";
        break;
    case MemTrackerType::LOAD:
        str << "Mem usage has exceed the limit of load";
        break;
    case MemTrackerType::CONSISTENCY:
        str << "Mem usage has exceed the limit of consistency";
        break;
    case MemTrackerType::SCHEMA_CHANGE_TASK:
        str << "You can change the limit by modify BE config [memory_limitation_per_thread_for_schema_change]";
        break;
    case MemTrackerType::RESOURCE_GROUP:
        // TODO: make default_wg configuable.
        if (label() == "default_wg") {
            str << "Mem usage has exceed the limit of query pool";
        } else {
            str << "Mem usage has exceed the limit of the resource group [" << label() << "]. "
                << "You can change the limit by modifying [mem_limit] of this group";
        }
        break;
    case MemTrackerType::RESOURCE_GROUP_BIG_QUERY:
        str << "Mem usage has exceed the big query limit of the resource group [" << label() << "]. "
            << "You can change the limit by modifying [big_query_mem_limit] of this group";
        break;
    default:
        break;
    }
    return str.str();
}

} // end namespace starrocks
