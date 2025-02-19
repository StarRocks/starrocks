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

std::string MemTracker::type_to_label(MemTrackerType type) {
    switch (type) {
    case MemTrackerType::NO_SET:
        return "";
    case MemTrackerType::PROCESS:
        return "process";
    case MemTrackerType::QUERY_POOL:
        return "query_pool";
    case MemTrackerType::LOAD:
        return "load";
    case MemTrackerType::CONSISTENCY:
        return "consistency";
    case MemTrackerType::COMPACTION:
        return "compaction";
    case MemTrackerType::SCHEMA_CHANGE:
        return "schema_change";
    case MemTrackerType::JEMALLOC:
        return "jemalloc_metadata";
    case MemTrackerType::PASSTHROUGH:
        return "passthrough";
    case MemTrackerType::CONNECTOR_SCAN:
        return "connector_scan";
    case MemTrackerType::METADATA:
        return "metadata";
    case MemTrackerType::TABLET_METADATA:
        return "tablet_metadata";
    case MemTrackerType::ROWSET_METADATA:
        return "rowset_metadata";
    case MemTrackerType::SEGMENT_METADATA:
        return "segment_metadata";
    case MemTrackerType::COLUMN_METADATA:
        return "column_metadata";
    case MemTrackerType::TABLET_SCHEMA:
        return "tablet_schema";
    case MemTrackerType::SEGMENT_ZONEMAP:
        return "segment_zonemap";
    case MemTrackerType::SHORT_KEY_INDEX:
        return "short_key_index";
    case MemTrackerType::COLUMN_ZONEMAP_INDEX:
        return "column_zonemap_index";
    case MemTrackerType::ORDINAL_INDEX:
        return "ordinal_index";
    case MemTrackerType::BITMAP_INDEX:
        return "bitmap_index";
    case MemTrackerType::BLOOM_FILTER_INDEX:
        return "bloom_filter_index";
    case MemTrackerType::PAGE_CACHE:
        return "page_cache";
    case MemTrackerType::JIT_CACHE:
        return "jit_cache";
    case MemTrackerType::UPDATE:
        return "update";
    case MemTrackerType::CHUNK_ALLOCATOR:
        return "chunk_allocator";
    case MemTrackerType::CLONE:
        return "clone";
    case MemTrackerType::DATACACHE:
        return "datacache";
    case MemTrackerType::POCO_CONNECTION_POOL:
        return "poco_connection_pool";
    case MemTrackerType::REPLICATION:
        return "replication";
    case MemTrackerType::ROWSET_UPDATE_STATE:
        return "rowset_update_state";
    case MemTrackerType::INDEX_CACHE:
        return "index_cache";
    case MemTrackerType::DEL_VEC_CACHE:
        return "del_vec_cache";
    case MemTrackerType::COMPACTION_STATE:
        return "compaction_state";
    case MemTrackerType::QUERY:
    case MemTrackerType::RESOURCE_GROUP:
    case MemTrackerType::RESOURCE_GROUP_BIG_QUERY:
    case MemTrackerType::COMPACTION_TASK:
    case MemTrackerType::SCHEMA_CHANGE_TASK:
        // Use user-defined label
        return "";
    default:
        return "";
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
    if (parent != nullptr) _parent->add_child_tracker(this);
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
    if (parent != nullptr) _parent->add_child_tracker(this);
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
    if (parent != nullptr) _parent->add_child_tracker(this);
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
