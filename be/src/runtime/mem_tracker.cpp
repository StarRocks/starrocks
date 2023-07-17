// This file is made available under Elastic License 2.0.
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

#include "gutil/strings/substitute.h"
#include "util/debug_util.h"
#include "util/mem_info.h"
#include "util/pretty_printer.h"
#include "util/stack_util.h"
#include "util/starrocks_metrics.h"
#include "util/uid_util.h"

namespace starrocks {

const std::string MemTracker::COUNTER_NAME = "PeakMemoryUsage";

MemTracker::MemTracker(int64_t byte_limit, std::string label, MemTracker* parent, bool auto_unregister)
        : _limit(byte_limit),
          _label(std::move(label)),
          _parent(parent),
          _consumption(&_local_counter),
          _local_counter(TUnit::BYTES),
          _auto_unregister(auto_unregister) {
    if (parent != nullptr) _parent->add_child_tracker(this);
    Init();
}

MemTracker::MemTracker(Type type, int64_t byte_limit, std::string label, MemTracker* parent, bool auto_unregister)
        : _type(type),
          _limit(byte_limit),
          _label(std::move(label)),
          _parent(parent),
          _consumption(&_local_counter),
          _local_counter(TUnit::BYTES),
          _auto_unregister(auto_unregister) {
    if (parent != nullptr) _parent->add_child_tracker(this);
    Init();
}

MemTracker::MemTracker(RuntimeProfile* profile, int64_t byte_limit, std::string label, MemTracker* parent,
                       bool auto_unregister)
        : _type(NO_SET),
          _limit(byte_limit),
          _label(std::move(label)),
          _parent(parent),
          _consumption(profile->AddHighWaterMarkCounter(COUNTER_NAME, TUnit::BYTES)),
          _local_counter(TUnit::BYTES),
          _auto_unregister(auto_unregister) {
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

    if (_auto_unregister && parent()) {
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

std::string MemTracker::err_msg(const std::string& msg) const {
    std::stringstream str;
    str << "Memory of " << label() << " exceed limit. " << msg << " ";
    str << "Used: " << consumption() << ", Limit: " << limit() << ". ";
    switch (type()) {
    case MemTracker::NO_SET:
        break;
    case MemTracker::QUERY:
        str << "Mem usage has exceed the limit of single query, You can change the limit by "
               "set session variable exec_mem_limit or query_mem_limit.";
        break;
    case MemTracker::PROCESS:
        str << "Mem usage has exceed the limit of BE";
        break;
    case MemTracker::QUERY_POOL:
        str << "Mem usage has exceed the limit of query pool";
        break;
    case MemTracker::LOAD:
        str << "Mem usage has exceed the limit of load";
        break;
    case MemTracker::CONSISTENCY:
        str << "Mem usage has exceed the limit of consistency";
        break;
    case MemTracker::SCHEMA_CHANGE_TASK:
        str << "You can change the limit by modify BE config [memory_limitation_per_thread_for_schema_change]";
        break;
    case MemTracker::RESOURCE_GROUP:
        // TODO: make default_wg configuable.
        if (label() == "default_wg") {
            str << "Mem usage has exceed the limit of query pool";
        } else {
            str << "Mem usage has exceed the limit of the resource group [" << label() << "]. "
                << "You can change the limit by modifying [mem_limit] of this group";
        }
        break;
    case MemTracker::RESOURCE_GROUP_BIG_QUERY:
        str << "Mem usage has exceed the big query limit of the resource group [" << label() << "]. "
            << "You can change the limit by modifying [big_query_mem_limit] of this group";
        break;
    default:
        break;
    }
    return str.str();
}

} // end namespace starrocks
