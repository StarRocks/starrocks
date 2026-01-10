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

#pragma once

#include <memory>

#include "runtime/mem_tracker.h"
#include "work_group_fwd.h"

namespace starrocks::workgroup {

using MemTrackerPtr = std::shared_ptr<MemTracker>;

struct MemTrackerInfo {
    MemTrackerPtr tracker;
    uint32_t child_count;
};

struct MemTrackerManager {
public:
    std::vector<std::string> list_mem_trackers() const;
    /**
    * Constructs and returns a shared_mem_tracker for the workgroup if one does not already exist.
    * Otherwise, returns the existing instance and increments the number of tracked workgroups by one.
    * This method must be called whenever a new workgroup is constructed.
    */
    [[nodiscard]] MemTrackerPtr register_workgroup(const WorkGroupPtr& wg);

    /**
    * Decrements the number of tracked workgroups of the given memory pool by one.
    * If all its tracked children are deregistered, the memory pool entry will be erased.
    * This method must be called whenever an existing workgroup is destructed.
    */
    void deregister_workgroup(const std::string& mem_pool);

private:
    std::unordered_map<std::string, MemTrackerInfo> _shared_mem_trackers{};
};
} // namespace starrocks::workgroup
