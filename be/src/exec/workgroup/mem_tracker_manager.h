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
struct MemTrackerManager {
public:
    using MemTrackerPtr = std::shared_ptr<MemTracker>;
    MemTrackerPtr get_parent_mem_tracker(const WorkGroupPtr& wg);

private:
    std::unordered_map<std::string, MemTrackerPtr> _shared_mem_trackers{};
};
} // namespace starrocks::workgroup
