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

#include "mem_tracker_manager.h"

#include <memory>

#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "work_group.h"

namespace starrocks::workgroup {

MemTrackerPtr MemTrackerManager::register_workgroup(const WorkGroupPtr& wg) {
    if (WorkGroup::DEFAULT_MEM_POOL == wg->mem_pool()) {
        return GlobalEnv::GetInstance()->query_pool_mem_tracker_shared();
    }

    const double mem_limit_fraction = wg->mem_limit();
    const int64_t memory_limit_bytes =
            static_cast<int64_t>(GlobalEnv::GetInstance()->query_pool_mem_tracker()->limit() * mem_limit_fraction);

    // Frontend (FE) validation ensures that active resource groups (RGs) sharing
    // the same mem_pool also have the same mem_limit.
    if (_shared_mem_trackers.contains(wg->mem_pool())) {
        // We must handle an edge case:
        // 1. All RGs using a specific mem_pool are marked for deletion.
        // 2. The shared tracker for that pool remains cached here.
        // 3. A new RG is created with the same mem_pool name but a different mem_limit.
        // Therefore, we must verify the cached tracker's limit matches the current RG's limit.
        // Otherwise, a new mem_pool with thew new mem_limit will be constructed, and expiring children are orphaned.
        if (MemTrackerInfo& tracker_info = _shared_mem_trackers.at(wg->mem_pool());
            tracker_info.tracker->limit() == memory_limit_bytes) {
            tracker_info.child_count++;
            return tracker_info.tracker;
        }
    }

    auto shared_mem_tracker =
            std::make_shared<MemTracker>(MemTrackerType::RESOURCE_GROUP_SHARED_MEMORY_POOL, memory_limit_bytes,
                                         wg->mem_pool(), GlobalEnv::GetInstance()->query_pool_mem_tracker());

    _shared_mem_trackers[wg->mem_pool()].tracker = shared_mem_tracker;
    _shared_mem_trackers[wg->mem_pool()].child_count++; // also handles orphaned children

    return shared_mem_tracker;
}

void MemTrackerManager::deregister_workgroup(const std::string& mem_pool) {
    if (WorkGroup::DEFAULT_MEM_POOL == mem_pool) {
        return;
    }

    if (_shared_mem_trackers.contains(mem_pool)) {
        MemTrackerInfo& tracker_info = _shared_mem_trackers.at(mem_pool);
        if (tracker_info.child_count == 1) {
            // The shared tracker will be erased if its last child is being deregistered
            _shared_mem_trackers.erase(mem_pool);
        } else {
            DCHECK(tracker_info.child_count > 1);
            tracker_info.child_count--;
        }
    }
}

std::vector<std::string> MemTrackerManager::list_mem_trackers() const {
    auto keys_view = std::ranges::views::keys(_shared_mem_trackers);
    std::vector mem_trackers(keys_view.begin(), keys_view.end());
    mem_trackers.push_back(WorkGroup::DEFAULT_MEM_POOL);
    return mem_trackers;
}
} // namespace starrocks::workgroup