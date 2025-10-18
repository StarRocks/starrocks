#include "mem_tracker_manager.h"

#include <memory>

#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "work_group.h"

namespace starrocks::workgroup {
MemTrackerManager::MemTrackerPtr MemTrackerManager::get_parent_mem_tracker(const WorkGroupPtr& wg) {
    if (WorkGroup::DEFAULT_MEM_POOL == wg->mem_pool()) {
        return GlobalEnv::GetInstance()->query_pool_mem_tracker_shared();
    }
    // We validate in frontend that resource groups with the same mem_pool will also have the specify the same mem_limit.
    if (_shared_mem_trackers.contains(wg->mem_pool())) {
        return _shared_mem_trackers.at(wg->mem_pool());
    }
    const double mem_limit_fraction = wg->mem_limit();
    const uint64_t memory_limit_bytes =
            static_cast<uint64_t>(GlobalEnv::GetInstance()->query_pool_mem_tracker()->limit() * mem_limit_fraction);
    auto shared_mem_tracker =
            std::make_shared<MemTracker>(MemTrackerType::RESOURCE_GROUP_SHARED_MEMORY_POOL, memory_limit_bytes,
                                         wg->mem_pool(), GlobalEnv::GetInstance()->query_pool_mem_tracker());

    const bool inserted = _shared_mem_trackers.insert(std::pair{wg->mem_pool(), shared_mem_tracker}).second;
    DCHECK(inserted);
    return shared_mem_tracker;
}
} // namespace starrocks::workgroup
