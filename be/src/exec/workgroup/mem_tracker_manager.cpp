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

    const double mem_limit_fraction = wg->mem_limit();
    const uint64_t memory_limit_bytes =
            static_cast<uint64_t>(GlobalEnv::GetInstance()->query_pool_mem_tracker()->limit() * mem_limit_fraction);

    // Frontend (FE) validation ensures that active resource groups (RGs) sharing
    // the same mem_pool also have the same mem_limit.
    if (_shared_mem_trackers.contains(wg->mem_pool())) {
        // We must handle an edge case:
        // 1. All RGs using a specific mem_pool are deleted.
        // 2. The shared tracker for that pool remains cached here.
        // 3. A new RG is created with the same mem_pool name but a different mem_limit.
        // Therefore, we must verify the cached tracker's limit matches the current RG's limit.
        if (auto& shared_mem_tracker = _shared_mem_trackers.at(wg->mem_pool());
            shared_mem_tracker->limit() == memory_limit_bytes) {
            return shared_mem_tracker;
        }
    }

    auto shared_mem_tracker =
            std::make_shared<MemTracker>(MemTrackerType::RESOURCE_GROUP_SHARED_MEMORY_POOL, memory_limit_bytes,
                                         wg->mem_pool(), GlobalEnv::GetInstance()->query_pool_mem_tracker());

    _shared_mem_trackers.insert_or_assign(wg->mem_pool(), shared_mem_tracker);
    return shared_mem_tracker;
}
} // namespace starrocks::workgroup