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
