// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/workgroup/work_group.h"

#include "runtime/exec_env.h"

namespace starrocks {
namespace workgroup {
WorkGroup::WorkGroup(const std::string& name, int id, size_t cpu_limit, size_t memory_limit, size_t concurrency,
                     WorkGroupType type)
        : _name(name),
          _id(id),
          _cpu_limit(cpu_limit),
          _memory_limit(memory_limit),
          _concurrency(concurrency),
          _type(type) {
    _mem_tracker = std::make_shared<starrocks::MemTracker>(memory_limit, name,
                                                           ExecEnv::GetInstance()->query_pool_mem_tracker());
    _driver_queue = std::make_unique<starrocks::pipeline::QuerySharedDriverQueue>();
}
WorkGroupManager::WorkGroupManager() {}
WorkGroupManager::~WorkGroupManager() {}

void WorkGroupManager::add_workgroup(const WorkGroupPtr& wg) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (!_workgroups.count(wg->id())) {
        _workgroups[wg->id()] = wg;
        _wg_cpu_queue.push(wg);
        _wg_cpu_queue.push(wg);
    }
}

void WorkGroupManager::remove_workgroup(int wg_id) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (_workgroups.count(wg_id)) {
        auto wg = std::move(_workgroups[wg_id]);
        _workgroups.erase(wg_id);
        wg->mark_del();
    }
}

WorkGroupPtr WorkGroupManager::pick_next_wg_for_cpu() {
    std::lock_guard<std::mutex> lock(_mutex);
    while (!_wg_cpu_queue.empty()) {
        auto wg = std::move(_wg_cpu_queue.top());
        if (!wg->is_mark_del()) {
            return wg;
        }
    }
    return nullptr;
}

WorkGroupPtr WorkGroupManager::pick_next_wg_for_io() {
    std::lock_guard<std::mutex> lock(_mutex);
    while (!_wg_io_queue.empty()) {
        auto wg = std::move(_wg_io_queue.top());
        if (!wg->is_mark_del()) {
            return wg;
        }
    }
    return nullptr;
}

class DefaultWorkGroupInitialization {
public:
    DefaultWorkGroupInitialization() {
        auto default_wg = std::make_shared<WorkGroup>("default_wg", 1, 10, 10, 10, WorkGroupType::WG_DEFAULT);
        WorkGroupManager::instance()->add_workgroup(default_wg);
    }
} init;

} // namespace workgroup
} // namespace starrocks