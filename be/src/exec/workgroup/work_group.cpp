// This file is licensed under the Elastic License 2.0. Copyright 2021-present StarRocks Limited.

#include "exec/workgroup/work_group.h"

#include "gen_cpp/internal_service.pb.h"
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
          _type(type) {}

WorkGroup::WorkGroup(const TWorkGroup& twg) : _name(twg.name), _id(twg.id) {
    if (twg.__isset.cpu_limit) {
        _cpu_limit = twg.cpu_limit;
    } else {
        _cpu_limit = -1;
    }
    if (twg.__isset.memory_limit) {
        _memory_limit = twg.memory_limit;
    } else {
        _memory_limit = -1;
    }
    if (twg.__isset.concurrency) {
        _concurrency = twg.concurrency;
    } else {
        _concurrency = -1;
    }
    if (twg.__isset.type) {
        switch (twg.type) {
        case TWorkGroupType::WG_NORMAL:
            _type = WG_NORMAL;
            break;
        case TWorkGroupType::WG_DEFAULT:
            _type = WG_DEFAULT;
            break;
        case TWorkGroupType::WG_REALTIME:
            _type = WG_REALTIME;
            break;
        }
    } else {
        _type = WG_NORMAL;
    }
}

void WorkGroup::init() {
    _mem_tracker = std::make_shared<starrocks::MemTracker>(_memory_limit, _name,
                                                           ExecEnv::GetInstance()->query_pool_mem_tracker());
    _driver_queue = std::make_unique<starrocks::pipeline::QuerySharedDriverQueue>();
}

WorkGroupManager::WorkGroupManager() {}
WorkGroupManager::~WorkGroupManager() {}
void WorkGroupManager::destroy() {
    std::lock_guard<std::mutex> lock(_mutex);
    this->_workgroups.clear();
}

WorkGroupPtr WorkGroupManager::add_workgroup(const WorkGroupPtr& wg) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (!_workgroups.count(wg->id())) {
        _workgroups[wg->id()] = wg;
        _wg_cpu_queue.add(wg);
        _wg_io_queue.add(wg);
        return wg;
    } else {
        return _workgroups[wg->id()];
    }
}

WorkGroupPtr WorkGroupManager::get_default_workgroup() {
    std::lock_guard<std::mutex> lock(_mutex);
    DCHECK(_workgroups.count(WorkGroup::DEFAULT_WG_ID));
    return _workgroups[WorkGroup::DEFAULT_WG_ID];
}

void WorkGroupManager::remove_workgroup(int wg_id) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (_workgroups.count(wg_id)) {
        auto wg = std::move(_workgroups[wg_id]);
        _workgroups.erase(wg_id);
        _wg_cpu_queue.remove(wg);
        _wg_io_queue.remove(wg);
    }
}

WorkGroupPtr WorkGroupManager::pick_next_wg_for_cpu() {
    return _wg_cpu_queue.pick_next();
}

WorkGroupPtr WorkGroupManager::pick_next_wg_for_io() {
    return _wg_io_queue.pick_next();
}

WorkGroupQueue& WorkGroupManager::get_cpu_queue() {
    return _wg_cpu_queue;
}

WorkGroupQueue& WorkGroupManager::get_io_queue() {
    return _wg_io_queue;
}

DefaultWorkGroupInitialization::DefaultWorkGroupInitialization() {
    auto default_wg = std::make_shared<WorkGroup>("default_wg", 0, 1, 20L * (1L << 30), 10, WorkGroupType::WG_DEFAULT);
    default_wg->init();
    auto wg1 = std::make_shared<WorkGroup>("wg1", 1, 2, 10L * (1L << 30), 10, WorkGroupType::WG_NORMAL);
    wg1->init();
    auto wg2 = std::make_shared<WorkGroup>("wg2", 2, 4, 30L * (1L << 30), 10, WorkGroupType::WG_NORMAL);
    wg2->init();
    WorkGroupManager::instance()->add_workgroup(default_wg);
    WorkGroupManager::instance()->add_workgroup(wg1);
    WorkGroupManager::instance()->add_workgroup(wg2);
}

} // namespace workgroup
} // namespace starrocks