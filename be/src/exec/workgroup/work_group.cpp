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
          _type(type),
          _cpu_limit(cpu_limit),
          _memory_limit(memory_limit),
          _concurrency(concurrency) {}

WorkGroup::WorkGroup(const TWorkGroup& twg) : _name(twg.name), _id(twg.id) {
    if (twg.__isset.cpu_core_limit) {
        _cpu_limit = twg.cpu_core_limit;
    } else {
        _cpu_limit = -1;
    }
    if (twg.__isset.mem_limit) {
        _memory_limit = twg.mem_limit;
    } else {
        _memory_limit = -1;
    }
    if (twg.__isset.concurrency_limit) {
        _concurrency = twg.concurrency_limit;
    } else {
        _concurrency = -1;
    }
    if (twg.__isset.workgroup_type) {
        switch (twg.workgroup_type) {
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

double WorkGroup::get_cpu_expected_use_ratio() const {
    return static_cast<double>(_cpu_limit) / WorkGroupManager::instance()->get_sum_cpu_limit();
}

double WorkGroup::get_cpu_actual_use_ratio() const {
    int64_t sum_cpu_runtime_ns = WorkGroupManager::instance()->get_sum_cpu_runtime_ns();
    if (sum_cpu_runtime_ns == 0) {
        return 0;
    }
    return static_cast<double>(get_real_runtime_ns()) / sum_cpu_runtime_ns;
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
        _wg_io_queue.add(wg);
        _sum_cpu_limit += wg->get_cpu_limit();
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
        _sum_cpu_limit -= wg->get_cpu_limit();
        _workgroups.erase(wg_id);
        _wg_io_queue.remove(wg);
    }
}

WorkGroupPtr WorkGroupManager::pick_next_wg_for_io() {
    return _wg_io_queue.pick_next();
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