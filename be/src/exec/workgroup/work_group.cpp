// This file is licensed under the Elastic License 2.0. Copyright 2021-present StarRocks Limited.

#include "exec/workgroup/work_group.h"

#include "gen_cpp/internal_service.pb.h"
#include "glog/logging.h"
#include "runtime/exec_env.h"

namespace starrocks {
namespace workgroup {

WorkGroup::WorkGroup(const std::string& name, int64_t id, int64_t version, size_t cpu_limit, double memory_limit,
                     size_t concurrency, WorkGroupType type)
        : _name(name),
          _id(id),
          _version(version),
          _cpu_limit(cpu_limit),
          _memory_limit(memory_limit),
          _concurrency(concurrency),
          _type(type) {}

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
        _type = twg.workgroup_type;
    }
    if (twg.__isset.version) {
        _version = twg.version;
    }
}

TWorkGroup WorkGroup::to_thrift() const {
    TWorkGroup twg;
    twg.__set_id(_id);
    twg.__set_version(_version);
    return twg;
}
TWorkGroup WorkGroup::to_thrift_verbose() const {
    TWorkGroup twg;
    twg.__set_id(_id);
    twg.__set_name(_name);
    twg.__set_version(_version);
    twg.__set_workgroup_type(_type);
    std::string state = is_marked_del() ? "dead" : "alive";
    twg.__set_state(state);
    twg.__set_cpu_core_limit(_cpu_limit);
    twg.__set_mem_limit(_memory_limit);
    twg.__set_concurrency_limit(_concurrency);
    twg.__set_num_drivers(_acc_num_drivers);
    return twg;
}

void WorkGroup::init() {
    int64_t limit = ExecEnv::GetInstance()->query_pool_mem_tracker()->limit() * _memory_limit;
    _mem_tracker =
            std::make_shared<starrocks::MemTracker>(limit, _name, ExecEnv::GetInstance()->query_pool_mem_tracker());
    _driver_queue = std::make_unique<starrocks::pipeline::QuerySharedDriverQueueWithoutLock>();
}

double WorkGroup::get_cpu_expected_use_ratio() const {
    return static_cast<double>(_cpu_limit) / WorkGroupManager::instance()->get_sum_cpu_limit();
}

double WorkGroup::get_cpu_unadjusted_actual_use_ratio() const {
    int64_t sum_cpu_runtime_ns = WorkGroupManager::instance()->get_sum_unadjusted_cpu_runtime_ns();
    if (sum_cpu_runtime_ns == 0) {
        return 0;
    }
    return static_cast<double>(_unadjusted_real_runtime_ns) / sum_cpu_runtime_ns;
}

double WorkGroup::get_cpu_actual_use_ratio() const {
    int64_t sum_cpu_runtime_ns = WorkGroupManager::instance()->get_sum_cpu_runtime_ns();
    if (sum_cpu_runtime_ns == 0) {
        return 0;
    }
    return static_cast<double>(get_real_runtime_ns()) / sum_cpu_runtime_ns;
}

WorkGroupManager::WorkGroupManager()
        : _driver_dispatcher_owner_manager(std::make_unique<DispatcherOwnerManager>(
                  config::pipeline_exec_thread_pool_thread_num > 0 ? config::pipeline_exec_thread_pool_thread_num
                                                                   : std::thread::hardware_concurrency())),
          _io_dispatcher_owner_manager(std::make_unique<DispatcherOwnerManager>(
                  config::pipeline_scan_thread_pool_thread_num > 0 ? config::pipeline_scan_thread_pool_thread_num
                                                                   : std::thread::hardware_concurrency())) {}

WorkGroupManager::~WorkGroupManager() {}
void WorkGroupManager::destroy() {
    std::unique_lock write_lock(_mutex);

    _driver_dispatcher_owner_manager.reset(nullptr);
    _io_dispatcher_owner_manager.reset(nullptr);
    _workgroups.clear();
}

WorkGroupPtr WorkGroupManager::add_workgroup(const WorkGroupPtr& wg) {
    std::unique_lock write_lock(_mutex);
    auto unique_id = wg->unique_id();
    create_workgroup_unlocked(wg);
    if (_workgroup_versions.count(wg->id()) && _workgroup_versions[wg->id()] == wg->version()) {
        return _workgroups[unique_id];
    } else {
        return get_default_workgroup();
    }
}

WorkGroupPtr WorkGroupManager::get_default_workgroup() {
    std::shared_lock read_lock(_mutex);
    auto unique_id = WorkGroup::create_unique_id(WorkGroup::DEFAULT_VERSION, WorkGroup::DEFAULT_WG_ID);
    DCHECK(_workgroups.count(unique_id));
    return _workgroups[unique_id];
}

bool WorkGroup::try_offer_io_task(IoWorkGroupQueue::Task task) {
    _io_work_queue.emplace(std::move(task));
    return true;
}

IoWorkGroupQueue::Task WorkGroup::pick_io_task() {
    auto task = std::move(_io_work_queue.front());
    _io_work_queue.pop();
    return task;
}

// Should be call when read chunk_num from disk.
void WorkGroup::increase_chunk_num(int32_t chunk_num) {
    _cur_hold_total_chunk_num += chunk_num;
    _increase_chunk_num_period += chunk_num;
}

void WorkGroup::decrease_chunk_num(int32_t chunk_num) {
    _cur_hold_total_chunk_num -= chunk_num;
    _decrease_chunk_num_period += chunk_num;
}

double WorkGroup::get_expect_factor() const {
    return _expect_factor;
}

double WorkGroup::get_diff_factor() const {
    return _diff_factor;
}

double WorkGroup::get_select_factor() const {
    return _select_factor;
}

void WorkGroup::set_select_factor(double value) {
    _select_factor = value;
}

void WorkGroup::update_select_factor(double value) {
    _select_factor += value;
}

double WorkGroup::get_cur_select_factor() const {
    return _cur_select_factor;
}

void WorkGroup::update_cur_select_factor(double value) {
    _cur_select_factor += value;
}

void WorkGroup::estimate_trend_factor_period() {
    // As an example
    // During the execution period, This WorkGroup actually consumes 70 chunks and uses 80% of the cpu resources
    // But in reality, the WorkGroup's cpu resources are limited to 70% of
    // At the same time, the WorkGroup reads 200 chunks from io_threads
    // Therefore, we calculate the correlation factor as follows
    // decrease_factor = 70 / 0.8 * (0.7 / 0.8)
    // increase_factor = 200 / 0.7
    // _expect_factor indicates the percentage of io resources we expect the WorkGroup to use so that it meets the 70% cpu limit
    // so _expect_factor = decrease_factor / increase_factor * 70

    double decrease_factor = _decrease_chunk_num_period / get_cpu_actual_use_ratio() * get_cpu_expected_use_ratio() /
                             get_cpu_actual_use_ratio();
    double increase_factor = _increase_chunk_num_period / get_cpu_actual_use_ratio();

    _expect_factor = decrease_factor / increase_factor * get_cpu_expected_use_ratio();

    // diff_factor indicates the difference between the actual percentage of io resources used and the limited cpu percentage
    // If it is negative, it means that there are not enough resources and more resources are needed
    // If it is positive, it means that its resources are sufficient and can be reduced by a fraction
    //_diff_factor = get_cpu_expected_use_ratio() - _expect_factor;
    _diff_factor = _select_factor - _expect_factor;

    _decrease_chunk_num_period = 1;
    _increase_chunk_num_period = 1;
}

size_t WorkGroup::io_task_queue_size() {
    return _io_work_queue.size();
}

void IoWorkGroupQueue::_maybe_adjust_weight() {
    if (--_remaining_schedule_num_period > 0) {
        return;
    }

    int num_tasks = 0;
    // calculate all wg factors
    for (auto& wg : _ready_wgs) {
        wg->estimate_trend_factor_period();
        num_tasks += wg->io_task_queue_size();
    }

    _remaining_schedule_num_period = std::min(_max_schedule_num_period, num_tasks);

    // negative_total_diff_factor Accumulate All Under-resourced WorkGroup
    // positive_total_diff_factor Cumulative All Resource Excess WorkGroup
    double positive_total_diff_factor = 0.0;
    double negative_total_diff_factor = 0.0;
    for (auto const& wg : _ready_wgs) {
        if (wg->get_diff_factor() > 0) {
            positive_total_diff_factor += wg->get_diff_factor();
        } else {
            negative_total_diff_factor += wg->get_diff_factor();
        }
    }

    // If positive_total_diff_factor <= 0, it This means that all WorkGs have no excess resources
    // So we don't need to adjust it and keep the original limit
    if (positive_total_diff_factor <= 0) {
        for (auto& wg : _ready_wgs) {
            wg->set_select_factor(wg->get_cpu_expected_use_ratio());
        }
        return;
    }

    // As an example
    // There are two WorkGroups A and B
    // A _expect_factor : 0.18757812499999998, and _cpu_expect_use_ratio : 0.7, _diff_factor : 0.512421875
    // B _expect_factor : 0.6749999999999999, and _cpu_expect_use_ratio : 0.3, _diff_factor :  -0.37499999999999994
    // so 0.18757812499999998 + 0.6749999999999999 < 1.0, it mean resource is enough, and available is  -0.37499999999999994 + 0.512421875

    if (positive_total_diff_factor + negative_total_diff_factor > 0) {
        // if positive_total_diff_factor + negative_total_diff_factor > 0
        // This means that the resources are sufficient
        // So we can reduce the proportion of resources in the WorkGroup that are over-resourced
        // Then increase the proportion of resources for those WorkGs that are under-resourced
        for (auto& wg : _ready_wgs) {
            if (wg->get_diff_factor() < 0) {
                wg->update_select_factor(0 - negative_total_diff_factor * wg->get_diff_factor() /
                                                     negative_total_diff_factor);
            } else if (wg->get_diff_factor() > 0) {
                wg->update_select_factor(negative_total_diff_factor * wg->get_diff_factor() /
                                         positive_total_diff_factor);
            }
        }
    } else {
        // if positive_total_diff_factor + negative_total_diff_factor <= 0
        // This means that there are not enough resources, but some WorkGs are still over-resourced
        // So we can reduce the proportion of resources in the WorkGroup that are over-resourced
        // Then increase the proportion of resources for those WorkGs that are under-resourced
        for (auto& wg : _ready_wgs) {
            if (wg->get_diff_factor() < 0) {
                wg->update_select_factor(positive_total_diff_factor * wg->get_diff_factor() /
                                         negative_total_diff_factor);
            } else if (wg->get_diff_factor() > 0) {
                wg->update_select_factor(0 - positive_total_diff_factor * wg->get_diff_factor() /
                                                     positive_total_diff_factor);
            }
        }
    }
}

WorkGroupPtr IoWorkGroupQueue::_select_next_wg(int dispatcher_id) {
    auto owner_wgs = workgroup::WorkGroupManager::instance()->get_owners_of_io_dispatcher(dispatcher_id);

    WorkGroupPtr max_owner_wg = nullptr;
    WorkGroupPtr max_other_wg = nullptr;
    double total = 0;
    for (auto wg : _ready_wgs) {
        wg->update_cur_select_factor(wg->get_select_factor());
        total += wg->get_select_factor();

        if (owner_wgs->find(wg) != owner_wgs->end()) {
            if (max_owner_wg == nullptr || wg->get_cur_select_factor() > max_owner_wg->get_cur_select_factor()) {
                max_owner_wg = wg;
            }
        } else if (max_other_wg == nullptr || wg->get_cur_select_factor() > max_other_wg->get_cur_select_factor()) {
            max_other_wg = wg;
        }
    }

    // Try to take task from any owner workgroup first.
    if (max_owner_wg != nullptr) {
        max_owner_wg->update_cur_select_factor(0 - total);
        return max_owner_wg;
    }

    // All the owner workgroups don't have ready tasks, so select the other workgroup.
    max_other_wg->update_cur_select_factor(0 - total);
    return max_other_wg;
}

StatusOr<IoWorkGroupQueue::Task> IoWorkGroupQueue::pick_next_task(int dispatcher_id) {
    std::unique_lock<std::mutex> lock(_global_io_mutex);

    if (_is_closed) {
        return Status::Cancelled("Shutdown");
    }
    while (_ready_wgs.empty()) {
        _cv.wait(lock);
        if (_is_closed) {
            return Status::Cancelled("Shutdown");
        }
    }

    _maybe_adjust_weight();

    WorkGroupPtr wg = _select_next_wg(dispatcher_id);

    if (wg->io_task_queue_size() == 1) {
        _ready_wgs.erase(wg);
    }

    _total_task_num--;

    return wg->pick_io_task();
}

void WorkGroupManager::apply(const std::vector<TWorkGroupOp>& ops) {
    std::unique_lock write_lock(_mutex);

    size_t original_num_workgroups = _workgroups.size();
    auto it = _workgroup_expired_versions.begin();
    // collect removable workgroups
    while (it != _workgroup_expired_versions.end()) {
        auto wg_it = _workgroups.find(*it);
        DCHECK(wg_it != _workgroups.end());
        if (wg_it->second->is_removable()) {
            _wg_io_queue.remove(wg_it->second);
            _workgroups.erase(wg_it);
            _sum_cpu_limit -= wg_it->second->get_cpu_limit();
            _workgroup_expired_versions.erase(it++);
        } else {
            ++it;
        }
    }
    if (original_num_workgroups != _workgroups.size()) {
        reassign_dispatcher_to_wgs();
    }

    for (const auto& op : ops) {
        auto op_type = op.op_type;
        auto wg = std::make_shared<WorkGroup>(op.workgroup);
        switch (op_type) {
        case TWorkGroupOpType::WORKGROUP_OP_CREATE:
            create_workgroup_unlocked(wg);
            break;
        case TWorkGroupOpType::WORKGROUP_OP_ALTER:
            alter_workgroup_unlocked(wg);
            break;
        case TWorkGroupOpType::WORKGROUP_OP_DELETE:
            delete_workgroup_unlocked(wg);
            break;
        }
    }
}

void WorkGroupManager::create_workgroup_unlocked(const WorkGroupPtr& wg) {
    auto unique_id = wg->unique_id();
    // only current version not exists or current version is older than wg->version(), then create a new WorkGroup
    if (_workgroup_versions.count(wg->id()) && _workgroup_versions[wg->id()] >= wg->version()) {
        return;
    }
    wg->init();
    _workgroups[unique_id] = wg;
    _sum_cpu_limit += wg->get_cpu_limit();
    reassign_dispatcher_to_wgs();

    // old version exists, so mark the stale version delete
    if (_workgroup_versions.count(wg->id())) {
        auto stale_version = _workgroup_versions[wg->id()];
        DCHECK(stale_version < wg->version());
        auto old_unique_id = WorkGroup::create_unique_id(wg->id(), stale_version);
        if (_workgroups.count(old_unique_id)) {
            _workgroups[old_unique_id]->mark_del();
            _workgroup_expired_versions.push_back(old_unique_id);
        }
    }
    // install new version
    _workgroup_versions[wg->id()] = wg->version();
    _wg_io_queue.add(wg);
}

void WorkGroupManager::alter_workgroup_unlocked(const WorkGroupPtr& wg) {
    create_workgroup_unlocked(wg);
}

void WorkGroupManager::delete_workgroup_unlocked(const WorkGroupPtr& wg) {
    auto unique_id = wg->unique_id();
    auto wg_it = _workgroups.find(unique_id);
    if (wg_it != _workgroups.end()) {
        wg_it->second->mark_del();
    }
}

std::vector<TWorkGroup> WorkGroupManager::list_workgroups() {
    std::shared_lock read_lock(_mutex);
    std::vector<TWorkGroup> alive_workgroups;
    for (auto it = _workgroups.begin(); it != _workgroups.end(); ++it) {
        const auto& wg = it->second;
        if (!wg->is_marked_del() && wg->version() != WorkGroup::DEFAULT_VERSION) {
            alive_workgroups.push_back(wg->to_thrift());
        }
    }
    return alive_workgroups;
}
std::vector<TWorkGroup> WorkGroupManager::list_all_workgroups() {
    std::vector<TWorkGroup> workgroups;
    {
        std::shared_lock read_lock(_mutex);
        workgroups.reserve(_workgroups.size());
        for (auto it = _workgroups.begin(); it != _workgroups.end(); ++it) {
            const auto& wg = it->second;
            auto twg = wg->to_thrift_verbose();
            workgroups.push_back(twg);
        }
    }
    std::sort(workgroups.begin(), workgroups.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.name < rhs.name; });
    return workgroups;
}

bool IoWorkGroupQueue::try_offer_io_task(WorkGroupPtr wg, Task task) {
    std::lock_guard<std::mutex> lock(_global_io_mutex);

    wg->try_offer_io_task(std::move(task));
    if (_ready_wgs.find(wg) == _ready_wgs.end()) {
        _ready_wgs.emplace(wg);
    }

    _total_task_num++;
    _cv.notify_one();
    return true;
}

void IoWorkGroupQueue::close() {
    std::lock_guard<std::mutex> lock(_global_io_mutex);

    if (_is_closed) {
        return;
    }

    _is_closed = true;
    _cv.notify_all();
}

void WorkGroupManager::close() {
    _wg_io_queue.close();
}

StatusOr<IoWorkGroupQueue::Task> WorkGroupManager::pick_next_task_for_io(int dispatcher_id) {
    return _wg_io_queue.pick_next_task(dispatcher_id);
}

bool WorkGroupManager::try_offer_io_task(WorkGroupPtr wg, IoWorkGroupQueue::Task task) {
    return _wg_io_queue.try_offer_io_task(wg, task);
}

void WorkGroupManager::reassign_dispatcher_to_wgs() {
    _driver_dispatcher_owner_manager->reassign_to_wgs(_workgroups, _sum_cpu_limit);
    _io_dispatcher_owner_manager->reassign_to_wgs(_workgroups, _sum_cpu_limit);
}

std::shared_ptr<WorkGroupPtrSet> WorkGroupManager::get_owners_of_driver_dispatcher(int dispatcher_id) {
    return _driver_dispatcher_owner_manager->get_owners(dispatcher_id);
}

bool WorkGroupManager::should_yield_driver_dispatcher(int dispatcher_id, WorkGroupPtr running_wg) {
    return _driver_dispatcher_owner_manager->should_yield(dispatcher_id, std::move(running_wg));
}

std::shared_ptr<WorkGroupPtrSet> WorkGroupManager::get_owners_of_io_dispatcher(int dispatcher_id) {
    return _io_dispatcher_owner_manager->get_owners(dispatcher_id);
}

bool WorkGroupManager::should_yield_io_dispatcher(int dispatcher_id, WorkGroupPtr running_wg) {
    return _io_dispatcher_owner_manager->should_yield(dispatcher_id, std::move(running_wg));
}

DefaultWorkGroupInitialization::DefaultWorkGroupInitialization() {
    auto default_wg = std::make_shared<WorkGroup>("default_wg", WorkGroup::DEFAULT_WG_ID, WorkGroup::DEFAULT_VERSION, 1,
                                                  0.5, 10, WorkGroupType::WG_DEFAULT);
    auto wg1 = std::make_shared<WorkGroup>("wg1", 1, WorkGroup::DEFAULT_VERSION, 2, 0.5, 10, WorkGroupType::WG_NORMAL);
    auto wg2 = std::make_shared<WorkGroup>("wg2", 2, WorkGroup::DEFAULT_VERSION, 4, 0.5, 10, WorkGroupType::WG_NORMAL);
    WorkGroupManager::instance()->add_workgroup(default_wg);
    WorkGroupManager::instance()->add_workgroup(wg1);
    WorkGroupManager::instance()->add_workgroup(wg2);

    default_wg->set_select_factor(default_wg->get_cpu_expected_use_ratio());
    wg1->set_select_factor(wg1->get_cpu_expected_use_ratio());
    wg2->set_select_factor(wg2->get_cpu_expected_use_ratio());
}

DispatcherOwnerManager::DispatcherOwnerManager(int num_total_dispatchers)
        : _num_total_dispatchers(num_total_dispatchers) {
    for (int i = 0; i < 2; ++i) {
        _dispatcher_id2owner_wgs[i] = std::vector<std::shared_ptr<WorkGroupPtrSet>>(_num_total_dispatchers);
    }
}

void DispatcherOwnerManager::reassign_to_wgs(const std::unordered_map<int128_t, WorkGroupPtr>& workgroups,
                                             int sum_cpu_limit) {
    auto& dispatcher_id2owner_wgs = _dispatcher_id2owner_wgs[(_index + 1) % 2];
    for (auto& owner_wgs : dispatcher_id2owner_wgs) {
        owner_wgs = std::make_shared<WorkGroupPtrSet>();
    }

    int dispatcher_id = 0;
    for (const auto& [_, wg] : workgroups) {
        int num_dispatchers = std::max(1, _num_total_dispatchers * int(wg->get_cpu_limit()) / sum_cpu_limit);
        for (int i = 0; i < num_dispatchers; ++i) {
            dispatcher_id2owner_wgs[(dispatcher_id++) % _num_total_dispatchers]->emplace(wg);
        }
    }

    _index++;
}

bool DispatcherOwnerManager::should_yield(int dispatcher_id, const WorkGroupPtr& running_wg) const {
    if (dispatcher_id >= _num_total_dispatchers) {
        return false;
    }

    auto wgs = _dispatcher_id2owner_wgs[_index % 2][dispatcher_id];
    // This dispatcher doesn't belong to any workgroup, or running_wg is the owner of it.
    if (wgs == nullptr || wgs->empty() || wgs->find(running_wg) != wgs->end()) {
        return false;
    }

    // Any owner of this dispatcher has running drivers.
    for (const auto& wg : *wgs) {
        if (wg->num_drivers() > 0) {
            return true;
        }
    }

    return false;
}

} // namespace workgroup
} // namespace starrocks
