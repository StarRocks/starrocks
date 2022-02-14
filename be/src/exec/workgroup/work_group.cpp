// This file is licensed under the Elastic License 2.0. Copyright 2021-present StarRocks Limited.

#include "exec/workgroup/work_group.h"

#include "gen_cpp/internal_service.pb.h"
#include "glog/logging.h"
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

bool WorkGroup::try_offer_io_task(const PriorityThreadPool::Task& task) {
    _io_work_queue.emplace(std::move(task));
    return true;
}

PriorityThreadPool::Task WorkGroup::pick_io_task() {
    PriorityThreadPool::Task task = std::move(_io_work_queue.front());
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

WorkGroupPtr IoWorkGroupQueue::_select_next_wg() {
    WorkGroupPtr max_wg = nullptr;
    double total = 0;
    for (auto wg : _ready_wgs) {
        wg->update_cur_select_factor(wg->get_select_factor());
        total += wg->get_select_factor();
        if (max_wg == nullptr || wg->get_cur_select_factor() > max_wg->get_cur_select_factor()) {
            max_wg = wg;
        }
    }
    max_wg->update_cur_select_factor(0 - total);
    return max_wg;
}

StatusOr<PriorityThreadPool::Task> IoWorkGroupQueue::pick_next_task() {
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
    WorkGroupPtr wg = _select_next_wg();
    if (wg->io_task_queue_size() == 1) {
        _ready_wgs.erase(wg);
    }

    _total_task_num--;

    return wg->pick_io_task();
}

bool IoWorkGroupQueue::try_offer_io_task(WorkGroupPtr wg, const PriorityThreadPool::Task& task) {
    std::lock_guard<std::mutex> lock(_global_io_mutex);

    wg->try_offer_io_task(task);
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

void WorkGroupManager::close() {
    _wg_io_queue.close();
}

StatusOr<PriorityThreadPool::Task> WorkGroupManager::pick_next_task_for_io() {
    return _wg_io_queue.pick_next_task();
}

bool WorkGroupManager::try_offer_io_task(WorkGroupPtr wg, const PriorityThreadPool::Task& task) {
    return _wg_io_queue.try_offer_io_task(wg, task);
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

    default_wg->set_select_factor(default_wg->get_cpu_expected_use_ratio());
    wg1->set_select_factor(wg1->get_cpu_expected_use_ratio());
    wg2->set_select_factor(wg2->get_cpu_expected_use_ratio());
}

} // namespace workgroup
} // namespace starrocks