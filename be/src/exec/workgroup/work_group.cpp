// This file is licensed under the Elastic License 2.0. Copyright 2021-present StarRocks Limited.

#include "exec/workgroup/work_group.h"

#include "gen_cpp/internal_service.pb.h"
#include "runtime/exec_env.h"
#include "glog/logging.h"

namespace starrocks {
namespace workgroup {

WorkGroup::WorkGroup(const std::string& name, int id, size_t cpu_limit, size_t memory_limit, size_t concurrency,
                     WorkGroupType type)
        : _name(name),
          _id(id),
          _cpu_limit(cpu_limit),
          _memory_limit(memory_limit),
          _concurrency(concurrency),
          _type(type),
          _io_work_queue(1024) {
    _expect_factor = _cpu_expect_use_ratio;
    _select_factor = _expect_factor;
    _cur_select_factor = _select_factor;
}

WorkGroup::WorkGroup(const TWorkGroup& twg) : _name(twg.name), _id(twg.id), _io_work_queue(1024) {
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

double WorkGroup::get_cpu_actual_use_ratio() const {
    int64_t sum_cpu_runtime_ns = WorkGroupManager::instance()->get_sum_cpu_runtime_ns();
    if (sum_cpu_runtime_ns == 0) {
        return 0;
    }
    return static_cast<double>(get_real_runtime_ns()) / sum_cpu_runtime_ns;
}

WorkGroupManager::WorkGroupManager() : _schedule_num_period(1024) {
    _cur_schedule_num = 0;
    _cur_wait_run_wgs.resize(_schedule_num_period, nullptr);
}
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

bool WorkGroup::try_offer_io_task(const PriorityThreadPool::Task& task) {
    return _io_work_queue.try_put(task);
}

void WorkGroup::pick_and_run_io_task() {
    PriorityThreadPool::Task task;
    if (_io_work_queue.blocking_get(&task)) {
        task.work_function();
    }
}

// shuold be call when read chunk_num from disk
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
    // maybe should add a guard

    // Exapmle
    // during a schedule period, actually this WorkGroup consume 70 chunk and use cpu resource percent 80
    // but this WorkGroup cpu limit to percent 80
    // meanwhile, WorkGroup read 200 chunk from io_threads, so we calculate some factor bellow
    // decrease_factor = 70 / 0.8 * (0.7 / 0.8)
    // increase_factor = 200 / 0.7
    // _expect_factor mean we except io resource ratio of this WorkGroup to suit to 70 percent cpu limit
    // so _expect_factor = decrease_factor / increase_factor * 70

    double decrease_factor =
            _decrease_chunk_num_period / get_cpu_actual_use_ratio() * get_cpu_expected_use_ratio() / get_cpu_actual_use_ratio();
    double increase_factor = _increase_chunk_num_period / get_cpu_actual_use_ratio();

    _expect_factor = decrease_factor / increase_factor * get_cpu_expected_use_ratio();

    // diff_factor mean diff between actually need io resource percent and limit percent of cpu
    // if it is nagative, it mean resouse less and need more
    // if it is positive, it mean resouse is enough and can be reduce
    _diff_factor = get_cpu_expected_use_ratio() - _expect_factor;

    // just for debug
    LOG(WARNING) << "estimate_trend_factor_period: " << name() 
                 << " decrease_chunk_num_period: " << _decrease_chunk_num_period
                 << " get_cpu_actual_use_ratio: " << get_cpu_actual_use_ratio()
                 << " get_cpu_expected_use_ratio: " << get_cpu_expected_use_ratio()
                 << " increase_chunk_num_period: " << _increase_chunk_num_period
                 << " expect_factor: " << _expect_factor
                 << " decrease_factor: " << decrease_factor
                 << " increase_factor: " << increase_factor
                 << " diff_factor: " << _diff_factor;

    // it only use for this period to calculate factors
    // over calculate, it must be reset to zero for next period
    _decrease_chunk_num_period = 1;
    _increase_chunk_num_period = 1;
}

size_t WorkGroup::io_task_queue_size() {
    return _io_work_queue.get_size();
}

void WorkGroupManager::adjust_weight_if_need() {
    if (--_cur_schedule_num > 0) {
        return;
    }

    bool expect = false;
    if (!_is_scheduled.compare_exchange_strong(expect, true)) {
        // maybe has other thread do it
        return;
    }

    // lock 
    _io_wgs.clear();
    for (const auto& wg : _ready_wgs) {
       _io_wgs.push_back(wg); 
    }

    // calculate all wg factors
    for (auto const& wg : _io_wgs) {
        wg->estimate_trend_factor_period();
    }

    // positive_total_diff_factor count all workgroup which is short of io resource
    // negative_total_diff_factor count all workgroup which over pay in io resource
    double positive_total_diff_factor = 0.0;
    double negative_total_diff_factor = 0.0;
    for (auto const& wg : _io_wgs) {
        if (wg->get_diff_factor() > 0) {
            positive_total_diff_factor += wg->get_diff_factor();
        } else {
            negative_total_diff_factor += wg->get_diff_factor();
        }
    }

    if (_schedule_num_period > _total_task_num) {
        _cur_schedule_num_period = _total_task_num;
    } else {
        _cur_schedule_num_period = _schedule_num_period;
    }

    // just for debug 
    LOG(WARNING) << "positive_total_diff_factor: " << positive_total_diff_factor
                 << " negative_total_diff_factor: " << negative_total_diff_factor;

    // If positive_total_diff_factor <= 0, it mean not exist workgroup which over pay io resource
    // so it don't need adjust, just keep
    if (positive_total_diff_factor <= 0) {
        for (auto const& wg : _io_wgs) {
            wg->set_select_factor(wg->get_cpu_expected_use_ratio());
        }
        schedule_io_task();
        _cur_schedule_num = _cur_schedule_num_period ;
        _is_scheduled.store(false, std::memory_order_release);
        return;
    }

    // Example:
    // there is two workgroup A and B
    // A _expect_factor : 0.18757812499999998, and _cpu_expect_use_ratio : 0.7, _diff_factor : 0.512421875
    // B _expect_factor : 0.6749999999999999, and _cpu_expect_use_ratio : 0.3, _diff_factor :  -0.37499999999999994
    // so 0.18757812499999998 + 0.6749999999999999 < 1.0, it mean resource is enough, and available is  -0.37499999999999994 + 0.512421875

    if (positive_total_diff_factor + negative_total_diff_factor > 0) {
        // if positive_total_diff_factor + negative_total_diff_factor > 0
        // it mean available resource more than short of resource
        // so we just reduce resource of the workgroup which it over pay by ratio
        // and add resource of the workgroup which it short of
        for (auto const& wg : _io_wgs) {
            if (wg->get_diff_factor() < 0) {
                wg->update_select_factor(0 - negative_total_diff_factor * wg->get_diff_factor() /
                                                     negative_total_diff_factor);
            } else if (wg->get_diff_factor() > 0) {
                wg->update_select_factor(0 - negative_total_diff_factor * wg->get_diff_factor() /
                                                     positive_total_diff_factor);
            }
        }
    } else {
        // if positive_total_diff_factor + negative_total_diff_factor <= 0
        // it mean available resource less than short of resource, but exist some workgroup which over pay
        // so we just reduce resource of the workgroup which it over pay by ratio
        // and add resource of the workgroup which it short of
        for (auto const& wg : _io_wgs) {
            if (wg->get_diff_factor() < 0) {
                wg->update_select_factor(positive_total_diff_factor * wg->get_diff_factor() /
                                         negative_total_diff_factor);
            } else if (wg->get_diff_factor() > 0) {
                wg->update_select_factor(0 - positive_total_diff_factor * wg->get_diff_factor() /
                                                     positive_total_diff_factor);
            }
        }
    }
    schedule_io_task();
    _cur_schedule_num = _cur_schedule_num_period;
    _is_scheduled.store(false, std::memory_order_release);
}

WorkGroup* WorkGroupManager::get_next_wg() {
    size_t index = _cur_index;
    while (!_cur_index.compare_exchange_strong(index, index + 1)) {
        index = _cur_index;
        if (index >= _cur_schedule_num_period) {
            break;
        }
    }

    if (index < _cur_schedule_num_period) {
        return _cur_wait_run_wgs[index];
    }

    return nullptr;
}

void WorkGroupManager::schedule_io_task() {
    // during schedule period, we pre calculate next period wg for speed up
    for (size_t i = 0; i < _cur_schedule_num_period; i++) {
        size_t idx = get_next_wg_index();
        _cur_wait_run_wgs[i] = _io_wgs[idx];
    }
    _cur_index = 0;
}

size_t WorkGroupManager::get_next_wg_index() {
    // we use Weighted round robin
    size_t index = -1;
    double total = 0;
    for (int i = 0; i < _io_wgs.size(); i++) {
        _io_wgs[i]->update_cur_select_factor(_io_wgs[i]->get_select_factor());
        total += _io_wgs[i]->get_select_factor();
        if (index == -1 || _io_wgs[i]->get_cur_select_factor() > _io_wgs[index]->get_cur_select_factor()) {
            index = i;
        }
    }
    _io_wgs[index]->update_cur_select_factor(0 - total);
    return index;
}

WorkGroup* WorkGroupManager::pick_next_wg_for_io() {
    std::unique_lock<std::mutex> lock(_global_io_mutex);
    while (_ready_wgs.empty()) {
        _cv.wait(lock);
    }
    WorkGroup* wg = nullptr;
    do {
        adjust_weight_if_need();
        wg = get_next_wg();
    } while (wg->io_task_queue_size() == 0);

    if (wg->io_task_queue_size() == 1) {
       _ready_wgs.erase(wg); 
    }
    _total_task_num--;

    // just for debug 
    LOG(WARNING) << "select: " << wg->name();
    return wg;
}

WorkGroupQueue& WorkGroupManager::get_io_queue() {
    return _wg_io_queue;
}

bool WorkGroupManager::try_offer_io_task(WorkGroup* wg, const PriorityThreadPool::Task& task) {
    std::lock_guard<std::mutex> lock(_global_io_mutex);
    bool is_ok = false;
    if (_ready_wgs.find(wg) == _ready_wgs.end()) {
        is_ok = wg->try_offer_io_task(task);
        if (is_ok) {
            _ready_wgs.emplace(wg);
        }    
    } else {
        is_ok = wg->try_offer_io_task(task);
    }
    
    if (is_ok) {
        _total_task_num++;
        _cv.notify_one();
    }

    return is_ok;
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