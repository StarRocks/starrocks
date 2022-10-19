// This file is licensed under the Elastic License 2.0. Copyright 2021-present StarRocks Limited.

#include "exec/workgroup/scan_task_queue.h"

#include "exec/workgroup/work_group.h"

namespace starrocks::workgroup {

StatusOr<ScanTask> FifoScanTaskQueue::take(int worker_id) {
    auto task = std::move(_queue.front());
    _queue.pop();
    return task;
}

<<<<<<< HEAD
bool FifoScanTaskQueue::try_offer(ScanTask task) {
    _queue.emplace(std::move(task));
    return true;
=======
bool PriorityScanTaskQueue::try_offer(ScanTask task) {
    return _queue.try_put(std::move(task));
}

/// WorkGroupScanTaskQueue.
bool WorkGroupScanTaskQueue::WorkGroupScanSchedEntityComparator::operator()(
        const WorkGroupScanSchedEntityPtr& lhs, const WorkGroupScanSchedEntityPtr& rhs) const {
    return lhs->vruntime_ns() < rhs->vruntime_ns();
>>>>>>> a923234a7 ([Enhancement] Use non-blocking put for PriorityScanTaskQueue::try_offer (#12261))
}

void ScanTaskQueueWithWorkGroup::close() {
    std::lock_guard<std::mutex> lock(_global_mutex);

    if (_is_closed) {
        return;
    }

    _is_closed = true;
    _cv.notify_all();
}

void ScanTaskQueueWithWorkGroup::_cal_wg_cpu_real_use_ratio() {
    int64_t total_run_time = 0;
    std::vector<int64_t> growth_times;
    growth_times.reserve(_ready_wgs.size());
    for (auto& wg : _ready_wgs) {
        growth_times.emplace_back(wg->growth_real_runtime_ns());
        total_run_time += growth_times.back();
        wg->update_last_real_runtime_ns(wg->real_runtime_ns());
    }

    int i = 0;
    for (auto& wg : _ready_wgs) {
        double cpu_actual_use_ratio = ((double)growth_times[i] / (total_run_time));
        wg->set_cpu_actual_use_ratio(cpu_actual_use_ratio);
        i++;
    }
}

StatusOr<ScanTask> ScanTaskQueueWithWorkGroup::take(int worker_id) {
    std::unique_lock<std::mutex> lock(_global_mutex);

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

    WorkGroupPtr wg = _select_next_wg(worker_id);
    if (wg->scan_task_queue()->size() == 1) {
        _ready_wgs.erase(wg);
    }

    _total_task_num--;

    return wg->scan_task_queue()->take(worker_id);
}

bool ScanTaskQueueWithWorkGroup::try_offer(ScanTask task) {
    std::lock_guard<std::mutex> lock(_global_mutex);

    WorkGroupPtr wg = task.workgroup;
    wg->scan_task_queue()->try_offer(std::move(task));
    if (_ready_wgs.find(wg) == _ready_wgs.end()) {
        _ready_wgs.emplace(wg);
    }

    _total_task_num++;
    _cv.notify_one();
    return true;
}

void ScanTaskQueueWithWorkGroup::_maybe_adjust_weight() {
    if (--_remaining_schedule_num_period > 0) {
        return;
    }

    _cal_wg_cpu_real_use_ratio();

    int num_tasks = 0;
    // calculate all wg factors
    for (auto& wg : _ready_wgs) {
        wg->estimate_trend_factor_period();
        num_tasks += wg->scan_task_queue()->size();
    }

    _remaining_schedule_num_period = std::min(MAX_SCHEDULE_NUM_PERIOD, num_tasks);

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

    // If positive_total_diff_factor <= 0, This means that all WorkGs have no excess resources
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

WorkGroupPtr ScanTaskQueueWithWorkGroup::_select_next_wg(int worker_id) {
    auto owner_wgs = workgroup::WorkGroupManager::instance()->get_owners_of_scan_worker(worker_id);

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

} // namespace starrocks::workgroup
