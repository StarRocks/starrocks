// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/workgroup/pipeline_executor_set_manager.h"

#include "common/statusor.h"
#include "common/thread/thread.h"
#include "common/thread/threadpool.h"
#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/workgroup/work_group.h"

namespace starrocks::workgroup {

namespace {

StatusOr<std::unique_ptr<pipeline::DriverExecutor>> create_driver_executor(
        const PipelineExecutorSetConfig& conf, const std::string& name, const CpuUtil::CpuIds& cpuids,
        const std::vector<CpuUtil::CpuIds>& borrowed_cpuids, uint32_t num_driver_threads,
        const WorkGroupSchedulePolicy& schedule_policy) {
    std::unique_ptr<ThreadPool> thread_pool;
    const Status status = ThreadPoolBuilder("pip_exec_" + name)
                                  .set_min_threads(0)
                                  .set_max_threads(num_driver_threads)
                                  .set_max_queue_size(1000)
                                  .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
                                  .set_cpuids(cpuids)
                                  .set_borrowed_cpuids(borrowed_cpuids)
                                  .build(&thread_pool);
    if (!status.ok()) {
        return status;
    }

    std::unique_ptr<pipeline::DriverExecutor> driver_executor = std::make_unique<pipeline::GlobalDriverExecutor>(
            name, std::move(thread_pool), true, cpuids, conf.metrics, schedule_policy);
    return driver_executor;
}

} // namespace

ExecutorsManager::ExecutorsManager(PipelineExecutorSetConfig conf, const WorkGroupSchedulePolicy& schedule_policy)
        : _conf(std::move(conf)),
          _schedule_policy(schedule_policy),
          _shared_executors(std::make_unique<PipelineExecutorSet>(_conf, "com", _conf.total_cpuids,
                                                                  std::vector<CpuUtil::CpuIds>{}, _schedule_policy)) {
    _wg_to_cpuids[COMMON_WORKGROUP] = _conf.total_cpuids;
    for (auto cpuid : _conf.total_cpuids) {
        _cpu_owners[cpuid].set_wg(COMMON_WORKGROUP);
    }
}

Status ExecutorsManager::start_shared_executors_unlocked() const {
    auto driver_executor = create_driver_executor(_conf, "com", _conf.total_cpuids, std::vector<CpuUtil::CpuIds>{},
                                                  _shared_executors->num_driver_threads(), _schedule_policy);
    if (!driver_executor.ok()) {
        return driver_executor.status();
    }
    return _shared_executors->start(std::move(driver_executor).value());
}

void ExecutorsManager::update_shared_executors() const {
    std::vector<CpuUtil::CpuIds> borrowed_cpuids;
    if (_conf.enable_cpu_borrowing) {
        for (const auto& [wg, cpuids] : _wg_to_cpuids) {
            if (wg != COMMON_WORKGROUP) {
                borrowed_cpuids.emplace_back(cpuids);
            }
        }
    }
    _shared_executors->change_cpus(get_cpuids_of_workgroup(COMMON_WORKGROUP), borrowed_cpuids);
}

void ExecutorsManager::assign_cpuids_to_workgroup(WorkGroup* wg) {
    if (wg->exclusive_cpu_cores() <= 0 || _wg_to_cpuids.contains(wg)) {
        return;
    }

    const auto& common_cpuids = get_cpuids_of_workgroup(COMMON_WORKGROUP);
    if (common_cpuids.size() <= 1) {
        return;
    }

    CpuUtil::CpuIds cpuids;
    CpuUtil::CpuIds new_common_cpuids;
    const size_t n = std::min<size_t>({wg->exclusive_cpu_cores(), common_cpuids.size() - 1, _conf.num_total_cores - 1});
    std::copy_n(common_cpuids.begin(), n, std::back_inserter(cpuids));
    std::copy(common_cpuids.begin() + n, common_cpuids.end(), std::back_inserter(new_common_cpuids));

    LOG(INFO) << "[WORKGROUP] assign cpuids to workgroup "
              << "[workgroup=" << wg->to_string() << "] "
              << "[cpuids=" << CpuUtil::to_string(cpuids) << "] ";

    if (!cpuids.empty()) {
        for (auto cpuid : cpuids) {
            _cpu_owners[cpuid].set_wg(wg);
        }
        _wg_to_cpuids[wg] = std::move(cpuids);
    }
    if (new_common_cpuids.size() != common_cpuids.size()) {
        _wg_to_cpuids[COMMON_WORKGROUP] = std::move(new_common_cpuids);
    }
}

void ExecutorsManager::reclaim_cpuids_from_worgroup(WorkGroup* wg) {
    const auto& cpuids = get_cpuids_of_workgroup(wg);
    if (cpuids.empty()) {
        return;
    }

    LOG(INFO) << "[WORKGROUP] reclaim cpuids from workgroup "
              << "[workgroup=" << wg->to_string() << "] "
              << "[cpuids=" << CpuUtil::to_string(cpuids) << "] ";

    std::ranges::copy(cpuids, std::back_inserter(_wg_to_cpuids[COMMON_WORKGROUP]));
    for (auto cpuid : cpuids) {
        _cpu_owners[cpuid].set_wg(COMMON_WORKGROUP);
    }

    _wg_to_cpuids.erase(wg);
}

const CpuUtil::CpuIds& ExecutorsManager::get_cpuids_of_workgroup(WorkGroup* wg) const {
    static const CpuUtil::CpuIds empty_cpuids;
    const auto it = _wg_to_cpuids.find(wg);
    if (it == _wg_to_cpuids.end()) {
        return empty_cpuids;
    }
    return it->second;
}

/// The `PipelineExecutorSet::start()` registers metrics, which acquires the metric lock. However, the metric collector
/// first acquires the metric lock and then requests the `WorkGroupManager` lock to update the metric.
/// Therefore, during `start`, it is crucial not to hold the `WorkGroupManager` lock to avoid a potential deadlock.
std::unique_ptr<PipelineExecutorSet> ExecutorsManager::maybe_create_exclusive_executors_unlocked(
        WorkGroup* wg, const CpuUtil::CpuIds& cpuids) const {
    if (wg->exclusive_cpu_cores() == 0 || cpuids.empty()) {
        LOG(INFO) << "[WORKGROUP] assign shared executors to workgroup "
                  << "[workgroup=" << wg->to_string() << "] ";
        return nullptr;
    }

    const std::string name = std::to_string(wg->id());
    auto executors = std::make_unique<PipelineExecutorSet>(_conf, name, cpuids, std::vector<CpuUtil::CpuIds>{},
                                                           _schedule_policy);
    auto driver_executor = create_driver_executor(_conf, name, cpuids, std::vector<CpuUtil::CpuIds>{},
                                                  executors->num_driver_threads(), _schedule_policy);
    if (!driver_executor.ok()) {
        LOG(WARNING) << "[WORKGROUP] failed to create driver executor for workgroup "
                     << "[workgroup=" << wg->to_string() << "] "
                     << "[conf=" << _conf.to_string() << "] "
                     << "[cpuids=" << CpuUtil::to_string(cpuids) << "] "
                     << "[status=" << driver_executor.status() << "]";
        LOG(INFO) << "[WORKGROUP] assign shared executors to workgroup "
                  << "[workgroup=" << wg->to_string() << "] ";
        return nullptr;
    }
    if (const Status status = executors->start(std::move(driver_executor).value()); !status.ok()) {
        LOG(WARNING) << "[WORKGROUP] failed to start executors for workgroup "
                     << "[workgroup=" << wg->to_string() << "] "
                     << "[conf=" << _conf.to_string() << "] "
                     << "[cpuids=" << CpuUtil::to_string(cpuids) << "] "
                     << "[status=" << status << "]";
        LOG(INFO) << "[WORKGROUP] assign shared executors to workgroup "
                  << "[workgroup=" << wg->to_string() << "] ";
        executors->close();
        return nullptr;
    }

    LOG(INFO) << "[WORKGROUP] assign dedicated executors to workgroup "
              << "[workgroup=" << wg->to_string() << "] ";
    return executors;
}

bool ExecutorsManager::change_num_connector_scan_threads(uint32_t num_connector_scan_threads) {
    const auto prev_val = _conf.num_total_connector_scan_threads;
    _conf.num_total_connector_scan_threads = num_connector_scan_threads;
    if (_conf.num_total_connector_scan_threads == prev_val) {
        return false;
    }

    return true;
}

void ExecutorsManager::change_enable_resource_group_cpu_borrowing(bool val) {
    const bool new_val = val && _conf.enable_bind_cpus;
    if (_conf.enable_cpu_borrowing == new_val) {
        return;
    }
    _conf.enable_cpu_borrowing = new_val;

    update_shared_executors();
}

bool ExecutorsManager::should_yield(const WorkGroup* wg) const {
    if (!_conf.enable_cpu_borrowing) {
        return false;
    }

    // Only resource groups without dedicated executors can borrow CPU.
    if (wg->exclusive_executors() != nullptr) {
        return false;
    }

    const Thread* thread = Thread::current_thread();
    if (thread == nullptr) {
        return false;
    }
    const auto it = _cpu_owners.find(thread->first_bound_cpuid());
    if (it == _cpu_owners.end()) {
        return false;
    }

    // Check before using `get_wg`, which is a little heavy.
    if (it->second.raw_wg == nullptr) {
        return false;
    }
    const auto owner_wg = it->second.get_wg();
    return owner_wg != nullptr && owner_wg->num_running_queries() > 0;
}

void ExecutorsManager::CpuOwnerContext::set_wg(WorkGroup* new_wg) {
    // TODO: use std::atomic<std::shared_ptr> instead of std::shared_ptr with atomic load/store,
    // when our compiler support it.
    std::atomic_store(&wg, new_wg == nullptr ? nullptr : new_wg->shared_from_this());
    raw_wg = new_wg;
}

std::shared_ptr<WorkGroup> ExecutorsManager::CpuOwnerContext::get_wg() const {
    return std::atomic_load(&wg);
}

} // namespace starrocks::workgroup
