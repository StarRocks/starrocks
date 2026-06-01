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

#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "base/types/int128.h"
#include "common/status.h"
#include "exec/pipeline/primitives/driver_queue.h"
#include "exec/workgroup/mem_tracker_manager.h"
#include "exec/workgroup/pipeline_executor_set_manager.h"
#include "exec/workgroup/work_group.h"

namespace starrocks {

class MetricRegistry;

namespace workgroup {

struct WorkGroupMetrics;
using WorkGroupMetricsPtr = std::shared_ptr<WorkGroupMetrics>;

// WorkGroupManager is a singleton used to manage WorkGroup instances in BE, it has an io queue and a cpu queues for
// pick next workgroup for computation and launching io tasks.
class WorkGroupManager {
public:
    using DriverQueueFactory = std::function<pipeline::DriverQueuePtr(pipeline::DriverQueueMetrics*)>;

    WorkGroupManager(PipelineExecutorSetConfig executors_manager_conf, MetricRegistry* metrics,
                     DriverQueueFactory driver_queue_factory);

    ~WorkGroupManager();

    Status start();

    // add a new workgroup to WorkGroupManger
    WorkGroupPtr add_workgroup(const WorkGroupPtr& wg);
    // return reserved beforehand default workgroup for query is not bound to any workgroup
    WorkGroupPtr get_default_workgroup();
    // return reserved beforehand default mv workgroup for MV query is not bound to any workgroup
    WorkGroupPtr get_default_mv_workgroup();

    size_t num_workgroups() const { return _workgroups.size(); }

    void close();
    // destruct workgroups
    void destroy();

    void apply(const std::vector<TWorkGroupOp>& ops);
    std::vector<TWorkGroup> list_workgroups();
    std::vector<std::string> list_memory_pools() const;

    using WorkGroupConsumer = std::function<void(const WorkGroup&)>;
    void for_each_workgroup(const WorkGroupConsumer& consumer) const;

    void update_metrics();

    bool should_yield(const WorkGroup* wg) const;
    PipelineExecutorSet* shared_executors() const { return _executors_manager.shared_executors(); }
    void for_each_executors(const ExecutorsManager::ExecutorsConsumer& consumer) const;
    void change_num_connector_scan_threads(uint32_t num_connector_scan_threads);
    void change_enable_resource_group_cpu_borrowing(bool val);
    void change_exec_state_report_max_threads(int max_threads);
    void change_priority_exec_state_report_max_threads(int max_threads);
    void set_workgroup_expiration_time(std::chrono::seconds value);

private:
    using MutexType = std::shared_mutex;
    using UniqueLockType = std::unique_lock<MutexType>;
    using SharedLockType = std::shared_lock<MutexType>;

    // {create, alter,delete}_workgroup_unlocked is used to replay WorkGroupOps.
    // WorkGroupManager::_mutex is held when invoking these method.
    void create_workgroup_unlocked(const WorkGroupPtr& wg, UniqueLockType& lock);
    void alter_workgroup_unlocked(const WorkGroupPtr& wg, UniqueLockType& lock);
    void delete_workgroup_unlocked(const WorkGroupPtr& wg);
    void add_metrics_unlocked(const WorkGroupPtr& wg, UniqueLockType& unique_lock);
    void update_metrics_unlocked();
    void for_each_executors_unlocked(const ExecutorsManager::ExecutorsConsumer& consumer) const;
    WorkGroupPtr get_default_workgroup_unlocked();

private:
    mutable std::shared_mutex _mutex;
    pipeline::DriverQueueMetrics* _driver_queue_metrics = nullptr;
    DriverQueueFactory _driver_queue_factory;
    // Keep it before _workgroups to ensure the shared executors is destructed after all the dedicated executors for
    // workgroups, since _executors_manager owns the shared executors, and WorkGroup owns the dedicated executors.
    ExecutorsManager _executors_manager;

    struct WorkGroupQueueSet;
    // Internal queue owners keyed the same way as _workgroups. This is not the user-facing resource group id.
    std::unordered_map<int128_t, std::unique_ptr<WorkGroupQueueSet>> _workgroup_queue_sets;
    std::unordered_map<int128_t, WorkGroupPtr> _workgroups;
    std::unordered_map<int64_t, int64_t> _workgroup_versions;
    std::list<int128_t> _workgroup_expired_versions;
    std::chrono::seconds _workgroup_expiration_time{120};

    std::atomic<size_t> _sum_cpu_weight = 0;
    MetricRegistry* _metrics = nullptr;
    MemTrackerManager _shared_mem_tracker_manager;
    std::once_flag init_metrics_once_flag;
    std::unordered_map<std::string, WorkGroupMetricsPtr> _wg_metrics;
};

class DefaultWorkGroupInitialization {
public:
    DefaultWorkGroupInitialization(WorkGroupManager* workgroup_manager, int64_t max_executor_threads);

    // create or renew default group
    std::shared_ptr<WorkGroup> create_default_workgroup();
    // create or renew default mv group
    std::shared_ptr<WorkGroup> create_default_mv_workgroup();

private:
    WorkGroupManager* _workgroup_manager;
    int64_t _max_executor_threads;
};

} // namespace workgroup
} // namespace starrocks
