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

#include <cstdint>

#include "exec/workgroup/pipeline_executor_set.h"
#include "exec/workgroup/work_group_fwd.h"
#include "util/cpu_util.h"

namespace starrocks::workgroup {

/// Manage all PipelineExecutorSet and CPU resource allocation.
///
/// There are two types of PipelineExecutorSet:
/// - exclusive_executors: PipelineExecutorSet dedicated to a workgroup (workgroup with exclusive_cpu_cores > 0).
/// - shared_executors: PipelineExecutorSet shared by other workgroups (workgroup with exclusive_cpu_cores <= 0).
/// PipelineExecutorSet owner:
/// - exclusive_executors: owned by workgroup.
/// - shared_executors: owned by WorkGroupManager.
/// The timing of creating and starting PipelineExecutorSet:
/// - exclusive_executors: when creating or updating workgroup.
/// - shared_executors: when starting BE process.
/// The timing of stopping PipelineExecutorSet:
/// - exclusive_executors: when workgroup destructs.
/// - shared_executors: when closing BE process.
///
/// ExecutorsManager is owned by WorkGroupManager.
/// All the methods need to be protected by the `WorkGroupManager::_mutex` outside by callers.
class ExecutorsManager {
public:
    ExecutorsManager(WorkGroupManager* parent, PipelineExecutorSetConfig conf);

    void close() const;

    Status start_shared_executors();
    void update_shared_executors() const;
    PipelineExecutorSet* shared_executors() const { return _shared_executors.get(); }

    void assign_cpuids_to_workgroup(WorkGroup* wg);
    void reclaim_cpuids_from_worgroup(WorkGroup* wg);
    const CpuUtil::CpuIds& get_cpuids_of_workgroup(WorkGroup* wg) const;

    PipelineExecutorSet* create_and_assign_executors(WorkGroup* wg) const;

    void change_num_connector_scan_threads(uint32_t num_connector_scan_threads);
    void change_enable_resource_group_cpu_borrowing(bool val);

    using ExecutorsConsumer = std::function<void(PipelineExecutorSet&)>;
    void for_each_executors(const ExecutorsConsumer& consumer) const;

    /// Whether the task running on the borrowed CPU should yield the CPU, that is,
    /// the task of the owner resource group of the borrowed CPU has arrived.
    bool should_yield(const WorkGroup* wg) const;

private:
    static constexpr WorkGroup* COMMON_WORKGROUP = nullptr;

    WorkGroupManager* const _parent;
    PipelineExecutorSetConfig _conf;
    std::unordered_map<WorkGroup*, CpuUtil::CpuIds> _wg_to_cpuids;
    std::unique_ptr<PipelineExecutorSet> _shared_executors;

    struct CpuOwnerContext {
        std::shared_ptr<WorkGroup> wg;
        std::atomic<WorkGroup*> raw_wg;

        void set_wg(WorkGroup* new_wg);
        std::shared_ptr<WorkGroup> get_wg() const;
    };
    std::unordered_map<CpuUtil::CpuId, CpuOwnerContext> _cpu_owners;
};

} // namespace starrocks::workgroup
