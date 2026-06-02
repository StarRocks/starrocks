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

#include "exec/pipeline/driver_executor_factory.h"

#include "common/thread/threadpool.h"
#include "exec/pipeline/pipeline_driver_executor.h"

namespace starrocks::pipeline {

StatusOr<std::unique_ptr<DriverExecutor>> create_workgroup_driver_executor(
        const std::string& name, const CpuUtil::CpuIds& cpuids, const std::vector<CpuUtil::CpuIds>& borrowed_cpuids,
        uint32_t num_driver_threads, PipelineExecutorMetrics* metrics,
        const workgroup::WorkGroupSchedulePolicy& schedule_policy) {
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

    std::unique_ptr<DriverExecutor> driver_executor = std::make_unique<GlobalDriverExecutor>(
            name, std::move(thread_pool), true, cpuids, metrics, schedule_policy);
    return driver_executor;
}

} // namespace starrocks::pipeline
