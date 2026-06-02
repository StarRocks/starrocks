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
#include <memory>
#include <string>
#include <vector>

#include "common/statusor.h"
#include "common/thread/cpu_util.h"
#include "compute_env/workgroup/work_group_schedule_policy.h"
#include "exec/pipeline/primitives/driver_executor.h"

namespace starrocks::pipeline {

class PipelineExecutorMetrics;

StatusOr<std::unique_ptr<DriverExecutor>> create_workgroup_driver_executor(
        const std::string& name, const CpuUtil::CpuIds& cpuids, const std::vector<CpuUtil::CpuIds>& borrowed_cpuids,
        uint32_t num_driver_threads, PipelineExecutorMetrics* metrics,
        const workgroup::WorkGroupSchedulePolicy& schedule_policy);

} // namespace starrocks::pipeline
