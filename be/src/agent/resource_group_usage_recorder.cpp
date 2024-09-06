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

#include "agent/resource_group_usage_recorder.h"

#include "exec/workgroup/work_group.h"
#include "util/time.h"

namespace starrocks {

ResourceGroupUsageRecorder::ResourceGroupUsageRecorder() {
    // Record cpu usage of all the groups the first time.
    [[maybe_unused]] auto _ = get_resource_group_usages();
}

std::vector<TResourceGroupUsage> ResourceGroupUsageRecorder::get_resource_group_usages() {
    int64_t timestamp_ns = MonotonicNanos();
    int64_t delta_ns = std::max<int64_t>(1, timestamp_ns - _timestamp_ns);
    _timestamp_ns = timestamp_ns;

    std::unordered_map<int64_t, TResourceGroupUsage> group_to_usage;
    std::unordered_map<int64_t, int64_t> curr_group_to_cpu_runtime_ns;

    ExecEnv::GetInstance()->workgroup_manager()->for_each_workgroup(
            [&group_to_usage, &curr_group_to_cpu_runtime_ns](const workgroup::WorkGroup& wg) {
                auto it = group_to_usage.find(wg.id());
                if (it == group_to_usage.end()) {
                    TResourceGroupUsage group_usage;
                    group_usage.__set_group_id(wg.id());
                    group_usage.__set_mem_used_bytes(wg.mem_consumption_bytes());
                    group_usage.__set_num_running_queries(wg.num_running_queries());
                    group_to_usage.emplace(wg.id(), std::move(group_usage));

                    curr_group_to_cpu_runtime_ns.emplace(wg.id(), wg.cpu_runtime_ns());
                } else {
                    TResourceGroupUsage& group_usage = it->second;
                    group_usage.__set_mem_used_bytes(group_usage.mem_used_bytes + wg.mem_consumption_bytes());
                    group_usage.__set_num_running_queries(group_usage.num_running_queries + wg.num_running_queries());

                    curr_group_to_cpu_runtime_ns[wg.id()] += wg.cpu_runtime_ns();
                }
            });

    for (const auto& [group_id, cpu_runtime_ns] : curr_group_to_cpu_runtime_ns) {
        auto iter_prev = _group_to_cpu_runtime_ns.find(group_id);
        int64_t prev_runtime_ns;
        if (iter_prev == _group_to_cpu_runtime_ns.end()) {
            prev_runtime_ns = 0;
        } else {
            prev_runtime_ns = iter_prev->second;
        }

        int32_t cpu_core_used_permille = (cpu_runtime_ns - prev_runtime_ns) * 1000 / delta_ns;
        group_to_usage[group_id].__set_cpu_core_used_permille(cpu_core_used_permille);
    }
    _group_to_cpu_runtime_ns = std::move(curr_group_to_cpu_runtime_ns);

    std::vector<TResourceGroupUsage> group_usages;
    group_usages.reserve(group_to_usage.size());
    for (auto& [_, group_usage] : group_to_usage) {
        // Only report the resource group with effective resource usages.
        if (group_usage.cpu_core_used_permille > 0 || group_usage.mem_used_bytes > 0 ||
            group_usage.num_running_queries > 0) {
            group_usages.emplace_back(std::move(group_usage));
        }
    }

    return group_usages;
}

} // namespace starrocks
