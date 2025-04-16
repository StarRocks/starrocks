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

#include "util/cpu_util.h"

#include <fmt/format.h>

#include "common/config.h"
#include "util/thread.h"

namespace starrocks {

void CpuUtil::bind_cpus(Thread* thread, const std::vector<size_t>& cpuids) {
    if (cpuids.empty()) {
        return;
    }

    auto do_bind_cpus = [&] {
        if (!config::enable_resource_group_bind_cpus) {
            return 0;
        }

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        for (const auto cpu_id : cpuids) {
            CPU_SET(cpu_id, &cpuset);
        }

        return pthread_setaffinity_np(thread->pthread_id(), sizeof(cpu_set_t), &cpuset);
    };

    if (const int res = do_bind_cpus(); res != 0) {
        LOG(WARNING) << fmt::format("failed to bind cpus [tid={}] [cpuids={}] [error_code={}] [error={}]",
                                    pthread_self(), to_string(cpuids), res, std::strerror(res));
        return;
    }

    thread->set_num_bound_cpu_cores(config::enable_resource_group_bind_cpus ? cpuids.size() : 0);
    thread->set_first_bound_cpuid(cpuids[0]);
}

std::string CpuUtil::to_string(const CpuIds& cpuids) {
    std::string result = "(";
    for (size_t i = 0; i < cpuids.size(); i++) {
        if (i != 0) {
            result += ",";
        }
        result += std::to_string(cpuids[i]);
    }
    result += ")";
    return result;
}

} // namespace starrocks
