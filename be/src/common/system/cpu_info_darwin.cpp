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

#include <sys/sysctl.h>

#include <algorithm>
#include <cstdlib>
#include <iostream>

#include "common/system/cpu_info.h"

namespace starrocks {

void CpuInfo::_init_cpu_info() {
    hardware_flags_ |= _detect_hardware_flags("");
    cycles_per_ms_ = 1000000;
}

void CpuInfo::_init_max_num_cores() {
    // On macOS, use sysctl to get number of configured cores.
    int mib[2];
    size_t len = sizeof(max_num_cores_);
    mib[0] = CTL_HW;
    mib[1] = HW_NCPU;
    sysctl(mib, 2, &max_num_cores_, &len, NULL, 0);
    std::cerr << "Built on a system without sched_getcpu() support. Performance may be impacted.";
}

void CpuInfo::_init_numa() {
    // macOS has no Linux sysfs NUMA topology; retain the single-node fallback.
    core_to_numa_node_.reset(new int[max_num_cores_]);
    max_num_numa_nodes_ = 1;
    std::fill_n(core_to_numa_node_.get(), max_num_cores_, 0);
    _init_numa_node_to_cores();
}

void CpuInfo::_init_num_cores_with_cgroup() {}

void CpuInfo::_init_offline_cores() {
    offline_cores_.clear();
}

int CpuInfo::get_current_core() {
    return 0;
}

void CpuInfo::_get_cache_info(long cache_sizes[NUM_CACHE_LEVELS], long cache_line_sizes[NUM_CACHE_LEVELS]) {
    // On Mac OS X use sysctl() to get the cache sizes
    size_t len = 0;
    sysctlbyname("hw.cachesize", NULL, &len, NULL, 0);
    uint64_t* data = static_cast<uint64_t*>(malloc(len));
    sysctlbyname("hw.cachesize", data, &len, NULL, 0);
    DCHECK(len / sizeof(uint64_t) >= 3);
    for (size_t i = 0; i < NUM_CACHE_LEVELS; ++i) {
        cache_sizes[i] = data[i];
    }
    size_t linesize;
    size_t sizeof_linesize = sizeof(linesize);
    sysctlbyname("hw.cachelinesize", &linesize, &sizeof_linesize, NULL, 0);
    for (size_t i = 0; i < NUM_CACHE_LEVELS; ++i) cache_line_sizes[i] = linesize;
}

} // namespace starrocks
