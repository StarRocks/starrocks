// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <boost/cstdint.hpp>
#include <memory>
#include <string>
#include <vector>

#include "common/logging.h"

namespace starrocks {

/// CpuInfo is an interface to query for cpu information at runtime.  The caller can
/// ask for the sizes of the caches and what hardware features are supported.
/// On Linux, this information is pulled from a couple of sys files (/proc/cpuinfo and
/// /sys/devices)
class CpuInfo {
public:
    static const int64_t SSSE3 = (1 << 1);
    static const int64_t SSE4_1 = (1 << 2);
    static const int64_t SSE4_2 = (1 << 3);
    static const int64_t POPCNT = (1 << 4);
    static const int64_t AVX = (1 << 5);
    static const int64_t AVX2 = (1 << 6);

    /// Cache enums for L1 (data), L2 and L3
    enum CacheLevel {
        L1_CACHE = 0,
        L2_CACHE = 1,
        L3_CACHE = 2,
    };
    static const int NUM_CACHE_LEVELS = L3_CACHE + 1;

    /// Initialize CpuInfo.
    static void init();

    /// Returns whether of not the cpu supports this flag
    inline static bool is_supported(long flag) {
        DCHECK(initialized_);
        return (hardware_flags_ & flag) != 0;
    }

    /// Returns the number of cpu cycles per millisecond
    static int64_t cycles_per_ms() {
        DCHECK(initialized_);
        return cycles_per_ms_;
    }

    /// Returns the number of cores (including hyper-threaded) on this machine that are
    /// available for use by Impala (either the number of online cores or the value of
    /// the --num_cores command-line flag).
    static int num_cores() {
        DCHECK(initialized_);
        return num_cores_;
    }

    /// Returns the maximum number of cores that will be online in the system, including
    /// any offline cores or cores that could be added via hot-plugging.
    static int get_max_num_cores() { return max_num_cores_; }

    /// Returns the core that the current thread is running on. Always in range
    /// [0, GetMaxNumCores()). Note that the thread may be migrated to a different core
    /// at any time by the scheduler, so the caller should not assume the answer will
    /// remain stable.
    static int get_current_core();

    static std::string debug_string();

private:
    /// Initialize NUMA-related state - called from Init();
    static void _init_numa();

    /// Initialize num cores taking cgroup config into consideration
    static void _init_num_cores_with_cgroup();

    /// Initialize 'numa_node_to_cores_' based on 'max_num_numa_nodes_' and
    /// 'core_to_numa_node_'. Called from InitNuma();
    static void _init_numa_node_to_cores();

    /// Populates the arguments with information about this machine's caches.
    /// The values returned are not reliable in some environments, e.g. RHEL5 on EC2, so
    /// so we will keep this as a private method.
    static void _get_cache_info(long cache_sizes[NUM_CACHE_LEVELS], long cache_line_sizes[NUM_CACHE_LEVELS]);

    static bool initialized_;
    static int64_t hardware_flags_;
    static int64_t cycles_per_ms_;
    static int num_cores_;
    static int max_num_cores_;
    static std::string model_name_;

    /// Maximum possible number of NUMA nodes.
    static int max_num_numa_nodes_;

    /// Array with 'max_num_cores_' entries, each of which is the NUMA node of that core.
    static std::unique_ptr<int[]> core_to_numa_node_;

    /// Vector with 'max_num_numa_nodes_' entries, each of which is a vector of the cores
    /// belonging to that NUMA node.
    static std::vector<std::vector<int>> numa_node_to_cores_;

    /// Array with 'max_num_cores_' entries, each of which is the index of that core in its
    /// NUMA node.
    static std::vector<int> numa_node_core_idx_;
};
} // namespace starrocks
