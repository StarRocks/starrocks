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

#include <linux/magic.h>
#include <sched.h>
#include <sys/sysinfo.h>
#include <sys/vfs.h>
#include <unistd.h>

#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <cctype>
#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <system_error>

#include "base/path/file_util.h"
#include "base/string/string_parser.hpp"
#include "base/system/errno.h"
#include "common/system/cpu_info.h"
#include "gutil/strings/substitute.h"

// cgroup v2 filesystem magic, including builds with older Linux headers.
#ifndef CGROUP2_SUPER_MAGIC
#define CGROUP2_SUPER_MAGIC 0x63677270
#endif

using boost::algorithm::trim;
using std::max;
using std::string;

namespace starrocks {

void CpuInfo::_init_cpu_info() {
    float max_mhz = 0;
    int num_cores = 0;

    std::string cpuinfo_content;
    {
        std::ifstream file("/proc/cpuinfo");
        if (file.is_open()) {
            std::stringstream buffer;
            std::string line;
            while (std::getline(file, line)) {
                buffer << line << "\n";
            }
            if (file.bad() || (file.fail() && !file.eof())) {
                LOG(ERROR) << "I/O error reading /proc/cpuinfo; CPU feature detection may be incomplete";
                cpuinfo_content.clear();
            } else {
                cpuinfo_content = buffer.str();
            }
        } else {
            LOG(ERROR) << "Unable to open /proc/cpuinfo; CPU feature detection may be incomplete";
        }
    }
    std::string line;
    std::string name;
    std::string value;

    std::istringstream cpuinfo(cpuinfo_content);
    while (cpuinfo) {
        getline(cpuinfo, line);
        size_t colon = line.find(':');
        if (colon != string::npos) {
            name = line.substr(0, colon);
            value = line.substr(colon + 1, string::npos);
            trim(name);
            trim(value);
            if (name.compare("cpu MHz") == 0) {
                // Every core will report a different speed.  We'll take the max, assuming
                // that when impala is running, the core will not be in a lower power state.
                // TODO: is there a more robust way to do this, such as
                // Window's QueryPerformanceFrequency()
                float mhz = atof(value.c_str());
                max_mhz = max(mhz, max_mhz);
            } else if (name.compare("processor") == 0) {
                ++num_cores;
            } else if (name.compare("model name") == 0) {
                model_name_ = value;
            }
        }
    }

    hardware_flags_ |= _detect_hardware_flags(cpuinfo_content);
    cycles_per_ms_ = max_mhz != 0 ? max_mhz * 1000 : 1000000;
    if (num_cores > 0) {
        num_cores_ = num_cores;
    }
}

void CpuInfo::_init_max_num_cores() {
    max_num_cores_ = get_nprocs_conf();

    // Print a warning if something is wrong with sched_getcpu().
#ifdef HAVE_SCHED_GETCPU
    if (sched_getcpu() == -1) {
        std::cerr << "Kernel does not support sched_getcpu(). Performance may be impacted.";
    }
#else
    std::cerr << "Built on a system without sched_getcpu() support. Performance may be impacted.";
#endif
}

void CpuInfo::_init_numa() {
    // Use the NUMA info in the /sys filesystem. which is part of the Linux ABI:
    // see https://www.kernel.org/doc/Documentation/ABI/stable/sysfs-devices-node and
    // https://www.kernel.org/doc/Documentation/ABI/testing/sysfs-devices-system-cpu
    // The filesystem entries are only present if the kernel was compiled with NUMA support.
    core_to_numa_node_.reset(new int[max_num_cores_]);

    if (!std::filesystem::is_directory("/sys/devices/system/node")) {
        LOG(WARNING) << "/sys/devices/system/node is not present - no NUMA support";
        // Assume a single NUMA node.
        max_num_numa_nodes_ = 1;
        std::fill_n(core_to_numa_node_.get(), max_num_cores_, 0);
        _init_numa_node_to_cores();
        return;
    }

    // Search for node subdirectories - node0, node1, node2, etc to determine possible
    // NUMA nodes.
    max_num_numa_nodes_ = 0;
    for (const auto& item : std::filesystem::directory_iterator("/sys/devices/system/node")) {
        const string filename = item.path().filename().string();
        if (filename.find("node") == 0) ++max_num_numa_nodes_;
    }
    if (max_num_numa_nodes_ == 0) {
        LOG(WARNING) << "Could not find nodes in /sys/devices/system/node";
        max_num_numa_nodes_ = 1;
    }

    // Check which NUMA node each core belongs to based on the existence of a symlink
    // to the node subdirectory.
    for (int core = 0; core < max_num_cores_; ++core) {
        bool found_numa_node = false;
        for (int node = 0; node < max_num_numa_nodes_; ++node) {
            if (std::filesystem::exists(strings::Substitute("/sys/devices/system/cpu/cpu$0/node$1", core, node))) {
                core_to_numa_node_[core] = node;
                found_numa_node = true;
                break;
            }
        }
        if (!found_numa_node) {
            LOG(WARNING) << "Could not determine NUMA node for core " << core << " from /sys/devices/system/cpu/";
            core_to_numa_node_[core] = 0;
        }
    }
    _init_numa_node_to_cores();
}

void CpuInfo::_init_num_cores_with_cgroup() {
    std::error_code ec;
    bool running_in_docker = std::filesystem::exists("/.dockerenv", ec);
    if (!running_in_docker) {
        return;
    }
    struct statfs fs;
    if (statfs("/sys/fs/cgroup", &fs) < 0) {
        LOG(WARNING) << "Fail to get file system statistics. err: " << errno_to_string(errno);
        return;
    }

    std::string cfs_period_us_str;
    std::string cfs_quota_us_str;
    std::string cpuset_str;
    if (fs.f_type == TMPFS_MAGIC) {
        // cgroup v1
        if (!FileUtil::read_whole_content("/sys/fs/cgroup/cpu/cpu.cfs_period_us", cfs_period_us_str)) {
            return;
        }

        if (!FileUtil::read_whole_content("/sys/fs/cgroup/cpu/cpu.cfs_quota_us", cfs_quota_us_str)) {
            return;
        }

        if (!FileUtil::read_whole_content("/sys/fs/cgroup/cpuset/cpuset.cpus", cpuset_str)) {
            return;
        }
    } else if (fs.f_type == CGROUP2_SUPER_MAGIC) {
        // cgroup v2
        if (!FileUtil::read_contents("/sys/fs/cgroup/cpu.max", cfs_quota_us_str, cfs_period_us_str)) {
            return;
        }

        if (!FileUtil::read_whole_content("/sys/fs/cgroup/cpuset.cpus", cpuset_str)) {
            return;
        }
    }

    int32_t cfs_num_cores = num_cores_;
    {
        StringParser::ParseResult result;
        auto cfs_period_us =
                StringParser::string_to_int<int64_t>(cfs_period_us_str.data(), cfs_period_us_str.size(), &result);
        if (result != StringParser::PARSE_SUCCESS) {
            cfs_period_us = -1;
        }
        auto cfs_quota_us =
                StringParser::string_to_int<int64_t>(cfs_quota_us_str.data(), cfs_quota_us_str.size(), &result);
        if (result != StringParser::PARSE_SUCCESS) {
            cfs_quota_us = -1;
        }
        if (cfs_quota_us > 0 && cfs_period_us > 0) {
            cfs_num_cores = cfs_quota_us / cfs_period_us;
            is_cgroup_with_cpu_quota_ = true;
        }
    }

    int32_t cpuset_num_cores = num_cores_;
    if (!cpuset_str.empty() &&
        std::any_of(cpuset_str.begin(), cpuset_str.end(), [](char c) { return !std::isspace(c); })) {
        cpuset_cores_ = parse_cpus(cpuset_str);
        std::erase_if(cpuset_cores_, [&](const size_t core) { return offline_cores_.contains(core); });
        cpuset_num_cores = cpuset_cores_.size();
        is_cgroup_with_cpuset_ = true;
    }

    if (cfs_num_cores < num_cores_ || cpuset_num_cores < num_cores_) {
        num_cores_ = std::max(1, std::min(cfs_num_cores, cpuset_num_cores));
        LOG(INFO) << "Init docker hardware cores by cgroup's config, cfs_num_cores=" << cfs_num_cores
                  << ", cpuset_num_cores=" << cpuset_num_cores << ", final num_cores=" << num_cores_;
    }
}

void CpuInfo::_init_offline_cores() {
    offline_cores_.clear();
    std::string offline_cores_str;
    if (!FileUtil::read_whole_content("/sys/devices/system/cpu/offline", offline_cores_str)) {
        return;
    }

    std::vector<size_t> offline_cores = parse_cpus(offline_cores_str);
    offline_cores_.insert(offline_cores.begin(), offline_cores.end());
}

int CpuInfo::get_current_core() {
    // sched_getcpu() is not supported on some old kernels/glibcs (like the versions that
    // shipped with CentOS 5). In that case just pretend we're always running on CPU 0
    // so that we can build and run with degraded perf.
#ifdef HAVE_SCHED_GETCPU
    int cpu = sched_getcpu();
    if (cpu < 0) return 0;
    if (cpu >= max_num_cores_) {
        LOG_FIRST_N(WARNING, 5) << "sched_getcpu() return value " << cpu
                                << ", which is greater than get_nprocs_conf() retrun value " << max_num_cores_
                                << ", now is " << get_nprocs_conf();
        cpu %= max_num_cores_;
    }
    return cpu;
#else
    return 0;
#endif
}

void CpuInfo::_get_cache_info(long cache_sizes[NUM_CACHE_LEVELS], long cache_line_sizes[NUM_CACHE_LEVELS]) {
    // Call sysconf to query for the cache sizes
    // Note: on some systems (e.g. RHEL 5 on AWS EC2), this returns 0 instead of the
    // actual cache line size.
    cache_sizes[L1_CACHE] = sysconf(_SC_LEVEL1_DCACHE_SIZE);
    cache_sizes[L2_CACHE] = sysconf(_SC_LEVEL2_CACHE_SIZE);
    cache_sizes[L3_CACHE] = sysconf(_SC_LEVEL3_CACHE_SIZE);

    cache_line_sizes[L1_CACHE] = sysconf(_SC_LEVEL1_DCACHE_LINESIZE);
    cache_line_sizes[L2_CACHE] = sysconf(_SC_LEVEL2_CACHE_LINESIZE);
    cache_line_sizes[L3_CACHE] = sysconf(_SC_LEVEL3_CACHE_LINESIZE);
}

} // namespace starrocks
