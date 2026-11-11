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

#include "common/system/cpu_info.h"

#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <sstream>
#include <string_view>

#include "base/string/string_parser.hpp"
#include "common/config_runtime_fwd.h"
#include "gflags/gflags.h"
#include "gutil/strings/split.h"

using boost::algorithm::trim;
using std::string;

DEFINE_int32(num_cores, 0,
             "(Advanced) If > 0, it sets the number of cores available to"
             " Impala. Setting it to 0 means Impala will use all available cores on the machine"
             " according to /proc/cpuinfo.");

namespace starrocks {

bool CpuInfo::initialized_ = false;
int64_t CpuInfo::hardware_flags_ = 0;
int64_t CpuInfo::cycles_per_ms_;
int CpuInfo::num_cores_ = 1;
int CpuInfo::max_num_cores_ = 1;
std::string CpuInfo::model_name_ = "unknown";
bool CpuInfo::is_cgroup_with_cpuset_ = false;
bool CpuInfo::is_cgroup_with_cpu_quota_ = false;
int CpuInfo::max_num_numa_nodes_;
std::unique_ptr<int[]> CpuInfo::core_to_numa_node_;
std::vector<std::vector<int>> CpuInfo::numa_node_to_cores_;
std::vector<size_t> CpuInfo::cpuset_cores_;
std::set<size_t> CpuInfo::offline_cores_;
std::vector<int> CpuInfo::numa_node_core_idx_;
std::vector<long> CpuInfo::cache_sizes;
std::vector<long> CpuInfo::cache_line_sizes;

int64_t CpuInfo::_parse_flags(const string& values, const std::vector<FlagMapping>& mappings) {
    int64_t flags = 0;
    for (StringPiece token : strings::Split(values, " ", strings::SkipWhitespace())) {
        const std::string_view token_view(token.data(), token.size());
        for (const auto& flag_mapping : mappings) {
            if (token_view == flag_mapping.name) {
                flags |= flag_mapping.flag;
                break;
            }
        }
    }
    return flags;
}

int64_t CpuInfo::_parse_procfs_flags(const std::string& cpuinfo_content) {
    int64_t flags = 0;
    std::istringstream cpuinfo(cpuinfo_content);
    std::string line;
    while (getline(cpuinfo, line)) {
        size_t colon = line.find(':');
        if (colon == std::string::npos) continue;
        std::string name = line.substr(0, colon);
        trim(name);
        if (name == "flags") {
            std::string value = line.substr(colon + 1);
            trim(value);
            flags |= _parse_flags(value, _flag_mappings());
        }
    }
    return flags;
}

int64_t CpuInfo::_intersect_procfs_features(std::istream& stream, const std::vector<FlagMapping>& mappings) {
    std::string line;
    std::string name;
    std::string value;
    int64_t intersection_flags = 0;
    bool first_features_line = true;

    while (getline(stream, line)) {
        size_t colon = line.find(':');
        if (colon != std::string::npos) {
            name = line.substr(0, colon);
            value = line.substr(colon + 1);
            trim(name);
            trim(value);
            if (name == "Features") {
                int64_t core_flags = _parse_flags(value, mappings);
                if (first_features_line) {
                    intersection_flags = core_flags;
                    first_features_line = false;
                } else {
                    intersection_flags &= core_flags;
                }
            }
        }
    }
    if (stream.bad() || (stream.fail() && !stream.eof())) {
        return 0; // Fail closed on truncated or corrupted stream
    }
    return intersection_flags;
}

bool CpuInfo::_hwcap_available(unsigned long hwcap) {
    // glibc's getauxval() special-cases AT_HWCAP/AT_HWCAP2: it reads GLRO(dl_hwcap) directly
    // and never sets errno for these two auxv types (verified against glibc 2.39 in PR #79278
    // review), so an errno==0 check can never observe "unavailable" -- it is always true.
    // NEON/ASIMD is mandatory on every conforming AArch64 core, so a genuinely all-zero HWCAP
    // is the only signal we can trust that the auxv vector could not be populated (non-glibc
    // libc, sandboxed/emulated environment).
    return hwcap != 0;
}

int64_t CpuInfo::_resolve_arm_flags(bool aux_available, int64_t aux_flags, int64_t procfs_flags) {
    if (aux_available) {
        return aux_flags;
    }
    return procfs_flags;
}

void CpuInfo::init() {
    if (initialized_) return;

    _init_cpu_info();
    _init_offline_cores();
    _init_num_cores_with_cgroup();
    if (num_cores_ <= 0) {
        num_cores_ = 1;
    }
    if (config::num_cores > 0) num_cores_ = config::num_cores;
    _init_max_num_cores();
    _init_numa();
    _init_cache_info();
    initialized_ = true;
}

std::vector<size_t> CpuInfo::parse_cpus(const std::string& cpus_str) {
    std::vector<size_t> cpuids;
    std::vector<std::string> fields = strings::Split(cpus_str, ",", strings::SkipWhitespace());
    for (const auto& field : fields) {
        StringParser::ParseResult result;
        if (field.find('-') == std::string::npos) {
            auto cpu_id = StringParser::string_to_int<int32_t>(field.data(), field.size(), &result);
            if (result == StringParser::PARSE_SUCCESS) {
                cpuids.emplace_back(cpu_id);
            }
            continue;
        }

        std::vector<std::string> pair = strings::Split(field, "-", strings::SkipWhitespace());
        if (pair.size() != 2) {
            continue;
        }
        std::string& start_str = pair[0];
        std::string& end_str = pair[1];
        auto start = StringParser::string_to_int<int32_t>(start_str.data(), start_str.size(), &result);
        if (result != StringParser::PARSE_SUCCESS) {
            continue;
        }
        auto end = StringParser::string_to_int<int32_t>(end_str.data(), end_str.size(), &result);
        if (result != StringParser::PARSE_SUCCESS) {
            continue;
        }
        for (int i = start; i <= end; i++) {
            cpuids.emplace_back(i);
        }
    }
    return cpuids;
}

void CpuInfo::_init_numa_node_to_cores() {
    DCHECK(numa_node_to_cores_.empty());
    numa_node_to_cores_.resize(max_num_numa_nodes_);
    numa_node_core_idx_.resize(max_num_cores_);
    for (int core = 0; core < max_num_cores_; ++core) {
        std::vector<int>* cores_of_node = &numa_node_to_cores_[core_to_numa_node_[core]];
        numa_node_core_idx_[core] = cores_of_node->size();
        cores_of_node->push_back(core);
    }
}

void CpuInfo::_init_cache_info() {
    cache_sizes.resize(NUM_CACHE_LEVELS);
    cache_line_sizes.resize(NUM_CACHE_LEVELS);
    _get_cache_info(cache_sizes.data(), cache_line_sizes.data());
}

std::vector<size_t> CpuInfo::get_core_ids() {
    std::vector<size_t> core_ids;
    if (!cpuset_cores_.empty()) {
        core_ids = cpuset_cores_;
    } else {
        for (const auto& core_ids_of_node : numa_node_to_cores_) {
            core_ids.insert(core_ids.end(), core_ids_of_node.begin(), core_ids_of_node.end());
        }
    }

    std::erase_if(core_ids, [&](const size_t core) { return offline_cores_.contains(core); });

    return core_ids;
}

std::vector<std::string> CpuInfo::unsupported_cpu_flags_from_current_env() {
    std::vector<std::string> unsupported_flags;
    const int64_t required_flags = _compiled_hardware_flags();
    for (const auto& flag_mapping : _flag_mappings()) {
        if (!is_supported(flag_mapping.flag) && (required_flags & flag_mapping.flag) != 0) {
            unsupported_flags.emplace_back(flag_mapping.name);
        }
    }
    return unsupported_flags;
}

} // namespace starrocks
