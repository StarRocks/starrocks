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

#include <sstream>

#include "base/utility/pretty_printer.h"
#include "common/system/cpu_info.h"
#include "gutil/strings/substitute.h"

namespace starrocks {

std::string CpuInfo::debug_string() {
    DCHECK(initialized_);
    std::stringstream stream;
    long cache_sizes[NUM_CACHE_LEVELS];
    long cache_line_sizes[NUM_CACHE_LEVELS];
    _get_cache_info(cache_sizes, cache_line_sizes);

    std::string L1 =
            strings::Substitute("L1 Cache: $0 (Line: $1)", PrettyPrinter::print(cache_sizes[L1_CACHE], TUnit::BYTES),
                                PrettyPrinter::print(cache_line_sizes[L1_CACHE], TUnit::BYTES));
    std::string L2 =
            strings::Substitute("L2 Cache: $0 (Line: $1)", PrettyPrinter::print(cache_sizes[L2_CACHE], TUnit::BYTES),
                                PrettyPrinter::print(cache_line_sizes[L2_CACHE], TUnit::BYTES));
    std::string L3 =
            strings::Substitute("L3 Cache: $0 (Line: $1)", PrettyPrinter::print(cache_sizes[L3_CACHE], TUnit::BYTES),
                                PrettyPrinter::print(cache_line_sizes[L3_CACHE], TUnit::BYTES));
    stream << "Cpu Info:" << std::endl
           << "  Model: " << model_name_ << std::endl
           << "  Cores: " << num_cores_ << std::endl
           << "  Max Possible Cores: " << max_num_cores_ << std::endl
           << "  " << L1 << std::endl
           << "  " << L2 << std::endl
           << "  " << L3 << std::endl
           << "  Hardware Supports:" << std::endl;
    for (const auto& flag_mapping : _flag_mappings()) {
        if (is_supported(flag_mapping.flag)) {
            stream << "    " << flag_mapping.name << std::endl;
        }
    }
    stream << "  Numa Nodes: " << max_num_numa_nodes_ << std::endl;
    stream << "  Numa Nodes of Cores:";
    for (int core = 0; core < max_num_cores_; ++core) {
        stream << " " << core << "->" << core_to_numa_node_[core] << " |";
    }
    stream << std::endl;

    auto print_cores = [&stream](const std::string& title, const auto& cores) {
        stream << "  " << title << ": ";
        if (cores.empty()) {
            stream << "None";
        } else {
            bool is_first = true;
            for (const int core : cores) {
                if (!is_first) {
                    stream << ",";
                }
                is_first = false;
                stream << core;
            }
        }
        stream << std::endl;
    };

    print_cores("Cores from CGroup CPUSET", cpuset_cores_);
    print_cores("Offline Cores", offline_cores_);

    return stream.str();
}

} // namespace starrocks
