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

#include "common/system/cpu_info.h"

namespace starrocks {

const std::vector<CpuInfo::FlagMapping>& CpuInfo::_flag_mappings() {
    static const std::vector<FlagMapping> mappings = {
            {"ssse3", CpuInfo::SSSE3},     {"sse4_1", CpuInfo::SSE4_1},     {"sse4_2", CpuInfo::SSE4_2},
            {"popcnt", CpuInfo::POPCNT},   {"avx", CpuInfo::AVX},           {"avx2", CpuInfo::AVX2},
            {"avx512f", CpuInfo::AVX512F}, {"avx512bw", CpuInfo::AVX512BW},
    };
    return mappings;
}

int64_t CpuInfo::_detect_hardware_flags(const std::string& cpuinfo_content) {
    return _parse_procfs_flags(cpuinfo_content);
}

int64_t CpuInfo::_compiled_hardware_flags() {
    int64_t flags = 0;
#if defined(__x86_64__)
#if defined(__SSSE3__)
    flags |= SSSE3;
#endif
#if defined(__SSE4_1__)
    flags |= SSE4_1;
#endif
#if defined(__SSE4_2__)
    flags |= SSE4_2;
#endif
#if defined(__AVX2__)
    flags |= AVX2;
#endif
#if defined(__AVX512F__)
    flags |= AVX512F;
#endif
#if defined(__AVX512BW__)
    flags |= AVX512BW;
#endif
#endif
    return flags;
}

} // namespace starrocks
