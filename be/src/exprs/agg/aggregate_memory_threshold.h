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

#include <algorithm>
#include <cstddef>

#include "common/system/cpu_info.h"

namespace starrocks::agg {

inline constexpr size_t kMinTwoLevelMemoryThreshold = 4ULL * 1024 * 1024;
inline constexpr size_t kMaxPerCoreTwoLevelMemoryThreshold = 16ULL * 1024 * 1024;

inline constexpr size_t compute_two_level_memory_threshold(long l2_bytes, long l3_bytes, int num_cores) {
    const size_t l2_floor = (l2_bytes > 0) ? static_cast<size_t>(l2_bytes) * 4 : kMinTwoLevelMemoryThreshold;
    const size_t effective_floor =
            std::clamp(l2_floor, kMinTwoLevelMemoryThreshold, kMaxPerCoreTwoLevelMemoryThreshold);
    if (l3_bytes <= 0 || l3_bytes <= l2_bytes) {
        return effective_floor;
    }
    const size_t cores = static_cast<size_t>(std::max(1, num_cores));
    const size_t per_core_l3 = static_cast<size_t>(l3_bytes) / cores;
    return std::clamp(per_core_l3, effective_floor, kMaxPerCoreTwoLevelMemoryThreshold);
}

// Threshold at which a single-level hash set/map is converted to two-level.
// Sized to the per-core share of the detected L3 cache (clamped between a 4x L2
// floor of at least 4 MiB and a 16 MiB ceiling) so concurrent pipeline drivers
// do not collectively thrash a shared socket L3. Debug builds use a tiny value
// so tests exercise the two-level path cheaply.
inline size_t two_level_memory_threshold() {
#ifdef NDEBUG
    static const size_t threshold = compute_two_level_memory_threshold(
            CpuInfo::get_l2_cache_size(), CpuInfo::get_l3_cache_size(), CpuInfo::num_cores());
    return threshold;
#else
    return 64;
#endif
}

} // namespace starrocks::agg
