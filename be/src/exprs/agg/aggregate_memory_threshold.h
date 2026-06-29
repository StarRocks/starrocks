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

#include <cstddef>

#include "common/system/cpu_info.h"

namespace starrocks::agg {

// Threshold at which a single-level hash set/map is converted to two-level.
// Sized to the detected L3 cache (falling back to L2, then a default) so the
// conversion tracks the last-level cache instead of a fixed assumption. Debug
// builds use a tiny value so tests exercise the two-level path cheaply.
inline size_t two_level_memory_threshold() {
#ifdef NDEBUG
    return CpuInfo::get_l3_cache_size();
#else
    return 64;
#endif
}

} // namespace starrocks::agg
