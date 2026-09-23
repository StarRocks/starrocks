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
#include <cstdint>

namespace starrocks {

int64_t set_large_memory_alloc_failure_threshold(int64_t);

// Whether an allocation of `size` bytes must be reported as a large allocation.
// `threshold` comes from config::large_memory_alloc_report_threshold; a value of 0 or below
// disables reporting. Zero is also what the config global holds before config::init() applies
// the declared default, so allocations made during static initialization are never reported.
// Kept inline because every allocation goes through it.
constexpr bool should_report_large_memory_alloc(size_t size, int64_t threshold) {
    return threshold > 0 && size > static_cast<size_t>(threshold);
}

} // namespace starrocks
