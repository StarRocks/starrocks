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

#include "common/config_primary_key_fwd.h"

namespace starrocks::lake {

inline size_t get_pk_index_parallel_execution_min_rows() {
    constexpr size_t kDefaultMinRowsPerTask = 16384;
    const int64_t configured_min_rows = config::pk_index_parallel_execution_min_rows;
    return configured_min_rows > 0 ? static_cast<size_t>(configured_min_rows) : kDefaultMinRowsPerTask;
}

} // namespace starrocks::lake
