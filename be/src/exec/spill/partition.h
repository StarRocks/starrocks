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

#include <glog/logging.h>

#include <cstdint>

namespace starrocks {

// TODO: use clz instread of loop
inline int32_t partition_level(int32_t partition_id) {
    DCHECK_GT(partition_id, 0);
    int32_t level = 0;
    while (partition_id) {
        partition_id >>= 1;
        level++;
    }
    DCHECK_GE(level - 1, 0);
    return level - 1;
}

struct SpillPartitionInfo {
    SpillPartitionInfo(int32_t partition_id_) : partition_id(partition_id_), level(partition_level(partition_id_)) {}

    int32_t partition_id;
    int32_t level;
    size_t num_rows = 0;
    size_t mem_size = 0;
    size_t bytes = 0;
    bool in_mem = true;

    bool empty() const { return num_rows == 0; }

    int32_t level_elements() const { return 1 << level; }

    int32_t level_last_partition() const { return level_elements() * 2 - 1; }

    int32_t mask() const { return level_elements() - 1; }
};
} // namespace starrocks