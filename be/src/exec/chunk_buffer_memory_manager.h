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

#include <atomic>
#include <cstdint>
#include <limits>

#include "common/config.h"
#include "common/logging.h"

namespace starrocks::pipeline {
// Manage the memory usage for local exchange
class ChunkBufferMemoryManager {
public:
    ChunkBufferMemoryManager(size_t max_input_dop, int64_t per_driver_mem_limit,
                             int64_t max_buffered_rows = std::numeric_limits<int64_t>::max())
            : _max_input_dop(max_input_dop) {
        _max_buffered_rows = max_buffered_rows;
        if (per_driver_mem_limit > 0) {
            _max_memory_usage_per_driver = per_driver_mem_limit;
        } else {
            LOG(WARNING) << "invalid per_driver_mem_limit";
        }
        size_t res = max_input_dop * _max_memory_usage_per_driver;
        _max_memory_usage = (res > _max_memory_usage || res <= 0) ? _max_memory_usage : res;
    }

    void update_memory_usage(int64_t memory_usage, int64_t num_rows) {
        _memory_usage += memory_usage;
        _buffered_num_rows += num_rows;
    }

    size_t get_memory_limit_per_driver() const { return _max_memory_usage_per_driver; }

    int64_t get_memory_usage() const { return _memory_usage; }

    bool is_full() const { return _memory_usage >= _max_memory_usage || _buffered_num_rows > _max_buffered_rows; }

private:
    std::atomic<size_t> _max_memory_usage{128UL * 1024 * 1024 * 1024}; // 128GB
    size_t _max_memory_usage_per_driver = 128 * 1024 * 1024UL;         // 128MB
    size_t _max_buffered_rows{};
    std::atomic<int64_t> _memory_usage{};
    std::atomic<int64_t> _buffered_num_rows{};
    size_t _max_input_dop;
};
} // namespace starrocks::pipeline
