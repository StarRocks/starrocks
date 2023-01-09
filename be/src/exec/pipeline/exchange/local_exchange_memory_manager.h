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

#include "common/config.h"
#include "common/logging.h"

namespace starrocks::pipeline {
// Manage the memory usage for local exchange
class LocalExchangeMemoryManager {
public:
    LocalExchangeMemoryManager(size_t max_input_dop) {
        size_t res = max_input_dop * get_memory_limit_per_driver();
        const size_t MAX_MEM_LIMIT = 128 * 1024 * 1024 * 1024UL; // 128GB limit
        _max_memory_bytes = (res > MAX_MEM_LIMIT || res <= 0) ? MAX_MEM_LIMIT : res;
    }

    void update_memory_usage(size_t memory_bytes) { _memory_bytes += memory_bytes; }

    size_t get_memory_limit_per_driver() {
        size_t limit_bytes = 128 * 1024 * 1024UL; // 128 MB
        if (config::local_exchange_buffer_mem_limit_per_driver > 0) {
            limit_bytes = config::local_exchange_buffer_mem_limit_per_driver;
        } else {
            LOG(WARNING) << "invalid config::local_exchange_buffer_mem_limit_per_driver "
                         << config::local_exchange_buffer_mem_limit_per_driver;
        }
        return limit_bytes;
    }

    bool is_full() const { return _memory_bytes >= _max_memory_bytes; }

private:
    size_t _max_memory_bytes;
    std::atomic<size_t> _memory_bytes{0};
};
} // namespace starrocks::pipeline
