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

#include "compute_env/arrow_io_thread_pool.h"

#include <arrow/io/interfaces.h>
#include <fmt/format.h>

#include <cstdlib>

#include "common/logging.h"

namespace starrocks {

int arrow_io_thread_pool_default_capacity() {
    // Arrow creates the pool on first use and reads ARROW_IO_THREADS only then.
    static const int capacity = arrow::io::GetIOThreadPoolCapacity();
    return capacity;
}

Status apply_arrow_io_thread_pool_size(int32_t size) {
    if (size < 0 || size > kMaxArrowIOThreadPoolSize) {
        return Status::InvalidArgument(
                fmt::format("arrow_io_thread_pool_size must be in [0, {}], got {}", kMaxArrowIOThreadPoolSize, size));
    }
    // Read before the first resize below, so it still holds the capacity Arrow chose.
    const int default_capacity = arrow_io_thread_pool_default_capacity();
    const int capacity = size > 0 ? size : default_capacity;
    const int old_capacity = arrow::io::GetIOThreadPoolCapacity();
    if (capacity != old_capacity) {
        auto st = arrow::io::SetIOThreadPoolCapacity(capacity);
        if (!st.ok()) {
            return Status::InternalError(
                    fmt::format("failed to set arrow io thread pool capacity to {}: {}", capacity, st.ToString()));
        }
    }
    const char* env = std::getenv("ARROW_IO_THREADS");
    LOG(INFO) << "arrow io thread pool capacity: " << old_capacity << " -> " << capacity
              << " (arrow_io_thread_pool_size=" << size << ", ARROW_IO_THREADS=" << (env != nullptr ? env : "unset")
              << ")";
    return Status::OK();
}

} // namespace starrocks
