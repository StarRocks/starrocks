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

#include "arrow/memory_pool.h"
#include "common/compiler_util.h"
#include "glog/logging.h"

namespace starrocks {

class ArrowMemoryPool final : public arrow::MemoryPool {
public:
    using Status = arrow::Status;

    ~ArrowMemoryPool() override = default;

    /// Allocate a new memory region of at least size bytes.
    ///
    /// The allocated region shall be 64-byte aligned.
    Status Allocate(int64_t size, uint8_t** out) override;

    /// Resize an already allocated memory section.
    ///
    /// As by default most default allocators on a platform don't support aligned
    /// reallocation, this function can involve a copy of the underlying data.
    Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) override;

    /// Free an allocated region.
    ///
    /// @param buffer Pointer to the start of the allocated memory region
    /// @param size Allocated size located at buffer. An allocator implementation
    ///   may use this for tracking the amount of allocated bytes as well as for
    ///   faster deallocation if supported by its backend.
    void Free(uint8_t* buffer, int64_t size) override;

    /// The number of bytes that were allocated and not yet free'd through
    /// this allocator.
    int64_t bytes_allocated() const override { return _bytes_allocated.load(); }

    /// Return peak memory allocation in this memory pool
    ///
    /// \return Maximum bytes allocated. If not known (or not implemented),
    /// returns -1
    int64_t max_memory() const override { return -1; }

    /// The name of the backend used by this MemoryPool (e.g. "system" or "jemalloc").
    std::string backend_name() const override { return "starrocks"; }

private:
    std::atomic_int64_t _bytes_allocated{0};
};

} // namespace starrocks
