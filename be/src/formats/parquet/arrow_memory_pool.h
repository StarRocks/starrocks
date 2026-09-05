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

    Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override;

    Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment, uint8_t** ptr) override;

    void Free(uint8_t* buffer, int64_t size, int64_t alignment) override;

    int64_t bytes_allocated() const override { return _stats.bytes_allocated(); }

    int64_t total_bytes_allocated() const override { return _stats.total_bytes_allocated(); }

    int64_t max_memory() const override { return -1; }

    int64_t num_allocations() const override { return _stats.num_allocations(); }

    std::string backend_name() const override { return "starrocks"; }

private:
    arrow::internal::MemoryPoolStats _stats;
};

} // namespace starrocks
