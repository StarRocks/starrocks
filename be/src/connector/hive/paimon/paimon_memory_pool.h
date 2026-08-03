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

#include <paimon/memory/memory_pool.h>

#include <cstddef>
#include <cstdint>
#include <memory>

namespace starrocks {

class MemTracker;

class TrackedPaimonMemoryPool final : public paimon::MemoryPool {
public:
    explicit TrackedPaimonMemoryPool(MemTracker* tracker);

    void* Malloc(uint64_t size, uint64_t alignment = 0) override;
    void* Realloc(void* pointer, size_t old_size, size_t new_size, uint64_t alignment = 0) override;
    void Free(void* pointer, uint64_t size) override;
    void Free(void* pointer, uint64_t size, uint64_t alignment) override;

    uint64_t CurrentUsage() const override;
    uint64_t MaxMemoryUsage() const override;

private:
    MemTracker* _tracker;
    std::shared_ptr<paimon::MemoryPool> _delegate;
};

} // namespace starrocks
