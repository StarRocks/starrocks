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

#include "base/memory/memory_allocator.h"

namespace starrocks::memory {

template <bool clear_memory>
class JemallocAllocator : public Allocator {
public:
    JemallocAllocator() = default;
    ~JemallocAllocator() override = default;

    void* alloc(size_t size, size_t alignment = 0) override;
    void* realloc(void* ptr, size_t old_size, size_t new_size, size_t alignment = 0) override;
    void free(void* ptr, size_t size) override;
    int64_t nallox(size_t size, int flags = 0) const override;

    Allocator::MemoryKind memory_kind() const override { return Allocator::MemoryKind::kJemalloc; }

    static constexpr bool throw_bad_alloc_on_failure() { return false; }
};

} // namespace starrocks::memory
