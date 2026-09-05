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

#include "base/memory/counting_allocator.h"
#include "base/memory/jemalloc_allocator.h"
#include "base/memory/malloc_allocator.h"
#include "base/memory/memory_allocator.h"

namespace starrocks::memory {

template <class BaseAllocator>
class TrackedAllocator : public BaseAllocator {
public:
    TrackedAllocator() = default;
    ~TrackedAllocator() override = default;

    void* alloc(size_t size, size_t alignment = 0) override;
    void* realloc(void* ptr, size_t old_size, size_t new_size, size_t alignment = 0) override;
    void free(void* ptr, size_t size) override;
    int64_t nallox(size_t size, int flags = 0) const override;

    Allocator::MemoryKind memory_kind() const override { return BaseAllocator::memory_kind(); }

    static constexpr bool throw_bad_alloc_on_failure() { return true; }
};

#if !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && !defined(THREAD_SANITIZER)
extern TrackedAllocator<JemallocAllocator<false>> kDefaultAllocator;
#else
extern TrackedAllocator<MallocAllocator<false>> kDefaultAllocator;
#endif

} // namespace starrocks::memory
