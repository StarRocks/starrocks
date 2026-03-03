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

namespace starrocks::memory {

static constexpr size_t MALLOC_MIN_ALIGNMENT = 8;

class Allocator {
public:
    enum class MemoryKind {
        kJemalloc,
        kMalloc,
    };

    virtual ~Allocator() = default;
    virtual void* alloc(size_t size, size_t alignment = 0) = 0;
    virtual void* realloc(void* ptr, size_t old_size, size_t new_size, size_t alignment = 0) = 0;
    virtual void free(void* ptr, size_t size) = 0;
    virtual int64_t nallox(size_t size, int flags = 0) const = 0;

    virtual bool transfer_to(Allocator* target, void* ptr, size_t size) { return false; }
    virtual MemoryKind memory_kind() const = 0;
};

template <class Alloc>
class AllocHolder : private Alloc {
public:
    AllocHolder() = default;
    explicit AllocHolder(Alloc* alloc) : Alloc(*alloc) {}
    ~AllocHolder() override = default;

    Alloc* get_allocator() { return this; }
    const Alloc* get_allocator() const { return this; }
};

} // namespace starrocks::memory
