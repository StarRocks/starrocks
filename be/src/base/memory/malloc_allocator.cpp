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

#include "base/memory/malloc_allocator.h"

#include <cstdlib>
#include <cstring>

#include "base/compiler_util.h"
#include "gutil/strings/fastmem.h"

namespace starrocks::memory {

template <bool clear_memory>
void* MallocAllocator<clear_memory>::alloc(size_t size, size_t alignment) {
    void* ret = nullptr;
    if (alignment <= MALLOC_MIN_ALIGNMENT) {
        if constexpr (clear_memory) {
            ret = std::calloc(size, 1);
        } else {
            ret = std::malloc(size);
        }
    } else {
        int res = posix_memalign(&ret, alignment, size);
        if (UNLIKELY(res != 0)) {
            return nullptr;
        }
        if constexpr (clear_memory) {
            std::memset(ret, 0, size);
        }
    }
    return ret;
}

template <bool clear_memory>
void* MallocAllocator<clear_memory>::realloc(void* ptr, size_t old_size, size_t new_size, size_t alignment) {
    if (old_size == new_size) {
        return ptr;
    }

    void* ret = nullptr;
    if (alignment <= MALLOC_MIN_ALIGNMENT) {
        ret = std::realloc(ptr, new_size);
        if (UNLIKELY(ret == nullptr)) {
            return nullptr;
        }

        if constexpr (clear_memory) {
            if (new_size > old_size) {
                std::memset(static_cast<char*>(ret) + old_size, 0, new_size - old_size);
            }
        }
        return ret;
    }

    ret = MallocAllocator<clear_memory>::alloc(new_size, alignment);
    if (UNLIKELY(ret == nullptr)) {
        return nullptr;
    }
    strings::memcpy_inlined(ret, ptr, old_size);
    std::free(ptr);
    return ret;
}

template <bool clear_memory>
void MallocAllocator<clear_memory>::free(void* ptr, size_t size) {
    if (UNLIKELY(ptr == nullptr)) {
        return;
    }
    std::free(ptr);
}

template <bool clear_memory>
int64_t MallocAllocator<clear_memory>::nallox(size_t size, int flags) const {
    (void)flags;
    return static_cast<int64_t>(size);
}

template class MallocAllocator<false>;
template class MallocAllocator<true>;

} // namespace starrocks::memory
