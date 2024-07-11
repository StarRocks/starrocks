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

#include "arrow_memory_pool.h"

namespace starrocks {

// https://arrow.apache.org/docs/format/Columnar.html#buffer-alignment-and-padding
static constexpr int kDefaultBufferAlignment = 64;
static constexpr int64_t kDebugXorSuffix = -0x181fe80e0b464188LL;
alignas(kDefaultBufferAlignment) int64_t zero_size_area[1] = {kDebugXorSuffix};
static uint8_t* const kZeroSizeArea = reinterpret_cast<uint8_t*>(&zero_size_area);

ArrowMemoryPool::Status ArrowMemoryPool::Allocate(int64_t size, int64_t alignment, uint8_t** out) {
    if (size == 0) {
        *out = kZeroSizeArea;
    }
    // On Linux (and other systems), posix_memalign() does not modify memptr on failure.
    if (posix_memalign(reinterpret_cast<void**>(out), alignment, size)) {
        return Status::OutOfMemory("malloc of size ", size, " failed");
    }
    _stats.DidAllocateBytes(size);
    return Status::OK();
}

ArrowMemoryPool::Status ArrowMemoryPool::Reallocate(int64_t old_size, int64_t new_size, int64_t alignment,
                                                    uint8_t** ptr) {
    uint8_t* previous_ptr = *ptr;
    if (previous_ptr == kZeroSizeArea) {
        DCHECK_EQ(old_size, 0);
        return Allocate(new_size, alignment, ptr);
    }
    if (new_size == 0) {
        Free(*ptr, old_size, alignment);
        *ptr = kZeroSizeArea;
        return Status::OK();
    }

    // Allocate new chunk
    uint8_t* out = nullptr;
    RETURN_NOT_OK(Allocate(new_size, alignment, &out));
    DCHECK(out);
    // Copy contents and release old memory chunk
    memcpy(out, *ptr, static_cast<size_t>(std::min(new_size, old_size)));
    Free(*ptr, old_size, alignment);
    *ptr = out;
    return Status::OK();
}

void ArrowMemoryPool::Free(uint8_t* buffer, int64_t size, int64_t /*alignment*/) {
    if (buffer == kZeroSizeArea) {
        DCHECK_EQ(size, 0);
        return;
    }
    std::free(buffer);
    _stats.DidFreeBytes(size);
}

} // namespace starrocks
