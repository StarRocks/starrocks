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

#include "runtime/mem_pool.h"
#include "types/type_info_allocator.h"

namespace starrocks {

inline uint8_t* type_info_allocate_from_mem_pool(void* ctx, size_t size) {
    auto* mem_pool = static_cast<MemPool*>(ctx);
    return mem_pool->allocate(size);
}

inline TypeInfoAllocator make_type_info_allocator(MemPool* mem_pool) {
    return TypeInfoAllocator{mem_pool, type_info_allocate_from_mem_pool};
}

} // namespace starrocks
