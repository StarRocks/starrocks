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

#include "util/counter_memory_pool.h"

#include <cassert>
#include <cstdint>

namespace starrocks {

void* CounterMemoryPool::allocate(size_t size) {
    size = (size + ALIGNMENT - 1) & ~(ALIGNMENT - 1);

    if (_current_block == nullptr || _current_block_offset + size > BLOCK_SIZE) {
        allocate_new_block();
    }

    void* ptr = _current_block + _current_block_offset;
    _current_block_offset += size;
    return ptr;
}

void CounterMemoryPool::allocate_new_block() {
    auto new_block = std::make_unique<char[]>(BLOCK_SIZE);
    _current_block = new_block.get();
    _blocks.push_back(std::move(new_block));
    _current_block_offset = 0;
}

} // namespace starrocks