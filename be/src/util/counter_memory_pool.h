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
#include <memory>
#include <vector>

namespace starrocks {

// this pool is used to make same level's counter is near to each other
// so when merge profile, the breadth-first traversal algorithm can have less cache miss
class CounterMemoryPool {
public:
    static constexpr size_t BLOCK_SIZE = 4096; // 1KB
    static constexpr size_t ALIGNMENT = 8;
    CounterMemoryPool() = default;
    ~CounterMemoryPool() = default;

    void* allocate(size_t size);

private:
    void allocate_new_block();

    std::vector<std::unique_ptr<char[]>> _blocks;
    size_t _current_block_offset = 0;
    char* _current_block = nullptr;
};

} // namespace starrocks