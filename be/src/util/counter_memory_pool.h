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

// 一个简单的内存池实现，用于分配和管理 counter 对象
// 每个内存段大小为 1KB，并且内存对齐
class CounterMemoryPool {
public:
    static constexpr size_t BLOCK_SIZE = 4096; // 1KB
    static constexpr size_t ALIGNMENT = 8;     // 8字节对齐

    CounterMemoryPool() = default;
    ~CounterMemoryPool() = default;

    // 分配指定大小的内存
    void* allocate(size_t size);

private:
    // 分配一个新的内存块
    void allocate_new_block();

    // 当前内存块
    std::vector<std::unique_ptr<char[]>> _blocks;
    size_t _current_block_offset = 0;
    char* _current_block = nullptr;
};

} // namespace starrocks