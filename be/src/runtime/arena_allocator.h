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

#include <memory_resource>

#include "runtime/mem_pool.h"

namespace starrocks {

// std::pmr::memory_resource backed by a MemPool.
//
// Allocations are forwarded to MemPool::allocate_aligned().
// Deallocations are no-ops — memory is freed in bulk when the MemPool
// is destroyed or cleared.
//
// Usage:
//   MemPool pool;
//   MemPoolResource mr(&pool);
//   std::pmr::vector<int> v(&mr);
//   v.push_back(42);  // allocated from pool
//   std::pmr::string s("hello", &mr);  // also from pool
//
class MemPoolResource final : public std::pmr::memory_resource {
public:
    explicit MemPoolResource(MemPool* pool) noexcept : _pool(pool) {}

    MemPool* pool() const noexcept { return _pool; }

private:
    void* do_allocate(size_t bytes, size_t alignment) override { return _pool->allocate_aligned(bytes, alignment); }

    void do_deallocate(void* /*p*/, size_t /*bytes*/, size_t /*alignment*/) override {
        // No-op: MemPool owns all memory and frees it in bulk.
    }

    bool do_is_equal(const memory_resource& other) const noexcept override { return this == &other; }

    MemPool* _pool;
};

} // namespace starrocks
