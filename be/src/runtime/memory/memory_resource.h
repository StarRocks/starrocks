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

#include <cstdint>
#include <memory_resource>

#include "gutil/dynamic_annotations.h"

namespace starrocks {
// memory resource provide for PMR
//
// usage:
// char buffer[1024];
// stack_memory_resource mr(buffer, sizeof(buffer));
// std::pmr::vector<int> vec(&mr);
// vec.resize(1024/sizeof(int)); // no any heap allocate
// vec.resize(8192); // allocate from heap
class stack_memory_resource final : public std::pmr::memory_resource {
public:
    stack_memory_resource(void* addr, size_t sz) : stack_memory_resource(addr, sz, std::pmr::get_default_resource()) {}
    stack_memory_resource(void* addr, size_t sz, std::pmr::memory_resource* upstream)
            : _stack_addr_start(addr),
              _stack_addr_end((uint8_t*)_stack_addr_start + sz),
              _current_addr(_stack_addr_start),
              _upstream(upstream) {
        ASAN_POISON_MEMORY_REGION(_stack_addr_start, sz);
    }
    ~stack_memory_resource() override {
        size_t sz = (uint8_t*)_stack_addr_end - (uint8_t*)_stack_addr_start;
        ASAN_UNPOISON_MEMORY_REGION(_stack_addr_start, sz);
    }

private:
    void* _stack_addr_start;
    void* _stack_addr_end;
    void* _current_addr;
    std::pmr::memory_resource* const _upstream;

    void* do_allocate(size_t __bytes, size_t __alignment) override {
        size_t left = (uint8_t*)_stack_addr_end - (uint8_t*)_current_addr;
        void* res = std::align(__alignment, __bytes, _current_addr, left);
        if (res == nullptr) {
            return _upstream->allocate(__bytes, __alignment);
        }
        ASAN_UNPOISON_MEMORY_REGION(res, __bytes);
        _current_addr = (uint8_t*)_current_addr + __bytes;

        return res;
    }

    void do_deallocate(void* __p, size_t __bytes, size_t __alignment) override {
        if (__p >= _stack_addr_start && __p <= _stack_addr_end) {
            // nothing todo
            ASAN_POISON_MEMORY_REGION(__p, __bytes);
        } else {
            return _upstream->deallocate(__p, __bytes, __alignment);
        }
    }

    bool do_is_equal(const memory_resource& __other) const noexcept override { return this == &__other; }
};

} // namespace starrocks
