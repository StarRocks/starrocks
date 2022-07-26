// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <cstdint>
#include <memory_resource>

namespace starrocks {
// memory resource provide for PMR
//
// usage:
// char buffer[4096];
// stack_memory_resource mr(buffer, sizeof(buffer));
// std::pmr::vector<int> vec;
// vec.resize(4096); // no any heap allocate
// vec.resize(8192); // allocate from heap
class stack_memory_resource : public std::pmr::memory_resource {
public:
    stack_memory_resource(void* addr, size_t sz) : stack_memory_resource(addr, sz, std::pmr::get_default_resource()) {}
    stack_memory_resource(void* addr, size_t sz, std::pmr::memory_resource* upstream)
            : _stack_addr_start(addr),
              _stack_addr_end((uint8_t*)_stack_addr_start + sz),
              _current_addr(_stack_addr_start),
              _upstream(upstream) {}

private:
    void* _stack_addr_start;
    void* _stack_addr_end;
    void* _current_addr;
    std::pmr::memory_resource* const _upstream;

    virtual void* do_allocate(size_t __bytes, size_t __alignment) {
        size_t left = (uint8_t*)_stack_addr_end - (uint8_t*)_current_addr;
        void* res = std::align(__alignment, __bytes, _current_addr, left);
        if (res == nullptr) {
            return _upstream->allocate(__bytes, __alignment);
        }
        _current_addr = (uint8_t*)_current_addr + __bytes;

        return res;
    }

    virtual void do_deallocate(void* __p, size_t __bytes, size_t __alignment) {
        if (__p >= _stack_addr_start && __p <= _stack_addr_end) {
            // nothing todo
        } else {
            return _upstream->deallocate(__p, __bytes, __alignment);
        }
    }

    virtual bool do_is_equal(const memory_resource& __other) const noexcept { return this == &__other; }
};

} // namespace starrocks