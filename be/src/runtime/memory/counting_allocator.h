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

#include <iostream>
#include <memory>

#include "exprs/expr_context.h"

namespace starrocks {

template <class T>
class CountingAllocator {
public:
    typedef T value_type;
    typedef size_t size_type;
    using propagate_on_container_copy_assignment = std::true_type; // for consistency
    using propagate_on_container_move_assignment = std::true_type; // to avoid the pessimization
    using propagate_on_container_swap = std::true_type;            // to avoid the undefined behavior

    template <typename U>
    struct rebind {
        using other = CountingAllocator<U>;
    };
    CountingAllocator() = default;
    explicit CountingAllocator(int64_t* counter) : _counter(counter) {}
    explicit CountingAllocator(const CountingAllocator& rhs) : _counter(rhs._counter) {}
    template <class U>
    CountingAllocator(const CountingAllocator<U>& other) : _counter(other._counter) {}

    ~CountingAllocator() = default;

    CountingAllocator(CountingAllocator&& rhs) noexcept { std::swap(_counter, rhs._counter); }

    CountingAllocator& operator=(CountingAllocator&& rhs) noexcept {
        if (this != &rhs) {
            std::swap(_counter, rhs._counter);
        }
        return *this;
    }

    T* allocate(size_t n) {
        DCHECK(_counter != nullptr);
        T* result = static_cast<T*>(malloc(n * sizeof(T)));
        *_counter += (result != nullptr) ? n * sizeof(T) : 0;
        return result;
    }

    void deallocate(T* ptr, size_t n) {
        DCHECK(_counter != nullptr);
        free(ptr);
        *_counter -= n * sizeof(T);
    }

    CountingAllocator& operator=(const CountingAllocator& rhs) {
        _counter = rhs._counter;
        return *this;
    }

    template <class U>
    CountingAllocator& operator=(const CountingAllocator<U>& rhs) {
        _counter = rhs._counter;
        return *this;
    }

    bool operator==(const CountingAllocator& rhs) const { return _counter == rhs._counter; }

    bool operator!=(const CountingAllocator& rhs) const { return !(*this == rhs); }

    // CountingAllocator doesn't support swap
    void swap(CountingAllocator& rhs) {}

    int64_t* _counter = nullptr;
};
template <class T>
void swap(CountingAllocator<T>& lhs, CountingAllocator<T>& rhs) {
    lhs.swap(rhs);
}

} // namespace starrocks