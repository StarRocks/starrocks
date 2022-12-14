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

#include "util/phmap/phmap.h"

#include <gtest/gtest.h>

#include "runtime/mem_pool.h"
#include "testutil/parallel_test.h"

namespace starrocks {
class PhmapTest : public testing::Test {};

static bool tls_alloc_succ = true;

template <class T, int Spec = 1>
class CheckedAlloc {
public:
    using value_type = T;

    CheckedAlloc() = default;
    CheckedAlloc(const CheckedAlloc&) = default;
    CheckedAlloc& operator=(const CheckedAlloc&) = default;

    template <class U>
    CheckedAlloc(const CheckedAlloc<U, Spec>& that) {}

    template <class U>
    struct rebind {
        using other = CheckedAlloc<U, Spec>;
    };

    T* allocate(size_t n) {
        if (!tls_alloc_succ) {
            throw std::bad_alloc();
        }
        T* ptr = std::allocator<T>().allocate(n);
        return ptr;
    }
    void deallocate(T* ptr, size_t n) {
        memset(ptr, 0, n * sizeof(T)); // The freed memory must be unpoisoned.
        return std::allocator<T>().deallocate(ptr, n);
    }

    void swap(CheckedAlloc& that) { using std::swap; }
};

PARALLEL_TEST(PhmapTest, resize_fail) {
    using phmap_hash = phmap::priv::hash_default_hash<int32_t>;
    using phmap_equal = phmap::priv::hash_default_eq<int32_t>;
    using phmap_alloc = CheckedAlloc<int32_t>;
    phmap::flat_hash_map<int32_t, int32_t, phmap_hash, phmap_equal, phmap_alloc> map;

    size_t i;
    tls_alloc_succ = true;
    try {
        for (i = 0; i < 1000; i++) {
            if (i == 100) {
                tls_alloc_succ = false;
            }
            map.lazy_emplace(i, [&](const auto& ctor) { ctor(i, i); });
        }
    } catch (std::bad_alloc const&) {
    }

    size_t j = 0;
    auto iter = map.begin();
    auto end = map.end();
    while (iter != end) {
        j++;
        iter++;
    }
    ASSERT_EQ(i, j);
}

struct Value {
public:
    Value(int i) {
        _ptr = new int;
        *_ptr = i;
    }

    ~Value() { delete _ptr; }

private:
    int* _ptr = nullptr;
};

PARALLEL_TEST(PhmapTest, lazy_emplace_fail) {
    using phmap_hash = phmap::priv::hash_default_hash<int32_t>;
    using phmap_equal = phmap::priv::hash_default_eq<int32_t>;
    using phmap_alloc = CheckedAlloc<int32_t>;

    phmap::flat_hash_map<int32_t, uint8_t*, phmap_hash, phmap_equal, phmap_alloc> map;
    MemPool mem_pool;

    size_t i;
    tls_alloc_succ = true;
    try {
        for (i = 0; i < 1000; i++) {
            if (i == 100) {
                tls_alloc_succ = false;
            }
            map.lazy_emplace(i, [&](const auto& ctor) {
                if (!tls_alloc_succ) {
                    throw std::bad_alloc();
                }
                uint8_t* ptr = mem_pool.allocate(sizeof(Value));
                new (ptr) Value(i);
                ctor(i, ptr);
            });
        }
    } catch (std::bad_alloc const&) {
    }

    size_t j = 0;
    auto iter = map.begin();
    auto end = map.end();
    while (iter != end) {
        j++;
        (*reinterpret_cast<Value*>(iter->second)).~Value();
        iter++;
    }
    ASSERT_EQ(i, j);
}

} // namespace starrocks
