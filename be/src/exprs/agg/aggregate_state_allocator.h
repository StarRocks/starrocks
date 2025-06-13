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

#include "column/hash_set.h"
#include "common/config.h"
#include "runtime/memory/allocator.h"
#include "runtime/memory/roaring_hook.h"

namespace starrocks {

inline thread_local Allocator* tls_agg_state_allocator = nullptr;

template <class T>
class AggregateStateAllocator {
public:
    typedef T value_type;
    typedef size_t size_type;
    using propagate_on_container_copy_assignment = std::true_type; // for consistency
    using propagate_on_container_move_assignment = std::true_type; // to avoid the pessimization
    using propagate_on_container_swap = std::true_type;            // to avoid the undefined behavior

    template <typename U>
    struct rebind {
        using other = AggregateStateAllocator<U>;
    };
    AggregateStateAllocator() = default;
    template <class U>
    AggregateStateAllocator(const AggregateStateAllocator<U>& other) {}

    ~AggregateStateAllocator() = default;

    T* allocate(size_t n) {
        DCHECK(tls_agg_state_allocator != nullptr);
        return static_cast<T*>(tls_agg_state_allocator->checked_alloc(n * sizeof(T)));
    }

    void deallocate(T* ptr, size_t n) {
        DCHECK(tls_agg_state_allocator != nullptr);
        tls_agg_state_allocator->free(ptr);
    }

    AggregateStateAllocator& operator=(const AggregateStateAllocator& rhs) = default;

    template <class U>
    AggregateStateAllocator& operator=(const AggregateStateAllocator<U>& rhs) {
        return *this;
    }

    bool operator==(const AggregateStateAllocator& rhs) const { return true; }

    bool operator!=(const AggregateStateAllocator& rhs) const { return false; }

    void swap(AggregateStateAllocator& rhs) {}
};
template <class T>
void swap(AggregateStateAllocator<T>& lhs, AggregateStateAllocator<T>& rhs) {
    lhs.swap(rhs);
}

class ThreadLocalAggregateStateAllocatorSetter {
public:
    ThreadLocalAggregateStateAllocatorSetter(Allocator* allocator) {
        _prev = tls_agg_state_allocator;
        tls_agg_state_allocator = allocator;
    }
    ~ThreadLocalAggregateStateAllocatorSetter() { tls_agg_state_allocator = _prev; }

private:
    Allocator* _prev = nullptr;
};
#define SCOPED_THREAD_LOCAL_AGG_STATE_ALLOCATOR_SETTER(allocator) \
    auto VARNAME_LINENUM(alloc_setter) = ThreadLocalAggregateStateAllocatorSetter(allocator)

template <typename T>
using HashSetWithAggStateAllocator =
        phmap::flat_hash_set<T, StdHash<T>, phmap::priv::hash_default_eq<T>, AggregateStateAllocator<T>>;

using SliceHashSetWithAggStateAllocator = phmap::flat_hash_set<SliceWithHash, HashOnSliceWithHash, EqualOnSliceWithHash,
                                                               AggregateStateAllocator<SliceWithHash>>;
using SliceTwoLevelHashSetWithAggStateAllocator =
        phmap::parallel_flat_hash_set<SliceWithHash, HashOnSliceWithHash, EqualOnSliceWithHash,
                                      AggregateStateAllocator<SliceWithHash>>;

template <typename T>
using VectorWithAggStateAllocator = std::vector<T, AggregateStateAllocator<T>>;

// Thread local aggregate state allocator setter with roaring allocator
class ThreadLocalStateAllocatorSetter {
public:
    ThreadLocalStateAllocatorSetter(Allocator* allocator)
            : _agg_state_allocator_setter(allocator), _roaring_allocator_setter(allocator) {}
    ~ThreadLocalStateAllocatorSetter() = default;

private:
    ThreadLocalAggregateStateAllocatorSetter _agg_state_allocator_setter;
    ThreadLocalRoaringAllocatorSetter _roaring_allocator_setter;
};

#define SCOPED_THREAD_LOCAL_STATE_ALLOCATOR_SETTER(allocator) \
    auto VARNAME_LINENUM(alloc_setter) = ThreadLocalStateAllocatorSetter(allocator)

} // namespace starrocks
