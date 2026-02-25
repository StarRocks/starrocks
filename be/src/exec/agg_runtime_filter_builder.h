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

#include <atomic>
#include <cstring>
#include <memory>
#include <type_traits>

#include "base/container/heap.h"
#include "base/hash/unaligned_access.h"
#include "base/string/slice.h"
#include "column/type_traits.h"
#include "common/status.h"
#include "exec/chunks_sorter_heap_sort.h"
#include "exprs/runtime_filter.h"
#include "exprs/runtime_filter_bank.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {
class Aggregator;
class HeapBuilder;

class AggInRuntimeFilterBuilder {
public:
    AggInRuntimeFilterBuilder(RuntimeFilterBuildDescriptor* build_desc, LogicalType type)
            : _build_desc(build_desc), _type(type) {}
    RuntimeFilter* build(Aggregator* aggretator, ObjectPool* pool);

private:
    RuntimeFilterBuildDescriptor* _build_desc;
    LogicalType _type{};
};

class AggInRuntimeFilterMerger {
public:
    AggInRuntimeFilterMerger(size_t dop) : _merged(dop), _target_filters(dop) {}
    bool merge(size_t sequence, RuntimeFilterBuildDescriptor* desc, RuntimeFilter* in_rf);
    bool always_true() const { return _always_true.load(std::memory_order_acquire); }
    RuntimeFilter* merged_runtime_filter() { return _target_filters[0]; }

private:
    std::atomic<size_t> _merged;
    std::vector<RuntimeFilter*> _target_filters;
    std::atomic<bool> _always_true = false;
};

class HeapBuilder {
public:
    virtual ~HeapBuilder() = default;
};

template <LogicalType Type, class Compare, typename Enable = void>
class THeapBuilder;

// Default heap builder for non-Slice types.
template <LogicalType Type, class Compare>
class THeapBuilder<Type, Compare, std::enable_if_t<!isSliceLT<Type>>> final : public HeapBuilder {
public:
    using T = RunTimeCppType<Type>;

    THeapBuilder(Compare comp) : _heap(std::move(comp)) {}

    const T& top() { return _heap.top(); }

    size_t size() { return _heap.size(); }

    bool empty() { return _heap.empty(); }

    void reserve(size_t reserve_sz) { _heap.reserve(reserve_sz); }

    void replace_top(T&& new_top) { _heap.replace_top(std::move(new_top)); }

    void push(T&& rowcur) { _heap.push(std::move(rowcur)); }

private:
    SortingHeap<T, std::vector<T>, Compare> _heap;
};

// A specialized heap builder for Slice-based types (TYPE_CHAR/TYPE_VARCHAR/TYPE_VARBINARY).
// It deep-copies Slice bytes into an internal MemPool, so the Slice stored in heap will never dangle.
template <LogicalType Type, class Compare>
class THeapBuilder<Type, Compare, std::enable_if_t<isSliceLT<Type>>> final : public HeapBuilder {
public:
    using T = RunTimeCppType<Type>; // Slice

    THeapBuilder(Compare comp) : _heap(std::move(comp)) {}

    const T& top() { return _heap.top(); }

    size_t size() { return _heap.size(); }

    bool empty() { return _heap.empty(); }

    void reserve(size_t reserve_sz) { _heap.reserve(reserve_sz); }

    void replace_top(T&& new_top) { _heap.replace_top(_copy_to_pool(new_top)); }

    void push(T&& rowcur) { _heap.push(_copy_to_pool(rowcur)); }

private:
    ALWAYS_INLINE T _copy_to_pool(const T& s) {
        if (s.size == 0) {
            return s;
        }
        // Persist bytes into MemPool; lifetime is bound to this heap builder.
        uint8_t* dst = _pool->allocate_with_reserve(s.size, SLICE_MEMEQUAL_OVERFLOW_PADDING);
        memcpy(dst, s.data, s.size);
        return Slice(reinterpret_cast<char*>(dst), s.size);
    }

    std::unique_ptr<MemPool> _pool = std::make_unique<MemPool>();
    SortingHeap<T, std::vector<T>, Compare> _heap;
};

class AggTopNRuntimeFilterBuilder {
public:
    AggTopNRuntimeFilterBuilder(RuntimeFilterBuildDescriptor* build_desc, LogicalType type)
            : _build_desc(build_desc), _type(type), _runtime_filter(nullptr), _heap_builder(nullptr) {}
    RuntimeFilter* build(Aggregator* aggretator, ObjectPool* pool);

    // update the topn runtime filter with the new group keys
    void update(const Columns& group_by_columns, const Filter& selection);

    RuntimeFilter* runtime_filter() { return _runtime_filter; }

private:
    RuntimeFilterBuildDescriptor* _build_desc;
    LogicalType _type{};

    // the topn runtime filter
    RuntimeFilter* _runtime_filter;
    // the heap builder for the topn runtime filter
    HeapBuilder* _heap_builder;
};

} // namespace starrocks