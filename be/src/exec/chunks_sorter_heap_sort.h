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

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <functional>
#include <memory>

#include "base/container/heap.h"
#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "exec/chunks_sorter.h"
#include "exprs/expr_context.h"
#include "exprs/runtime_filter.h"
#include "glog/logging.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {

namespace detail {
using DataSegmentPtr = std::shared_ptr<DataSegment>;
// this class will be delete by self
// so we made desctuctor private to avoid alloc memory in stack
class ChunkHolder {
public:
    ChunkHolder(DataSegmentPtr&& ref_value) : _ref_val(std::move(ref_value)) {}
    ChunkHolder(const ChunkHolder&) = delete;
    void unref() noexcept {
        DCHECK_GT(_ref_count, 0);
        _ref_count--;
        if (_ref_count == 0) {
            delete this;
        }
    }
    void ref() noexcept { _ref_count++; }

    int ref_count() const { return _ref_count; }

    DataSegmentPtr& value() { return _ref_val; }

private:
    ~ChunkHolder() noexcept = default;
    int _ref_count{0};
    DataSegmentPtr _ref_val;
};

struct ChunkRowCursor {
public:
    ChunkRowCursor(int row_id, ChunkHolder* holder) : _row_id(row_id), _holder(holder) { _holder->ref(); }
    ChunkRowCursor(const ChunkRowCursor& other) {
        _row_id = other._row_id;
        _holder = other._holder;
        _holder->ref();
    }

    ChunkRowCursor& operator=(const ChunkRowCursor& other) {
        if (_holder) {
            _holder->unref();
        }
        _row_id = other._row_id;
        _holder = other._holder;
        _holder->ref();
        return *this;
    }

    ChunkRowCursor(ChunkRowCursor&& other) noexcept {
        _row_id = other._row_id;
        _holder = other._holder;
        other._holder = nullptr;
    }

    ChunkRowCursor& operator=(ChunkRowCursor&& other) noexcept {
        std::swap(_row_id, other._row_id);
        std::swap(_holder, other._holder);
        return *this;
    }

    ~ChunkRowCursor() noexcept {
        if (_holder) {
            _holder->unref();
        }
    }
    int row_id() const { return _row_id; }

    int ref_count() const { return _holder->ref_count(); }

    const DataSegmentPtr& data_segment() const { return _holder->value(); }

private:
    size_t _row_id;
    ChunkHolder* _holder;
};

struct ChunkCursorComparator {
    ChunkCursorComparator(const SortDescs& sort_desc) : _sort_desc(sort_desc) {}

    bool operator()(const ChunkRowCursor& lhs, const ChunkRowCursor& rhs) const {
        size_t l_row_id = lhs.row_id();
        size_t r_row_id = rhs.row_id();
        int order_by_columns_sz = lhs.data_segment()->order_by_columns.size();
        for (int i = 0; i < order_by_columns_sz; ++i) {
            int null_first = _sort_desc.get_column_desc(i).null_first;
            int sort_order = _sort_desc.get_column_desc(i).sort_order;
            int res = lhs.data_segment()->order_by_columns[i]->compare_at(
                    l_row_id, r_row_id, *rhs.data_segment()->order_by_columns[i].get(), null_first);
            if (res != 0) return res * sort_order < 0;
        }

        return false;
    }

private:
    // 1 or -1
    const SortDescs& _sort_desc;
};
} // namespace detail

class ChunksSorterHeapSort final : public ChunksSorter {
public:
    using DataSegmentPtr = std::shared_ptr<DataSegment>;
    ChunksSorterHeapSort(RuntimeState* state, const std::vector<ExprContext*>* sort_exprs,
                         const std::vector<bool>* is_asc_order, const std::vector<bool>* is_null_first,
                         const std::string& sort_keys, size_t offset, size_t limit)
            : ChunksSorter(state, sort_exprs, is_asc_order, is_null_first, sort_keys, true),
              _offset(offset),
              _limit(limit) {}
    ~ChunksSorterHeapSort() override = default;

    Status update(RuntimeState* state, const ChunkPtr& chunk) override;
    Status do_done(RuntimeState* state) override;
    Status get_next(ChunkPtr* chunk, bool* eos) override;
    std::vector<RuntimeFilter*>* runtime_filters(ObjectPool* pool) override;
    int64_t mem_usage() const override {
        if (_sort_heap == nullptr || _sort_heap->empty()) {
            return 0;
        }
        int first_rows = _sort_heap->top().data_segment()->chunk->num_rows();
        return _sort_heap->size() * _sort_heap->top().data_segment()->mem_usage() / first_rows;
    }

    size_t get_output_rows() const override;

    void setup_runtime(RuntimeState* state, RuntimeProfile* profile, MemTracker* parent_mem_tracker) override;

private:
    size_t _number_of_rows_to_sort() const { return _offset + _limit; }

    // For TOPN cases, we can filter out a very large amount of data with
    // the elements at the top of the heap, which will significantly improve the sorting performance
    int _filter_data(detail::ChunkHolder* chunk_holder, int row_sz);

    template <LogicalType TYPE>
    void _do_filter_data_for_type(detail::ChunkHolder* chunk_holder, Filter* filter, int row_sz);

    std::vector<RuntimeFilter*> _runtime_filter;

    using CursorContainer = std::vector<detail::ChunkRowCursor>;
    using CommonCursorSortHeap = SortingHeap<detail::ChunkRowCursor, CursorContainer, detail::ChunkCursorComparator>;

    std::unique_ptr<CommonCursorSortHeap> _sort_heap = nullptr;
    std::function<void(detail::ChunkHolder*, Filter*, int)> _do_filter_data;

    const size_t _offset;
    const size_t _limit;
    size_t _next_output_row = 0;

    RuntimeProfile::Counter* _sort_filter_rows = nullptr;
    RuntimeProfile::Counter* _sort_filter_costs = nullptr;

    DataSegment _merged_segment;
};

} // namespace starrocks
