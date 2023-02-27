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

#include "exec/chunks_sorter_heap_sort.h"

#include <functional>
#include <memory>
#include <vector>

#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "exec/sorting/merge.h"
#include "exprs/runtime_filter.h"
#include "glog/logging.h"
#include "gutil/casts.h"
#include "types/logical_type_infra.h"
#include "util/defer_op.h"

namespace starrocks {

Status ChunksSorterHeapSort::update(RuntimeState* state, const ChunkPtr& chunk) {
    ScopedTimer<MonotonicStopWatch> timer(_build_timer);

    if (chunk->is_empty() || _number_of_rows_to_sort() == 0) {
        return Status::OK();
    }

    // chunk_holder was shared ownership by itself
    auto* chunk_holder = new detail::ChunkHolder(std::make_shared<DataSegment>(_sort_exprs, chunk));
    chunk_holder->ref();
    DeferOp defer([&] { chunk_holder->unref(); });
    int row_sz = chunk_holder->value()->chunk->num_rows();
    if (_sort_heap == nullptr) {
        _sort_heap = std::make_unique<CommonCursorSortHeap>(detail::ChunkCursorComparator(_sort_desc));
        // avoid exaggerated limit + offset, for an example select * from t order by col limit 9223372036854775800,1
        _sort_heap->reserve(std::min<size_t>(_number_of_rows_to_sort(), 10'000'000ul));
        // build heap
        size_t direct_push = std::min<size_t>(_number_of_rows_to_sort(), row_sz);
        size_t i = 0;

        // push data to heap if heap was not full
        for (; i < direct_push; ++i) {
            detail::ChunkRowCursor cursor(i, chunk_holder);
            _sort_heap->push(std::move(cursor));
        }

        // compare to heap top and replace top
        for (; i < row_sz; ++i) {
            detail::ChunkRowCursor cursor(i, chunk_holder);
            _sort_heap->replace_top_if_less(std::move(cursor));
        }

        // Special optimization for single columns
        if (_sort_exprs->size() == 1) {
            switch ((*_sort_exprs)[0]->root()->type().type) {
#define M(NAME)                                                                                               \
    case LogicalType::NAME: {                                                                                 \
        _do_filter_data = std::bind(&ChunksSorterHeapSort::_do_filter_data_for_type<LogicalType::NAME>, this, \
                                    std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);     \
        break;                                                                                                \
    }
                APPLY_FOR_ALL_SCALAR_TYPE(M)
#undef M
            default:
                break;
            }
        }

    } else {
        if (_number_of_rows_to_sort() == _sort_heap->size()) {
            // if heap was full
            int rows_afterfilter_sz = _filter_data(chunk_holder, row_sz);
            if (_sort_filter_rows != nullptr) {
                COUNTER_UPDATE(_sort_filter_rows, (row_sz - rows_afterfilter_sz));
            }
            for (int i = 0; i < rows_afterfilter_sz; ++i) {
                detail::ChunkRowCursor cursor(i, chunk_holder);
                _sort_heap->replace_top_if_less(std::move(cursor));
            }
        } else {
            // heap was not full we need try push
            size_t direct_push = std::min<size_t>(_number_of_rows_to_sort() - _sort_heap->size(), row_sz);

            int i = 0;
            // push data to heap if heap was not full
            for (; i < direct_push; ++i) {
                detail::ChunkRowCursor cursor(i, chunk_holder);
                _sort_heap->push(std::move(cursor));
            }

            // compare to heap top and replace top
            for (; i < row_sz; ++i) {
                detail::ChunkRowCursor cursor(i, chunk_holder);
                _sort_heap->replace_top_if_less(std::move(cursor));
            }
        }
    }
    // TODO: merge chunk if necessary
    return Status::OK();
}

size_t ChunksSorterHeapSort::get_output_rows() const {
    return _merged_segment.chunk->num_rows();
}

Status ChunksSorterHeapSort::done(RuntimeState* state) {
    ScopedTimer<MonotonicStopWatch> timer(_build_timer);
    if (_sort_heap) {
        auto sorted_values = _sort_heap->sorted_seq();
        size_t result_rows = sorted_values.size();
        ChunkPtr result_chunk = sorted_values[0].data_segment()->chunk->clone_empty(result_rows);
        for (int i = 0; i < result_rows; ++i) {
            auto rid = sorted_values[i].row_id();
            const auto& ref_chunk = sorted_values[i].data_segment()->chunk;
            result_chunk->append_safe(*ref_chunk, rid, 1);
        }
        _merged_segment.init(_sort_exprs, result_chunk);
        // Skip top OFFSET rows
        if (_offset > 0) {
            if (_offset > _merged_segment.chunk->num_rows()) {
                _merged_segment.clear();
                _next_output_row = 0;
            } else {
                _next_output_row += _offset;
            }
        } else {
            _next_output_row = 0;
        }
    }
    _sort_heap.reset();
    return Status::OK();
}

Status ChunksSorterHeapSort::get_next(ChunkPtr* chunk, bool* eos) {
    ScopedTimer<MonotonicStopWatch> timer(_output_timer);
    if (_next_output_row >= _merged_segment.chunk->num_rows()) {
        *chunk = nullptr;
        *eos = true;
        return Status::OK();
    }
    *eos = false;
    size_t count = std::min(size_t(_state->chunk_size()), _merged_segment.chunk->num_rows() - _next_output_row);
    chunk->reset(_merged_segment.chunk->clone_empty(count).release());
    (*chunk)->append_safe(*_merged_segment.chunk, _next_output_row, count);
    _next_output_row += count;

    return Status::OK();
}

std::vector<JoinRuntimeFilter*>* ChunksSorterHeapSort::runtime_filters(ObjectPool* pool) {
    if (_sort_heap == nullptr || _sort_heap->size() < _number_of_rows_to_sort()) {
        return nullptr;
    }

    // avoid limit 0
    if (_sort_heap->empty()) {
        return nullptr;
    }

    const auto& top_cursor = _sort_heap->top();
    const int cursor_rid = top_cursor.row_id();
    const auto& top_cursor_column = top_cursor.data_segment()->order_by_columns[0];

    if (_runtime_filter.empty()) {
        auto rf = type_dispatch_predicate<JoinRuntimeFilter*>(
                (*_sort_exprs)[0]->root()->type().type, false, detail::SortRuntimeFilterBuilder(), pool,
                top_cursor_column, cursor_rid, _sort_desc.descs[0].asc_order(), false);
        _runtime_filter.emplace_back(rf);
    } else {
        type_dispatch_predicate<std::nullptr_t>((*_sort_exprs)[0]->root()->type().type, false,
                                                detail::SortRuntimeFilterUpdater(), _runtime_filter.back(),
                                                top_cursor_column, cursor_rid, _sort_desc.descs[0].asc_order());
    }
    return &_runtime_filter;
}

template <LogicalType TYPE>
void ChunksSorterHeapSort::_do_filter_data_for_type(detail::ChunkHolder* chunk_holder, Filter* filter, int row_sz) {
    const auto& top_cursor = _sort_heap->top();
    const int cursor_rid = top_cursor.row_id();

    const auto& top_cursor_column = top_cursor.data_segment()->order_by_columns[0];
    const auto& input_column = chunk_holder->value()->order_by_columns[0];

    DCHECK_EQ(top_cursor_column->is_nullable(), input_column->is_nullable());

    if (top_cursor_column->only_null()) {
        // case for order by null
        bool top_is_null = top_cursor_column->is_null(cursor_rid);
        int null_compare_flag = _sort_desc.get_column_desc(0).nan_direction();
        auto* __restrict__ filter_data = filter->data();

        for (int i = 0; i < row_sz; ++i) {
            if (!top_is_null) {
                filter_data[i] = null_compare_flag < 0;
            }
        }
    } else if (top_cursor_column->is_nullable()) {
        bool top_is_null = top_cursor_column->is_null(cursor_rid);
        const auto& need_filter_data =
                ColumnHelper::cast_to_raw<TYPE>(down_cast<NullableColumn*>(top_cursor_column.get())->data_column())
                        ->get_data()[cursor_rid];

        const auto& order_by_null_column = down_cast<NullableColumn*>(input_column.get())->null_column();
        const auto& order_by_data_column = down_cast<NullableColumn*>(input_column.get())->data_column();

        const auto* null_data = order_by_null_column->get_data().data();
        const auto* order_by_data = ColumnHelper::cast_to_raw<TYPE>(order_by_data_column)->get_data().data();
        auto* __restrict__ filter_data = filter->data();

        // null compare flag
        int null_compare_flag = _sort_desc.get_column_desc(0).nan_direction();
        int sort_order_flag = _sort_desc.get_column_desc(0).sort_order;
        for (int i = 0; i < row_sz; ++i) {
            // data is null
            if (null_data[i] && !top_is_null) {
                filter_data[i] = null_compare_flag < 0;
            } else if (null_data[i] && top_is_null) {
                // filter equal rows
            } else if (!null_data[i] && top_is_null) {
                filter_data[i] = null_compare_flag > 0;
            } else {
                DCHECK(!null_data[i] && !top_is_null);
                if (sort_order_flag > 0) {
                    filter_data[i] = order_by_data[i] < need_filter_data;
                } else {
                    filter_data[i] = order_by_data[i] > need_filter_data;
                }
            }
        }

    } else {
        const auto& need_filter_data = ColumnHelper::cast_to_raw<TYPE>(top_cursor_column)->get_data()[cursor_rid];
        auto* order_by_column = ColumnHelper::cast_to_raw<TYPE>(input_column);

        const auto* __restrict__ order_by_data = order_by_column->get_data().data();
        auto* __restrict__ filter_data = filter->data();
        int sort_order_flag = _sort_desc.get_column_desc(0).sort_order;

        if (sort_order_flag > 0) {
            for (int i = 0; i < row_sz; ++i) {
                filter_data[i] = order_by_data[i] < need_filter_data;
            }
        } else {
            for (int i = 0; i < row_sz; ++i) {
                filter_data[i] = order_by_data[i] > need_filter_data;
            }
        }
    }
}

int ChunksSorterHeapSort::_filter_data(detail::ChunkHolder* chunk_holder, int row_sz) {
    ScopedTimer<MonotonicStopWatch> timer(_sort_filter_costs);
    // Filter greater or equal top_cursor columns
    const auto& top_cursor = _sort_heap->top();
    const int cursor_rid = top_cursor.row_id();
    const int column_sz = top_cursor.data_segment()->order_by_columns.size();

    Filter filter(row_sz);

    // For single column special optimization
    if (_do_filter_data) {
        _do_filter_data(chunk_holder, &filter, row_sz);
    } else {
        for (int i = 0; i < row_sz; ++i) {
            for (int j = 0; j < column_sz; ++j) {
                int res = chunk_holder->value()->order_by_columns[j]->compare_at(
                        i, cursor_rid, *top_cursor.data_segment()->order_by_columns[j],
                        _sort_desc.get_column_desc(j).null_first);
                if (res != 0) {
                    filter[i] = (res * _sort_desc.get_column_desc(j).sort_order) < 0;
                    break;
                }
            }
        }
    }
    return chunk_holder->value()->chunk->filter(filter);
}

void ChunksSorterHeapSort::setup_runtime(RuntimeProfile* profile, MemTracker* parent_mem_tracker) {
    ChunksSorter::setup_runtime(profile, parent_mem_tracker);
    _sort_filter_costs = ADD_TIMER(profile, "SortFilterCost");
    _sort_filter_rows = ADD_COUNTER(profile, "SortFilterRows", TUnit::UNIT);
}

} // namespace starrocks
