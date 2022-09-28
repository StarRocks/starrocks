// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/vectorized//chunk_sorter_heapsorter.h"

#include <functional>
#include <memory>
#include <vector>

#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "glog/logging.h"
#include "gutil/casts.h"
#include "runtime/primitive_type_infra.h"
#include "util/defer_op.h"

namespace starrocks::vectorized {

Status HeapChunkSorter::update(RuntimeState* state, const ChunkPtr& chunk) {
    ScopedTimer<MonotonicStopWatch> timer(_build_timer);

    if (chunk->is_empty() || _number_of_rows_to_sort() == 0) {
        return Status::OK();
    }

    // chunk_holder was shared ownership by itself
    detail::ChunkHolder* chunk_holder = new detail::ChunkHolder(std::make_shared<DataSegment>(_sort_exprs, chunk));
    chunk_holder->ref();
    DeferOp defer([&] { chunk_holder->unref(); });
    int row_sz = chunk_holder->value()->chunk->num_rows();
    if (_sort_heap == nullptr) {
        _sort_heap = std::make_unique<CommonCursorSortHeap>(
                detail::ChunkCursorComparator(_sort_order_flag.data(), _null_first_flag.data()));
        _sort_heap->reserve(_number_of_rows_to_sort());
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
#define M(NAME)                                                                                            \
    case PrimitiveType::NAME: {                                                                            \
        _do_filter_data = std::bind(&HeapChunkSorter::_do_filter_data_for_type<PrimitiveType::NAME>, this, \
                                    std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);  \
        break;                                                                                             \
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

DataSegment* HeapChunkSorter::get_result_data_segment() {
    return &_merged_segment;
}
uint64_t HeapChunkSorter::get_partition_rows() const {
    return _merged_segment.chunk->num_rows();
}

Status HeapChunkSorter::done(RuntimeState* state) {
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
    }
    _sort_heap.reset();
    return Status::OK();
}

void HeapChunkSorter::get_next(ChunkPtr* chunk, bool* eos) {
    ScopedTimer<MonotonicStopWatch> timer(_output_timer);
    if (_next_output_row >= _merged_segment.chunk->num_rows()) {
        *chunk = nullptr;
        *eos = true;
        return;
    }
    *eos = false;
    size_t count = std::min(size_t(_state->chunk_size()), _merged_segment.chunk->num_rows() - _next_output_row);
    chunk->reset(_merged_segment.chunk->clone_empty(count).release());
    (*chunk)->append_safe(*_merged_segment.chunk, _next_output_row, count);
    _next_output_row += count;
}

bool HeapChunkSorter::pull_chunk(ChunkPtr* chunk) {
    if (_next_output_row >= _merged_segment.chunk->num_rows()) {
        *chunk = nullptr;
        return true;
    }
    size_t count = std::min(size_t(_state->chunk_size()), _merged_segment.chunk->num_rows() - _next_output_row);
    chunk->reset(_merged_segment.chunk->clone_empty(count).release());
    (*chunk)->append_safe(*_merged_segment.chunk, _next_output_row, count);
    _next_output_row += count;

    if (_next_output_row >= _merged_segment.chunk->num_rows()) {
        return true;
    }
    return false;
}

template <PrimitiveType TYPE>
void HeapChunkSorter::_do_filter_data_for_type(detail::ChunkHolder* chunk_holder, Column::Filter* filter, int row_sz) {
    const auto& top_cursor = _sort_heap->top();
    const int cursor_rid = top_cursor.row_id();

    const auto& top_cursor_column = top_cursor.data_segment()->order_by_columns[0];
    const auto& input_column = chunk_holder->value()->order_by_columns[0];

    DCHECK_EQ(top_cursor_column->is_nullable(), input_column->is_nullable());

    if (top_cursor_column->only_null()) {
        // case for order by null
        bool top_is_null = top_cursor_column->is_null(cursor_rid);
        int null_compare_flag = _null_first_flag[0];
        auto* __restrict__ filter_data = filter->data();

        if (!top_is_null) {
            for (int i = 0; i < row_sz; ++i) {
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
        int null_compare_flag = _null_first_flag[0] * _sort_order_flag[0];
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
                if (_sort_order_flag[0] > 0) {
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

        if (_sort_order_flag[0] > 0) {
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

int HeapChunkSorter::_filter_data(detail::ChunkHolder* chunk_holder, int row_sz) {
    ScopedTimer<MonotonicStopWatch> timer(_sort_filter_costs);
    // Filter greater or equal top_cursor columns
    const auto& top_cursor = _sort_heap->top();
    const int cursor_rid = top_cursor.row_id();
    const int column_sz = top_cursor.data_segment()->order_by_columns.size();

    Column::Filter filter(row_sz);

    // For single column special optimization
    if (_do_filter_data) {
        _do_filter_data(chunk_holder, &filter, row_sz);
    } else {
        for (int i = 0; i < row_sz; ++i) {
            for (int j = 0; j < column_sz; ++j) {
                int res = chunk_holder->value()->order_by_columns[j]->compare_at(
                        i, cursor_rid, *top_cursor.data_segment()->order_by_columns[j], _null_first_flag[j]);
                if (res != 0) {
                    filter[i] = res * _sort_order_flag[j] < 0;
                    break;
                }
            }
        }
    }
    return chunk_holder->value()->chunk->filter(filter);
}

void HeapChunkSorter::setup_runtime(RuntimeProfile* profile) {
    ChunksSorter::setup_runtime(profile);
    _sort_filter_costs = ADD_TIMER(profile, "SortFilterCost");
    _sort_filter_rows = ADD_COUNTER(profile, "SortFilterRows", TUnit::UNIT);
}

} // namespace starrocks::vectorized
