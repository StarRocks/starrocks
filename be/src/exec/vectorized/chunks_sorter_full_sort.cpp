// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "chunks_sorter_full_sort.h"

#include "column/type_traits.h"
#include "exprs/expr.h"
#include "gutil/casts.h"
#include "runtime/runtime_state.h"
#include "util/orlp/pdqsort.h"
#include "util/stopwatch.hpp"

namespace starrocks::vectorized {

// SortHelper functions only work for full sort.
class SortHelper {
public:
    // Sort on type-known column, and the column has no NULL value in sorting range.
    template <PrimitiveType PT, bool stable>
    static void sort_on_not_null_column(Column* column, bool is_asc_order, Permutation& perm) {
        sort_on_not_null_column_within_range<PT, stable>(column, is_asc_order, perm, 0, perm.size());
    }

    // Sort on type-known column, and the column may have NULL values in the sorting range.
    template <PrimitiveType PT, bool stable>
    static void sort_on_nullable_column(Column* column, bool is_asc_order, bool is_null_first, Permutation& perm) {
        auto* nullable_col = down_cast<NullableColumn*>(column);

        auto null_first_fn = [&nullable_col](const PermutationItem& item) -> bool {
            return nullable_col->is_null(item.index_in_chunk);
        };
        auto null_last_fn = [&nullable_col](const PermutationItem& item) -> bool {
            return !nullable_col->is_null(item.index_in_chunk);
        };

        size_t data_offset = 0;
        size_t data_count = 0;
        // separate null and non-null values
        if (is_null_first) {
            // put all NULLs at the begin of the permutation.
            auto begin_of_not_null = std::stable_partition(perm.begin(), perm.end(), null_first_fn);
            data_offset = begin_of_not_null - perm.begin();
            if (data_offset < perm.size()) {
                data_count = perm.size() - data_offset;
            } else {
                return;
            }
        } else {
            // put all NULLs at the end of the permutation.
            auto end_of_not_null = std::stable_partition(perm.begin(), perm.end(), null_last_fn);
            data_count = end_of_not_null - perm.begin();
        }
        // sort non-null values
        sort_on_not_null_column_within_range<PT, stable>(nullable_col->mutable_data_column(), is_asc_order, perm,
                                                         data_offset, data_count);
    }

    // Sort on column that with unknown data type.
    static void sort_on_other_column(Column* column, int sort_order_flag, int null_first_flag, Permutation& perm) {
        // decides whether element l precedes element r.
        auto cmp_fn = [&column, &sort_order_flag, &null_first_flag](const PermutationItem& l,
                                                                    const PermutationItem& r) -> bool {
            int cmp = column->compare_at(l.index_in_chunk, r.index_in_chunk, *column, null_first_flag);
            if (cmp == 0) {
                return l.permutation_index < r.permutation_index;
            } else {
                cmp *= sort_order_flag;
                return cmp < 0;
            }
        };
        pdqsort(perm.begin(), perm.end(), cmp_fn);
    }

private:
    // Sort on type-known column, and the column has no NULL value in sorting range.
    template <PrimitiveType PT, bool stable>
    static void sort_on_not_null_column_within_range(Column* column, bool is_asc_order, Permutation& perm,
                                                     size_t offset, size_t count = 0) {
        using ColumnTypeName = typename RunTimeTypeTraits<PT>::ColumnType;
        using CppTypeName = typename RunTimeTypeTraits<PT>::CppType;

        // for numeric column: integers, floats, date, datetime, decimals
        if constexpr (pt_is_fixedlength<PT>) {
            sort_on_not_null_fixed_size_column<CppTypeName, stable>(column, is_asc_order, perm, offset, count);
            return;
        }

        // for binary column
        if constexpr (pt_is_binary<PT>) {
            sort_on_not_null_binary_column<stable>(column, is_asc_order, perm, offset, count);
            return;
        }

        // for other columns
        const ColumnTypeName* col = down_cast<ColumnTypeName*>(column);
        auto less_fn = [&col](const PermutationItem& l, const PermutationItem& r) -> bool {
            int c = col->compare_at(l.index_in_chunk, r.index_in_chunk, *col, 1);
            if constexpr (stable) {
                if (c == 0) {
                    return l.permutation_index < r.permutation_index;
                }
                return c < 0;
            } else {
                return c < 0;
            }
        };
        auto greater_fn = [&col](const PermutationItem& l, const PermutationItem& r) -> bool {
            int c = col->compare_at(l.index_in_chunk, r.index_in_chunk, *col, 1);
            if constexpr (stable) {
                if (c == 0) {
                    return l.permutation_index < r.permutation_index; // first element is greater.
                }
                return c > 0;
            } else {
                return c > 0;
            }
        };
        size_t end_pos = (count == 0 ? perm.size() : offset + count);
        if (end_pos > perm.size()) {
            end_pos = perm.size();
        }
        if (is_asc_order) {
            pdqsort(perm.begin() + offset, perm.begin() + end_pos, less_fn);
        } else {
            pdqsort(perm.begin() + offset, perm.begin() + end_pos, greater_fn);
        }
    }

    template <typename CppTypeName>
    struct SortItem {
        CppTypeName value;
        uint32_t index_in_chunk;
        uint32_t permutation_index; // sequence index for keeping sort stable.
    };

    // Sort string
    template <bool stable>
    static void sort_on_not_null_binary_column(Column* column, bool is_asc_order, Permutation& perm, size_t offset,
                                               size_t count = 0) {
        const size_t row_num = (count == 0 || offset + count > perm.size()) ? (perm.size() - offset) : count;
        auto* binary_column = reinterpret_cast<BinaryColumn*>(column);
        auto& data = binary_column->get_data();
        std::vector<SortItem<Slice>> sort_items(row_num);
        for (uint32_t i = 0; i < row_num; ++i) {
            sort_items[i] = {data[perm[i + offset].index_in_chunk], perm[i + offset].index_in_chunk, i};
        }
        auto less_fn = [](const SortItem<Slice>& l, const SortItem<Slice>& r) -> bool {
            if constexpr (stable) {
                int res = l.value.compare(r.value);
                if (res == 0) {
                    return l.permutation_index < r.permutation_index;
                } else {
                    return res < 0;
                }
            } else {
                int res = l.value.compare(r.value);
                return res < 0;
            }
        };
        auto greater_fn = [](const SortItem<Slice>& l, const SortItem<Slice>& r) -> bool {
            if constexpr (stable) {
                int res = l.value.compare(r.value);
                if (res == 0) {
                    return l.permutation_index < r.permutation_index;
                } else {
                    return res > 0;
                }
            } else {
                int res = l.value.compare(r.value);
                return res > 0;
            }
        };

        if (is_asc_order) {
            pdqsort(sort_items.begin(), sort_items.end(), less_fn);
        } else {
            pdqsort(sort_items.begin(), sort_items.end(), greater_fn);
        }
        for (size_t i = 0; i < row_num; ++i) {
            perm[i + offset].index_in_chunk = sort_items[i].index_in_chunk;
        }
    }

    // Sort on some numeric column which has no NULL value in sorting range.
    // Only supports: integers, floats. Not Slice, DecimalV2Value.
    template <typename CppTypeName, bool stable>
    static void sort_on_not_null_fixed_size_column(Column* column, bool is_asc_order, Permutation& perm, size_t offset,
                                                   size_t count = 0) {
        // column->size() == perm.size()
        const size_t row_num = (count == 0 || offset + count > perm.size()) ? (perm.size() - offset) : count;
        const CppTypeName* data = static_cast<CppTypeName*>((void*)column->mutable_raw_data());
        std::vector<SortItem<CppTypeName>> sort_items(row_num);
        for (uint32_t i = 0; i < row_num; ++i) {
            sort_items[i] = {data[perm[i + offset].index_in_chunk], perm[i + offset].index_in_chunk, i};
        }
        auto less_fn = [](const SortItem<CppTypeName>& l, const SortItem<CppTypeName>& r) -> bool {
            if constexpr (stable) {
                if (l.value == r.value) {
                    return l.permutation_index < r.permutation_index; // for stable sort
                } else {
                    return l.value < r.value;
                }
            } else {
                return l.value < r.value;
            }
        };
        auto greater_fn = [](const SortItem<CppTypeName>& l, const SortItem<CppTypeName>& r) -> bool {
            if constexpr (stable) {
                if (l.value == r.value) {
                    return l.permutation_index < r.permutation_index; // for stable sort
                } else {
                    return l.value > r.value;
                }
            } else {
                return l.value > r.value;
            }
        };

        if (is_asc_order) {
            pdqsort(sort_items.begin(), sort_items.end(), less_fn);
        } else {
            pdqsort(sort_items.begin(), sort_items.end(), greater_fn);
        }
        // output permutation
        for (size_t i = 0; i < row_num; ++i) {
            perm[i + offset].index_in_chunk = sort_items[i].index_in_chunk;
        }
    }
};

ChunksSorterFullSort::ChunksSorterFullSort(const std::vector<ExprContext*>* sort_exprs, const std::vector<bool>* is_asc,
                                           const std::vector<bool>* is_null_first, size_t size_of_chunk_batch)
        : ChunksSorter(sort_exprs, is_asc, is_null_first, size_of_chunk_batch) {
    _selective_values.resize(config::vector_chunk_size);
}

ChunksSorterFullSort::~ChunksSorterFullSort() = default;

Status ChunksSorterFullSort::update(RuntimeState* state, const ChunkPtr& chunk) {
    if (UNLIKELY(_big_chunk == nullptr)) {
        _big_chunk = chunk->clone_empty();
    }

    if (_big_chunk->num_rows() + chunk->num_rows() > std::numeric_limits<uint32_t>::max()) {
        LOG(WARNING) << "full sort row is " << _big_chunk->num_rows() + chunk->num_rows();
        return Status::InternalError("Full sort in single query instance only support at most 4294967295 rows");
    }

    _big_chunk->append(*chunk);

    DCHECK(!_big_chunk->has_const_column());
    return Status::OK();
}

Status ChunksSorterFullSort::done(RuntimeState* state) {
    if (_big_chunk != nullptr && _big_chunk->num_rows() > 0) {
        RETURN_IF_ERROR(_sort_chunks(state));
    }

    DCHECK_EQ(_next_output_row, 0);
    return Status::OK();
}

void ChunksSorterFullSort::get_next(ChunkPtr* chunk, bool* eos) {
    SCOPED_TIMER(_output_timer);
    if (_next_output_row >= _sorted_permutation.size()) {
        *chunk = nullptr;
        *eos = true;
        return;
    }
    *eos = false;
    size_t count = std::min(size_t(config::vector_chunk_size), _sorted_permutation.size() - _next_output_row);
    chunk->reset(_sorted_segment->chunk->clone_empty(count).release());
    _append_rows_to_chunk(chunk->get(), _sorted_segment->chunk.get(), _sorted_permutation, _next_output_row, count);
    _next_output_row += count;
}

/*
 * _next_output_row index the next row we need to get,  
 * _sorted_permutation means all the result datas. In this case, 
 * _sorted_permutation use as an index, 
 * The actual data is _sorted_segment->chunk, 
 * so we use _next_output_row and _sorted_permutation to get datas from _sorted_segment->chunk, 
 * and copy it in chunk as output.
 */
bool ChunksSorterFullSort::pull_chunk(ChunkPtr* chunk) {
    // _next_output_row used to record next row to get,
    // This condition is used to determine whether all data has been retrieved.
    if (_next_output_row >= _sorted_permutation.size()) {
        *chunk = nullptr;
        return true;
    }
    size_t count = std::min(size_t(config::vector_chunk_size), _sorted_permutation.size() - _next_output_row);
    chunk->reset(_sorted_segment->chunk->clone_empty(count).release());
    _append_rows_to_chunk(chunk->get(), _sorted_segment->chunk.get(), _sorted_permutation, _next_output_row, count);
    _next_output_row += count;

    if (_next_output_row >= _sorted_permutation.size()) {
        return true;
    }
    return false;
}

Status ChunksSorterFullSort::_sort_chunks(RuntimeState* state) {
    // Step1: construct permutation
    RETURN_IF_ERROR(_build_sorting_data(state));

    // Step2: sort by columns or row
    // For no more than three order-by columns, sorting by columns can benefit from reducing
    // the cost of calling virtual functions of Column::compare_at.
    if (_get_number_of_order_by_columns() <= 3) {
        _sort_by_columns();
    } else {
        _sort_by_row_cmp();
    }
    return Status::OK();
}

Status ChunksSorterFullSort::_build_sorting_data(RuntimeState* state) {
    SCOPED_TIMER(_build_timer);
    size_t row_count = _big_chunk->num_rows();

    _sorted_segment = std::make_unique<DataSegment>(_sort_exprs, ChunkPtr(_big_chunk.release()));

    _sorted_permutation.resize(row_count);
    for (uint32_t i = 0; i < row_count; ++i) {
        _sorted_permutation[i] = {0, i, i};
    }

    return Status::OK();
}

// Sort in row style with simplified Permutation struct for the seek of a better cache.
void ChunksSorterFullSort::_sort_by_row_cmp() {
    SCOPED_TIMER(_sort_timer);

    if (_get_number_of_order_by_columns() < 1) {
        return;
    }

    // In this case, PermutationItem::chunk_index is constantly 0,
    // and PermutationItem::index_in_chunk is always equal to PermutationItem::permutation_index,
    // which is the sequence index of the element in the array.
    // This simplified index array can help the sort routine to get a better performance.
    const size_t elem_number = _sorted_permutation.size();
    std::vector<size_t> indices(elem_number);
    for (size_t i = 0; i < elem_number; ++i) {
        indices[i] = i;
    }

    const DataSegment& data_segment = *_sorted_segment;
    const std::vector<int>& sort_order_flag = _sort_order_flag;
    const std::vector<int>& null_first_flag = _null_first_flag;

    auto cmp_fn = [&data_segment, &sort_order_flag, &null_first_flag](const size_t& l, const size_t& r) {
        int c = data_segment.compare_at(l, data_segment, r, sort_order_flag, null_first_flag);
        if (c == 0) {
            return l < r;
        } else {
            return c < 0;
        }
    };

    pdqsort(indices.begin(), indices.end(), cmp_fn);

    // Set the permutation array to sorted indices.
    for (size_t i = 0; i < elem_number; ++i) {
        _sorted_permutation[i].index_in_chunk = _sorted_permutation[i].permutation_index = indices[i];
    }
}

#define CASE_FOR_NULLABLE_COLUMN_SORT(PrimitiveTypeName)                                                       \
    case PrimitiveTypeName: {                                                                                  \
        if (stable) {                                                                                          \
            SortHelper::sort_on_nullable_column<PrimitiveTypeName, true>(column, is_asc_order, is_null_first,  \
                                                                         _sorted_permutation);                 \
        } else {                                                                                               \
            SortHelper::sort_on_nullable_column<PrimitiveTypeName, false>(column, is_asc_order, is_null_first, \
                                                                          _sorted_permutation);                \
        }                                                                                                      \
        break;                                                                                                 \
    }

#define CASE_FOR_NOT_NULL_COLUMN_SORT(PrimitiveTypeName)                                                              \
    case PrimitiveTypeName: {                                                                                         \
        if (stable) {                                                                                                 \
            SortHelper::sort_on_not_null_column<PrimitiveTypeName, true>(column, is_asc_order, _sorted_permutation);  \
        } else {                                                                                                      \
            SortHelper::sort_on_not_null_column<PrimitiveTypeName, false>(column, is_asc_order, _sorted_permutation); \
        }                                                                                                             \
        break;                                                                                                        \
    }

// Sort in column style to avoid calling virtual methods of Column.
void ChunksSorterFullSort::_sort_by_columns() {
    SCOPED_TIMER(_sort_timer);

    if (_get_number_of_order_by_columns() < 1) {
        return;
    }

    for (int col_index = static_cast<int>(_get_number_of_order_by_columns()) - 1; col_index >= 0; --col_index) {
        Column* column = _sorted_segment->order_by_columns[col_index].get();
        bool stable = col_index != _get_number_of_order_by_columns() - 1;
        if (column->is_constant()) {
            continue;
        }

        bool is_asc_order = (_sort_order_flag[col_index] == 1);
        bool is_null_first;
        if (is_asc_order) {
            is_null_first = (_null_first_flag[col_index] == -1);
        } else {
            is_null_first = (_null_first_flag[col_index] == 1);
        }
        ExprContext* expr_ctx = (*_sort_exprs)[col_index];
        if (column->is_nullable()) {
            switch (expr_ctx->root()->type().type) {
                CASE_FOR_NULLABLE_COLUMN_SORT(TYPE_BOOLEAN)
                CASE_FOR_NULLABLE_COLUMN_SORT(TYPE_TINYINT)
                CASE_FOR_NULLABLE_COLUMN_SORT(TYPE_SMALLINT)
                CASE_FOR_NULLABLE_COLUMN_SORT(TYPE_INT)
                CASE_FOR_NULLABLE_COLUMN_SORT(TYPE_BIGINT)
                CASE_FOR_NULLABLE_COLUMN_SORT(TYPE_LARGEINT)
                CASE_FOR_NULLABLE_COLUMN_SORT(TYPE_FLOAT)
                CASE_FOR_NULLABLE_COLUMN_SORT(TYPE_DOUBLE)
                CASE_FOR_NULLABLE_COLUMN_SORT(TYPE_DECIMALV2)
                CASE_FOR_NULLABLE_COLUMN_SORT(TYPE_DECIMAL32)
                CASE_FOR_NULLABLE_COLUMN_SORT(TYPE_DECIMAL64)
                CASE_FOR_NULLABLE_COLUMN_SORT(TYPE_DECIMAL128)
                CASE_FOR_NULLABLE_COLUMN_SORT(TYPE_CHAR)
                CASE_FOR_NULLABLE_COLUMN_SORT(TYPE_VARCHAR)
                CASE_FOR_NULLABLE_COLUMN_SORT(TYPE_DATE)
                CASE_FOR_NULLABLE_COLUMN_SORT(TYPE_DATETIME)
                CASE_FOR_NULLABLE_COLUMN_SORT(TYPE_TIME)
            default: {
                SortHelper::sort_on_other_column(column, _sort_order_flag[col_index], _null_first_flag[col_index],
                                                 _sorted_permutation);
                break;
            }
            }
        } else {
            switch (expr_ctx->root()->type().type) {
                CASE_FOR_NOT_NULL_COLUMN_SORT(TYPE_BOOLEAN)
                CASE_FOR_NOT_NULL_COLUMN_SORT(TYPE_TINYINT)
                CASE_FOR_NOT_NULL_COLUMN_SORT(TYPE_SMALLINT)
                CASE_FOR_NOT_NULL_COLUMN_SORT(TYPE_INT)
                CASE_FOR_NOT_NULL_COLUMN_SORT(TYPE_BIGINT)
                CASE_FOR_NOT_NULL_COLUMN_SORT(TYPE_LARGEINT)
                CASE_FOR_NOT_NULL_COLUMN_SORT(TYPE_FLOAT)
                CASE_FOR_NOT_NULL_COLUMN_SORT(TYPE_DOUBLE)
                CASE_FOR_NOT_NULL_COLUMN_SORT(TYPE_DECIMALV2)
                CASE_FOR_NOT_NULL_COLUMN_SORT(TYPE_DECIMAL32)
                CASE_FOR_NOT_NULL_COLUMN_SORT(TYPE_DECIMAL64)
                CASE_FOR_NOT_NULL_COLUMN_SORT(TYPE_DECIMAL128)
                CASE_FOR_NOT_NULL_COLUMN_SORT(TYPE_CHAR)
                CASE_FOR_NOT_NULL_COLUMN_SORT(TYPE_VARCHAR)
                CASE_FOR_NOT_NULL_COLUMN_SORT(TYPE_DATE)
                CASE_FOR_NOT_NULL_COLUMN_SORT(TYPE_DATETIME)
                CASE_FOR_NOT_NULL_COLUMN_SORT(TYPE_TIME)
            default: {
                SortHelper::sort_on_other_column(column, _sort_order_flag[col_index], _null_first_flag[col_index],
                                                 _sorted_permutation);
                break;
            }
            }
        }
        // reset permutation_index
        const size_t size = _sorted_permutation.size();
        for (size_t i = 0; i < size; ++i) {
            _sorted_permutation[i].permutation_index = i;
        }
    }
}

void ChunksSorterFullSort::_append_rows_to_chunk(Chunk* dest, Chunk* src, const Permutation& permutation, size_t offset,
                                                 size_t count) {
    for (size_t i = offset; i < offset + count; ++i) {
        _selective_values[i - offset] = permutation[i].index_in_chunk;
    }
    dest->append_selective(*src, _selective_values.data(), 0, count);

    DCHECK(!dest->has_const_column());
}

} // namespace starrocks::vectorized
