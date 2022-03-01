// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "chunks_sorter_full_sort.h"

#include <type_traits>

#include "column/type_traits.h"
#include "exec/vectorized/chunks_sorter.h"
#include "exprs/expr.h"
#include "gutil/casts.h"
#include "runtime/primitive_type_infra.h"
#include "runtime/runtime_state.h"
#include "util/orlp/pdqsort.h"
#include "util/stopwatch.hpp"

namespace starrocks::vectorized {

// SortHelper functions only work for full sort.
class SortHelper {
public:
    template <typename CppTypeName>
    struct SortItem {
        CppTypeName value;
        uint32_t index_in_chunk;
        uint32_t permutation_index; // sequence index for keeping sort stable.
    };

    template <class CppType, bool less>
    struct SortItemComparator {
        static constexpr int compareInt(int lhs, int rhs) {
            if (lhs > rhs) {
                return 1;
            } else if (lhs == rhs) {
                return 0;
            } else {
                return -1;
            }
        }

        int compare(const SortItem<CppType>& lhs, const SortItem<CppType>& rhs) {
            using cpp_type = std::remove_cv_t<CppType>;
            if constexpr (!std::is_same_v<cpp_type, Slice>) {
                if (lhs.value == rhs.value) {
                    return compareInt(lhs.permutation_index, rhs.permutation_index);
                } else if (lhs.value < rhs.value) {
                    return -1;
                } else {
                    return 1;
                }

            } else {
                int x = lhs.value.compare(rhs.value);
                if (x == 0) {
                    return compareInt(lhs.permutation_index, rhs.permutation_index);
                } else {
                    return x;
                }
            }
        }

        bool operator()(const SortItem<CppType>& lhs, const SortItem<CppType>& rhs) {
            if constexpr (less) {
                return compare(lhs, rhs) < 0;
            } else {
                return compare(lhs, rhs) > 0;
            }
        }
    };

    template <PrimitiveType ptype, bool less, bool stable>
    struct PermutationItemComparator {
        using ColumnType = RunTimeColumnType<ptype>;

    private:
        // down_cast to avoid virtual fucntion call
        ColumnType* _lhs_column;
        ColumnType* _rhs_column;

    public:
        PermutationItemComparator(Column* lhs_column, Column* rhs_column)
                : _lhs_column(down_cast<ColumnType*>(lhs_column)), _rhs_column(down_cast<ColumnType*>(rhs_column)) {}

        bool operator()(const PermutationItem& lhs, const PermutationItem& rhs) {
            int x = _lhs_column->compare_at(lhs.index_in_chunk, rhs.index_in_chunk, *_rhs_column, 1);
            if constexpr (stable) {
                if (x == 0) {
                    return lhs.permutation_index < rhs.permutation_index;
                }
                if constexpr (less) {
                    return x < 0;
                } else {
                    return x > 0;
                }
            } else {
                if constexpr (less) {
                    return x < 0;
                } else {
                    return x > 0;
                }
            }
        }
    };

    struct SingleColumnSortDesc {
        PrimitiveType sort_type;
        bool nullable;
        bool stable;
        bool is_asc;
        bool null_first;
    };

    struct SingleColumnSorter {
        template <PrimitiveType ptype>
        Status operator()(RuntimeState* state, Column* column, const SingleColumnSortDesc& desc, Permutation& perm) {
            if (desc.stable) {
                if (desc.nullable) {
                    return sort_on_nullable_column<ptype, true>(state, column, desc.is_asc, desc.null_first, perm);
                } else {
                    return sort_on_not_null_column<ptype, true>(state, column, desc.is_asc, perm);
                }
            } else {
                if (desc.nullable) {
                    return sort_on_nullable_column<ptype, false>(state, column, desc.is_asc, desc.null_first, perm);
                } else {
                    return sort_on_not_null_column<ptype, false>(state, column, desc.is_asc, perm);
                }
            }
            return Status::OK();
        }
    };

    // Sort a single column
    static Status sort_single_column(RuntimeState* state, Column* column, const SingleColumnSortDesc& desc,
                                     Permutation& perm) {
        return type_dispatch_sortable(desc.sort_type, SingleColumnSorter(), state, column, desc, perm);
    }

    // Sort on type-known column, and the column has no NULL value in sorting range.
    template <PrimitiveType PT, bool stable>
    static Status sort_on_not_null_column(RuntimeState* state, Column* column, bool is_asc_order, Permutation& perm) {
        return sort_on_not_null_column_within_range<PT, stable>(state, column, is_asc_order, perm, 0, perm.size());
    }

    // Sort on type-known column, and the column may have NULL values in the sorting range.
    template <PrimitiveType PT, bool stable>
    static Status sort_on_nullable_column(RuntimeState* state, Column* column, bool is_asc_order, bool is_null_first,
                                          Permutation& perm) {
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
                return Status::OK();
            }
        } else {
            // put all NULLs at the end of the permutation.
            auto end_of_not_null = std::stable_partition(perm.begin(), perm.end(), null_last_fn);
            data_count = end_of_not_null - perm.begin();
        }
        // sort non-null values
        return sort_on_not_null_column_within_range<PT, stable>(state, nullable_col->mutable_data_column(),
                                                                is_asc_order, perm, data_offset, data_count);
    }

private:
    // Sort on type-known column, and the column has no NULL value in sorting range.
    template <PrimitiveType PT, bool stable>
    static Status sort_on_not_null_column_within_range(RuntimeState* state, Column* column, bool is_asc_order,
                                                       Permutation& perm, size_t offset, size_t count = 0) {
        using ColumnTypeName = typename RunTimeTypeTraits<PT>::ColumnType;
        using CppTypeName = typename RunTimeTypeTraits<PT>::CppType;

        if constexpr (pt_is_fixedlength<PT> || pt_is_binary<PT>) {
            return _sort_by_sortitem<PT, stable>(state, column, is_asc_order, perm, offset);
        } else {
            return _sort_by_permutation<PT, stable>(state, column, is_asc_order, perm, offset);
        }
    }

    template <PrimitiveType PT, bool stable>
    static Status _sort_by_sortitem(RuntimeState* state, Column* column, bool is_asc_order, Permutation& perm,
                                    size_t offset, size_t count = 0) {
        using ColumnTypeName = typename RunTimeTypeTraits<PT>::ColumnType;
        using CppTypeName = typename RunTimeTypeTraits<PT>::CppType;

        const size_t row_num = (count == 0 || offset + count > perm.size()) ? (perm.size() - offset) : count;
        ColumnTypeName* col = down_cast<ColumnTypeName*>(column);
        auto& data = col->get_data();
        std::vector<SortItem<CppTypeName>> sort_items(row_num);
        for (uint32_t i = 0; i < row_num; ++i) {
            sort_items[i] = {data[perm[i + offset].index_in_chunk], perm[i + offset].index_in_chunk, i};
        }

        if (is_asc_order) {
            pdqsort(state->cancelled_ref(), sort_items.begin(), sort_items.end(),
                    SortItemComparator<CppTypeName, true>());
        } else {
            pdqsort(state->cancelled_ref(), sort_items.begin(), sort_items.end(),
                    SortItemComparator<CppTypeName, false>());
        }
        RETURN_IF_CANCELLED(state);
        for (size_t i = 0; i < row_num; ++i) {
            perm[i + offset].index_in_chunk = sort_items[i].index_in_chunk;
        }
        return Status::OK();
    }

    template <PrimitiveType PT, bool stable>
    static Status _sort_by_permutation(RuntimeState* state, Column* column, bool is_asc_order, Permutation& perm,
                                       size_t offset, size_t count = 0) {
        size_t end_pos = std::min(perm.size(), (count == 0 ? perm.size() : offset + count));
        if (is_asc_order) {
            PermutationItemComparator<PT, true, stable> cmp(column, column);
            pdqsort(state->cancelled_ref(), perm.begin() + offset, perm.begin() + end_pos, cmp);
        } else {
            PermutationItemComparator<PT, false, stable> cmp(column, column);
            pdqsort(state->cancelled_ref(), perm.begin() + offset, perm.begin() + end_pos, cmp);
        }
        RETURN_IF_CANCELLED(state);
        return Status::OK();
    }
};

ChunksSorterFullSort::ChunksSorterFullSort(RuntimeState* state, const std::vector<ExprContext*>* sort_exprs,
                                           const std::vector<bool>* is_asc, const std::vector<bool>* is_null_first,
                                           size_t size_of_chunk_batch)
        : ChunksSorter(state, sort_exprs, is_asc, is_null_first, size_of_chunk_batch) {
    _selective_values.resize(_state->chunk_size());
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
    size_t count = std::min(size_t(_state->chunk_size()), _sorted_permutation.size() - _next_output_row);
    chunk->reset(_sorted_segment->chunk->clone_empty(count).release());
    _append_rows_to_chunk(chunk->get(), _sorted_segment->chunk.get(), _sorted_permutation, _next_output_row, count);
    _next_output_row += count;
}

DataSegment* ChunksSorterFullSort::get_result_data_segment() {
    return _sorted_segment.get();
}

uint64_t ChunksSorterFullSort::get_partition_rows() const {
    return _sorted_permutation.size();
}

// Is used to index sorted datas.
Permutation* ChunksSorterFullSort::get_permutation() const {
    return &_sorted_permutation;
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
    size_t count = std::min(size_t(_state->chunk_size()), _sorted_permutation.size() - _next_output_row);
    chunk->reset(_sorted_segment->chunk->clone_empty(count).release());
    _append_rows_to_chunk(chunk->get(), _sorted_segment->chunk.get(), _sorted_permutation, _next_output_row, count);
    _next_output_row += count;

    return _next_output_row >= _sorted_permutation.size();
}

int64_t ChunksSorterFullSort::mem_usage() const {
    int64_t usage = 0;
    if (_big_chunk != nullptr) {
        usage += _big_chunk->memory_usage();
    }
    if (_sorted_segment != nullptr) {
        usage += _sorted_segment->mem_usage();
    }
    usage += _sorted_permutation.capacity() * sizeof(Permutation);
    usage += _selective_values.capacity() * sizeof(uint32_t);
    return usage;
}

Status ChunksSorterFullSort::_sort_chunks(RuntimeState* state) {
    // Step1: construct permutation
    RETURN_IF_ERROR(_build_sorting_data(state));

    // Step2: sort by columns or row
    // For no more than three order-by columns, sorting by columns can benefit from reducing
    // the cost of calling virtual functions of Column::compare_at.
    CompareStrategy strategy = Default;
    if (_compare_strategy != Default) {
        strategy = _compare_strategy;
    } else {
        if (_get_number_of_order_by_columns() <= 3) {
            strategy = ColumnWise;
        } else {
            strategy = RowWise;
        }
    }
    if (strategy == ColumnWise) {
        RETURN_IF_ERROR(_sort_by_columns(state));
    } else {
        RETURN_IF_ERROR(_sort_by_row_cmp(state));
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
Status ChunksSorterFullSort::_sort_by_row_cmp(RuntimeState* state) {
    SCOPED_TIMER(_sort_timer);

    if (_get_number_of_order_by_columns() < 1) {
        return Status::OK();
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

    pdqsort(state->cancelled_ref(), indices.begin(), indices.end(), cmp_fn);
    RETURN_IF_CANCELLED(state);

    // Set the permutation array to sorted indices.
    for (size_t i = 0; i < elem_number; ++i) {
        _sorted_permutation[i].index_in_chunk = _sorted_permutation[i].permutation_index = indices[i];
    }
    return Status::OK();
}

// Sort in column style to avoid calling virtual methods of Column.
Status ChunksSorterFullSort::_sort_by_columns(RuntimeState* state) {
    SCOPED_TIMER(_sort_timer);

    if (_get_number_of_order_by_columns() < 1) {
        return Status::OK();
    }

    // TODO(mofei) optimize it with incremental sort
    for (int col_index = static_cast<int>(_get_number_of_order_by_columns()) - 1; col_index >= 0; --col_index) {
        Column* column = _sorted_segment->order_by_columns[col_index].get();
        if (column->is_constant()) {
            continue;
        }

        bool stable = col_index != _get_number_of_order_by_columns() - 1;
        bool is_asc_order = (_sort_order_flag[col_index] == 1);
        bool is_null_first;
        if (is_asc_order) {
            is_null_first = (_null_first_flag[col_index] == -1);
        } else {
            is_null_first = (_null_first_flag[col_index] == 1);
        }

        ExprContext* expr_ctx = (*_sort_exprs)[col_index];
        PrimitiveType sort_type = expr_ctx->root()->type().type;
        SortHelper::SingleColumnSortDesc sort_desc{sort_type, stable, column->is_nullable(), is_asc_order,
                                                   is_null_first};
        SortHelper::sort_single_column(state, column, sort_desc, _sorted_permutation);

        // reset permutation_index
        const size_t size = _sorted_permutation.size();
        for (size_t i = 0; i < size; ++i) {
            _sorted_permutation[i].permutation_index = i;
        }
    }
    return Status::OK();
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
