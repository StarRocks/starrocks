// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <type_traits>

#include "column/column_builder.h"
#include "column/type_traits.h"
#include "exec/vectorized/chunks_sorter.h"
#include "gutil/casts.h"
#include "runtime/primitive_type.h"
#include "runtime/primitive_type_infra.h"
#include "runtime/runtime_state.h"
#include "util/orlp/pdqsort.h"

namespace starrocks::vectorized {

class SortHelper {
public:
    struct SortDesc {
        PrimitiveType sort_type;
        bool nullable;
        bool stable;
        bool is_asc;
        bool null_first;
    };

    struct ColumnComparator {
        // Compare function pointer
        using CompareFunction =
                std::add_pointer_t<int(Column* lhs, Column* rhs, size_t lhs_index, size_t rhs_index, bool null_first)>;
        using CompareFunctions = std::vector<CompareFunction>;

        // Compare a single column, use down_cast to avoid virtual function call
        template <PrimitiveType ptype>
        static int compare(Column* lhs, Column* rhs, size_t lhs_index, size_t rhs_index, bool null_first) {
            using ColumnType = RunTimeColumnType<ptype>;
            auto lhs_column = down_cast<ColumnType*>(lhs);
            auto rhs_column = down_cast<ColumnType*>(rhs);
            return lhs_column->compare_at(lhs_index, rhs_index, *rhs_column, null_first);
        }

        // Compare multiple columns
        static int compare_multi_column(const Columns& lhs, const Columns& rhs, int lhs_row, int rhs_row,
                                        const CompareFunctions& cmp_funcs, const std::vector<SortDesc>& sort_descs) {
            for (int i = 0; i < lhs.size(); i++) {
                CompareFunction cmp = cmp_funcs[i];
                int x = cmp(lhs[i].get(), rhs[i].get(), lhs_row, rhs_row, sort_descs[i].null_first);
                if (x != 0) {
                    int order = sort_descs[i].is_asc ? 1 : -1;
                    return x * order;
                }
            }
            return 0;
        }

        // Build a compare function
        template <PrimitiveType ptype>
        CompareFunction operator()() {
            return &compare<ptype>;
        }

        // Create a comparator a single column
        static CompareFunction create_comparator(PrimitiveType ptype) {
            return type_dispatch_sortable(ptype, ColumnComparator());
        }

        // Create a comparator for multi columns
        static CompareFunctions create_comparator(const std::vector<SortDesc>& sort_desc) {
            std::vector<CompareFunction> res;
            res.reserve(sort_desc.size());
            for (auto desc : sort_desc) {
                res.emplace_back(create_comparator(desc.sort_type));
            }
            return res;
        }
    };

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

    struct SingleColumnSorter {
        template <PrimitiveType ptype>
        Status operator()(RuntimeState* state, Column* column, const SortDesc& desc, Permutation& perm) {
            if (desc.stable) {
                if (desc.nullable) {
                    return _sort_on_nullable_column<ptype, true>(state, column, desc.is_asc, desc.null_first, perm);
                } else {
                    return _sort_on_not_null_column<ptype, true>(state, column, desc.is_asc, perm);
                }
            } else {
                if (desc.nullable) {
                    return _sort_on_nullable_column<ptype, false>(state, column, desc.is_asc, desc.null_first, perm);
                } else {
                    return _sort_on_not_null_column<ptype, false>(state, column, desc.is_asc, perm);
                }
            }
            return Status::OK();
        }
    };

    // Sort a single column
    static Status sort_single_column(RuntimeState* state, Column* column, const SortDesc& desc, Permutation& perm) {
        return type_dispatch_sortable(desc.sort_type, SingleColumnSorter(), state, column, desc, perm);
    }

    // Sort multiple columns, employ the LSD-like sort algorithm: sort column-wise from right to left
    // TODO(mofei) optimize it with incremental sort
    static Status sort_multi_column(RuntimeState* state, Columns columns, const std::vector<SortDesc>& descs,
                                    Permutation& perm) {
        int num_columns = descs.size();
        if (num_columns < 1) {
            return Status::OK();
        }

        for (int col_index = num_columns - 1; col_index >= 0; col_index--) {
            Column* column = columns[col_index].get();
            if (column->is_constant()) {
                continue;
            }
            SortDesc desc = descs[col_index];
            desc.nullable = column->is_nullable();
            desc.stable = col_index != num_columns - 1;

            RETURN_IF_ERROR(sort_single_column(state, column, desc, perm));

            // reset permutation_index
            const size_t size = perm.size();
            for (size_t i = 0; i < size; ++i) {
                perm[i].permutation_index = i;
            }
        }

        return Status::OK();
    }

    // Sort a DataSegment by row-wise compare
    static Status sort_multi_column_rowwise(RuntimeState* state, DataSegment& data_segment,
                                            const std::vector<SortDesc>& sort_descs, Permutation& perm) {
        int num_columns = data_segment.order_by_columns.size();
        if (num_columns < 1) {
            return Status::OK();
        }

        // In this case, PermutationItem::chunk_index is constantly 0,
        // and PermutationItem::index_in_chunk is always equal to PermutationItem::permutation_index,
        // which is the sequence index of the element in the array.
        // This simplified index array can help the sort routine to get a better performance.
        const size_t elem_number = perm.size();
        std::vector<size_t> indices(elem_number);
        std::iota(indices.begin(), indices.end(), 0);

        ColumnComparator::CompareFunctions cmp_funcs = ColumnComparator::create_comparator(sort_descs);

        // TODO(mofei) simplify the compare
        auto cmp_fn = [&data_segment, &sort_descs, &cmp_funcs](const size_t& l, const size_t& r) {
            int c = compare_datasegment(data_segment, data_segment, l, r, sort_descs, cmp_funcs);
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
            perm[i].index_in_chunk = perm[i].permutation_index = indices[i];
        }
        return Status::OK();
    }

    // Return value:
    //  < 0: current row precedes the row in the other chunk;
    // == 0: current row is equal to the row in the other chunk;
    //  > 0: current row succeeds the row in the other chunk;
    static int compare_datasegment(const DataSegment& lhs, const DataSegment& rhs, size_t lhs_index, size_t rhs_index,
                                   const std::vector<SortDesc>& sort_descs,
                                   const ColumnComparator::CompareFunctions& cmp_funcs) {
        DCHECK_EQ(lhs.order_by_columns.size(), rhs.order_by_columns.size());

        return ColumnComparator::compare_multi_column(lhs.order_by_columns, rhs.order_by_columns, lhs_index, rhs_index,
                                                      cmp_funcs, sort_descs);
    }

private:
    // Sort on type-known column, and the column has no NULL value in sorting range.
    template <PrimitiveType PT, bool stable>
    static Status _sort_on_not_null_column(RuntimeState* state, Column* column, bool is_asc_order, Permutation& perm) {
        return _sort_on_not_null_column_within_range<PT, stable>(state, column, is_asc_order, perm, 0, perm.size());
    }

    // Sort on type-known column, and the column may have NULL values in the sorting range.
    template <PrimitiveType PT, bool stable>
    static Status _sort_on_nullable_column(RuntimeState* state, Column* column, bool is_asc_order, bool is_null_first,
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
        return _sort_on_not_null_column_within_range<PT, stable>(state, nullable_col->mutable_data_column(),
                                                                 is_asc_order, perm, data_offset, data_count);
    }

    // Sort on type-known column, and the column has no NULL value in sorting range.
    template <PrimitiveType PT, bool stable>
    static Status _sort_on_not_null_column_within_range(RuntimeState* state, Column* column, bool is_asc_order,
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

} // namespace starrocks::vectorized