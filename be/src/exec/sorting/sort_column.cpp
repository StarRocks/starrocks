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

#include <algorithm>
#include <utility>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "column/column_visitor_adapter.h"
#include "column/const_column.h"
#include "column/fixed_length_column_base.h"
#include "column/json_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "exec/sorting/sort_helper.h"
#include "exec/sorting/sort_permute.h"
#include "exec/sorting/sorting.h"
#include "util/orlp/pdqsort.h"

namespace starrocks {

// Sort a column by permtuation
class ColumnSorter final : public ColumnVisitorAdapter<ColumnSorter> {
public:
    explicit ColumnSorter(const std::atomic<bool>& cancel, const SortDesc& sort_desc, SmallPermutation& permutation,
                          Tie& tie, std::pair<int, int> range, bool build_tie)
            : ColumnVisitorAdapter(this),
              _cancel(cancel),
              _sort_desc(sort_desc),
              _permutation(permutation),
              _tie(tie),
              _range(std::move(range)),
              _build_tie(build_tie) {}

    Status do_visit(const NullableColumn& column) {
        // Fastpath
        if (!column.has_null()) {
            return column.data_column_ref().accept(this);
        }

        const NullData& null_data = column.immutable_null_column_data();
        auto null_pred = [&](const SmallPermuteItem& item) -> bool {
            if (_sort_desc.is_null_first()) {
                return null_data[item.index_in_chunk] == 1;
            } else {
                return null_data[item.index_in_chunk] != 1;
            }
        };

        return sort_and_tie_helper_nullable(_cancel, &column, column.data_column(), null_pred, _sort_desc, _permutation,
                                            _tie, _range, _build_tie);
    }

    Status do_visit(const ConstColumn& column) {
        // noop
        return Status::OK();
    }

    Status do_visit(const ArrayColumn& column) {
        auto cmp = [&](const SmallPermuteItem& lhs, const SmallPermuteItem& rhs) {
            return column.compare_at(lhs.index_in_chunk, rhs.index_in_chunk, column, _sort_desc.nan_direction());
        };

        return sort_and_tie_helper(_cancel, &column, _sort_desc.asc_order(), _permutation, _tie, cmp, _range,
                                   _build_tie);
    }

    Status do_visit(const MapColumn& column) {
        auto cmp = [&](const SmallPermuteItem& lhs, const SmallPermuteItem& rhs) {
            return column.compare_at(lhs.index_in_chunk, rhs.index_in_chunk, column, _sort_desc.nan_direction());
        };

        return sort_and_tie_helper(_cancel, &column, _sort_desc.asc_order(), _permutation, _tie, cmp, _range,
                                   _build_tie);
    }

    Status do_visit(const StructColumn& column) {
        auto cmp = [&](const SmallPermuteItem& lhs, const SmallPermuteItem& rhs) {
            return column.compare_at(lhs.index_in_chunk, rhs.index_in_chunk, column, _sort_desc.nan_direction());
        };

        return sort_and_tie_helper(_cancel, &column, _sort_desc.asc_order(), _permutation, _tie, cmp, _range,
                                   _build_tie);
    }

    template <typename T>
    Status do_visit(const BinaryColumnBase<T>& column) {
        DCHECK_GE(column.size(), _permutation.size());
        using ItemType = InlinePermuteItem<Slice>;
        auto cmp = [&](const ItemType& lhs, const ItemType& rhs) -> int {
            return lhs.inline_value.compare(rhs.inline_value);
        };

        auto inlined = create_inline_permutation<Slice>(_permutation, column.get_proxy_data());
        RETURN_IF_ERROR(
                sort_and_tie_helper(_cancel, &column, _sort_desc.asc_order(), inlined, _tie, cmp, _range, _build_tie));
        restore_inline_permutation(inlined, _permutation);

        return Status::OK();
    }

    template <typename T>
    Status do_visit(const FixedLengthColumnBase<T>& column) {
        DCHECK_GE(column.size(), _permutation.size());
        using ItemType = InlinePermuteItem<T>;

        auto cmp = [&](const ItemType& lhs, const ItemType& rhs) {
            return SorterComparator<T>::compare(lhs.inline_value, rhs.inline_value);
        };

        auto inlined = create_inline_permutation<T>(_permutation, column.get_data());
        RETURN_IF_ERROR(
                sort_and_tie_helper(_cancel, &column, _sort_desc.asc_order(), inlined, _tie, cmp, _range, _build_tie));
        restore_inline_permutation(inlined, _permutation);

        return Status::OK();
    }

    template <typename T>
    Status do_visit(const ObjectColumn<T>& column) {
        DCHECK(false) << "not support object column sort_and_tie";

        return Status::NotSupported("not support object column sort_and_tie");
    }

    Status do_visit(const JsonColumn& column) {
        auto cmp = [&](const SmallPermuteItem& lhs, const SmallPermuteItem& rhs) {
            return column.get_object(lhs.index_in_chunk)->compare(*column.get_object(rhs.index_in_chunk));
        };

        return sort_and_tie_helper(_cancel, &column, _sort_desc.asc_order(), _permutation, _tie, cmp, _range,
                                   _build_tie);
    }

private:
    const std::atomic<bool>& _cancel;
    const SortDesc& _sort_desc;
    SmallPermutation& _permutation;
    Tie& _tie;
    std::pair<int, int> _range;
    bool _build_tie;
};

// Sort multiple a column from multiple chunks(vertical column)
class VerticalColumnSorter final : public ColumnVisitorAdapter<VerticalColumnSorter> {
public:
    explicit VerticalColumnSorter(const std::atomic<bool>& cancel, std::vector<ColumnPtr> columns,
                                  const SortDesc& sort_desc, Permutation& permutation, Tie& tie,
                                  std::pair<int, int> range, bool build_tie, size_t limit)
            : ColumnVisitorAdapter(this),
              _cancel(cancel),
              _sort_desc(sort_desc),
              _vertical_columns(std::move(columns)),
              _permutation(permutation),
              _tie(tie),
              _range(std::move(range)),
              _build_tie(build_tie),
              _limit(limit),
              _pruned_limit(permutation.size()) {}

    size_t get_limited() const { return _pruned_limit; }

    Status do_visit(const NullableColumn& column) {
        std::vector<const NullData*> null_datas;
        std::vector<ColumnPtr> data_columns;
        for (auto& col : _vertical_columns) {
            auto real = down_cast<const NullableColumn*>(col.get());
            null_datas.push_back(&real->immutable_null_column_data());
            data_columns.push_back(real->data_column());
        }

        if (_sort_desc.is_null_first()) {
            auto null_pred = [&](const PermutationItem& item) -> bool {
                return (*null_datas[item.chunk_index])[item.index_in_chunk] == 1;
            };

            RETURN_IF_ERROR(sort_and_tie_helper_nullable_vertical(_cancel, data_columns, null_pred, _sort_desc,
                                                                  _permutation, _tie, _range, _build_tie, _limit,
                                                                  &_pruned_limit));
        } else {
            auto null_pred = [&](const PermutationItem& item) -> bool {
                return (*null_datas[item.chunk_index])[item.index_in_chunk] != 1;
            };

            RETURN_IF_ERROR(sort_and_tie_helper_nullable_vertical(_cancel, data_columns, null_pred, _sort_desc,
                                                                  _permutation, _tie, _range, _build_tie, _limit,
                                                                  &_pruned_limit));
        }

        _prune_limit();

        return Status::OK();
    }

    template <typename T>
    Status do_visit(const BinaryColumnBase<T>& column) {
        using ColumnType = BinaryColumnBase<T>;

        if (_need_inline_value()) {
            using ItemType = CompactChunkItem<Slice>;
            using Container = typename BinaryColumnBase<T>::BinaryDataProxyContainer;

            auto cmp = [&](const ItemType& lhs, const ItemType& rhs) -> int {
                return lhs.inline_value.compare(rhs.inline_value);
            };

            std::vector<const Container*> containers;
            for (const auto& col : _vertical_columns) {
                const auto real = down_cast<const ColumnType*>(col.get());
                containers.push_back(&real->get_proxy_data());
            }

            auto inlined = _create_inlined_permutation<Slice>(containers);
            RETURN_IF_ERROR(sort_and_tie_helper(_cancel, &column, _sort_desc.asc_order(), inlined, _tie, cmp, _range,
                                                _build_tie, _limit, &_pruned_limit));
            _restore_inlined_permutation(inlined);
        } else {
            auto cmp = [&](const PermutationItem& lhs, const PermutationItem& rhs) {
                auto left_column = down_cast<const ColumnType*>(_vertical_columns[lhs.chunk_index].get());
                auto right_column = down_cast<const ColumnType*>(_vertical_columns[rhs.chunk_index].get());
                auto left_value = left_column->get_slice(lhs.index_in_chunk);
                auto right_value = right_column->get_slice(rhs.index_in_chunk);
                return SorterComparator<Slice>::compare(left_value, right_value);
            };

            RETURN_IF_ERROR(sort_and_tie_helper(_cancel, &column, _sort_desc.asc_order(), _permutation, _tie, cmp,
                                                _range, _build_tie, _limit, &_pruned_limit));
        }
        _prune_limit();
        return Status::OK();
    }

    template <typename T>
    Status do_visit(const FixedLengthColumnBase<T>& column) {
        using ColumnType = FixedLengthColumnBase<T>;
        using Container = typename FixedLengthColumnBase<T>::Container;

        if (_need_inline_value()) {
            using ItemType = CompactChunkItem<T>;
            auto cmp = [&](const ItemType& lhs, const ItemType& rhs) {
                return SorterComparator<T>::compare(lhs.inline_value, rhs.inline_value);
            };
            std::vector<const Container*> containers;
            for (const auto& col : _vertical_columns) {
                const auto real = down_cast<const ColumnType*>(col.get());
                containers.emplace_back(&real->get_data());
            }
            auto inlined = _create_inlined_permutation<T>(containers);
            RETURN_IF_ERROR(sort_and_tie_helper(_cancel, &column, _sort_desc.asc_order(), inlined, _tie, cmp, _range,
                                                _build_tie, _limit, &_pruned_limit));
            _restore_inlined_permutation(inlined);
        } else {
            using ItemType = PermutationItem;
            auto cmp = [&](const ItemType& lhs, const ItemType& rhs) {
                auto left_column = down_cast<const ColumnType*>(_vertical_columns[lhs.chunk_index].get());
                auto right_column = down_cast<const ColumnType*>(_vertical_columns[rhs.chunk_index].get());
                auto left_value = left_column->get_data()[lhs.index_in_chunk];
                auto right_value = right_column->get_data()[rhs.index_in_chunk];
                return SorterComparator<T>::compare(left_value, right_value);
            };

            RETURN_IF_ERROR(sort_and_tie_helper(_cancel, &column, _sort_desc.asc_order(), _permutation, _tie, cmp,
                                                _range, _build_tie, _limit, &_pruned_limit));
        }
        _prune_limit();
        return Status::OK();
    }

    Status do_visit(const ConstColumn& column) {
        // noop
        return Status::OK();
    }

    Status do_visit(const ArrayColumn& column) {
        auto cmp = [&](const PermutationItem& lhs, const PermutationItem& rhs) {
            auto& lhs_col = _vertical_columns[lhs.chunk_index];
            auto& rhs_col = _vertical_columns[rhs.chunk_index];
            return lhs_col->compare_at(lhs.index_in_chunk, rhs.index_in_chunk, *rhs_col, _sort_desc.nan_direction());
        };

        RETURN_IF_ERROR(sort_and_tie_helper(_cancel, &column, _sort_desc.asc_order(), _permutation, _tie, cmp, _range,
                                            _build_tie, _limit, &_pruned_limit));
        _prune_limit();
        return Status::OK();
    }

    Status do_visit(const MapColumn& column) {
        auto cmp = [&](const PermutationItem& lhs, const PermutationItem& rhs) {
            auto& lhs_col = _vertical_columns[lhs.chunk_index];
            auto& rhs_col = _vertical_columns[rhs.chunk_index];
            return lhs_col->compare_at(lhs.index_in_chunk, rhs.index_in_chunk, *rhs_col, _sort_desc.nan_direction());
        };

        RETURN_IF_ERROR(sort_and_tie_helper(_cancel, &column, _sort_desc.asc_order(), _permutation, _tie, cmp, _range,
                                            _build_tie, _limit, &_pruned_limit));
        _prune_limit();
        return Status::OK();
    }

    Status do_visit(const StructColumn& column) {
        // TODO(SmithCruise)
        return Status::NotSupported("Not support");
    }

    template <typename T>
    Status do_visit(const ObjectColumn<T>& column) {
        DCHECK(false) << "not supported";
        return Status::NotSupported("TOOD");
    }

    Status do_visit(const JsonColumn& column) {
        auto cmp = [&](const PermutationItem& lhs, const PermutationItem& rhs) {
            auto& lhs_col = _vertical_columns[lhs.chunk_index];
            auto& rhs_col = _vertical_columns[rhs.chunk_index];
            return lhs_col->compare_at(lhs.index_in_chunk, rhs.index_in_chunk, *rhs_col, _sort_desc.nan_direction());
        };

        RETURN_IF_ERROR(sort_and_tie_helper(_cancel, &column, _sort_desc.asc_order(), _permutation, _tie, cmp, _range,
                                            _build_tie, _limit, &_pruned_limit));
        _prune_limit();
        return Status::OK();
    }

private:
    template <class T>
    struct CompactChunkItem {
        uint32_t chunk_index;
        uint32_t index_in_chunk;

        T inline_value;
    };

    template <class T>
    using CompactChunkPermutation = std::vector<CompactChunkItem<T>>;

    bool _need_inline_value() {
        // TODO: figure out the inflection point
        // If limit exceeds 1/5 rows, inline will has benefits
        size_t total_rows = _permutation.size();
        return _limit > (total_rows / 5);
    }

    template <class T, class DataContainer>
    CompactChunkPermutation<T> _create_inlined_permutation(const std::vector<DataContainer>& containers) {
        CompactChunkPermutation<T> result(_permutation.size());
        for (size_t i = 0; i < _permutation.size(); i++) {
            int chunk_index = _permutation[i].chunk_index;
            int index_in_chunk = _permutation[i].index_in_chunk;
            result[i].chunk_index = chunk_index;
            result[i].index_in_chunk = index_in_chunk;
            result[i].inline_value = (*containers[chunk_index])[index_in_chunk];
        }
        return result;
    }

    template <class T>
    void _restore_inlined_permutation(const CompactChunkPermutation<T> inlined) {
        size_t n = std::min(inlined.size(), _pruned_limit);
        for (size_t i = 0; i < n; i++) {
            _permutation[i].chunk_index = inlined[i].chunk_index;
            _permutation[i].index_in_chunk = inlined[i].index_in_chunk;
        }
    }

    void _prune_limit() {
        if (_pruned_limit < _permutation.size()) {
            _permutation.resize(_pruned_limit);
            _tie.resize(_pruned_limit);
        }
    }

private:
    const std::atomic<bool>& _cancel;
    const SortDesc& _sort_desc;

    std::vector<ColumnPtr> _vertical_columns;
    Permutation& _permutation;
    Tie& _tie;
    std::pair<int, int> _range;
    bool _build_tie;
    size_t _limit;        // The requested limit
    size_t _pruned_limit; // The pruned limit during partial sorting
};

Status sort_and_tie_column(const std::atomic<bool>& cancel, const ColumnPtr& column, const SortDesc& sort_desc,
                           SmallPermutation& permutation, Tie& tie, std::pair<int, int> range, bool build_tie) {
    ColumnSorter column_sorter(cancel, sort_desc, permutation, tie, range, build_tie);
    return column->accept(&column_sorter);
}

Status sort_and_tie_column(const std::atomic<bool>& cancel, ColumnPtr& column, const SortDesc& sort_desc,
                           SmallPermutation& permutation, Tie& tie, std::pair<int, int> range, bool build_tie) {
    if (column->is_nullable() && !column->is_constant()) {
        ColumnHelper::as_column<NullableColumn>(column)->fill_null_with_default();
    }
    ColumnSorter column_sorter(cancel, sort_desc, permutation, tie, range, build_tie);
    return column->accept(&column_sorter);
}

Status sort_and_tie_columns(const std::atomic<bool>& cancel, const Columns& columns, const SortDescs& sort_desc,
                            Permutation* permutation) {
    if (columns.size() < 1) {
        return Status::OK();
    }
    size_t num_rows = columns[0]->size();
    Tie tie(num_rows, 1);
    std::pair<int, int> range{0, num_rows};
    SmallPermutation small_perm = create_small_permutation(num_rows);

    for (int col_index = 0; col_index < columns.size(); col_index++) {
        ColumnPtr column = columns[col_index];
        bool build_tie = col_index != columns.size() - 1;
        RETURN_IF_ERROR(sort_and_tie_column(cancel, column, sort_desc.get_column_desc(col_index), small_perm, tie,
                                            range, build_tie));
    }

    restore_small_permutation(small_perm, *permutation);

    return Status::OK();
}

Status stable_sort_and_tie_columns(const std::atomic<bool>& cancel, const Columns& columns, const SortDescs& sort_desc,
                                   SmallPermutation* small_perm) {
    if (columns.size() < 1) {
        return Status::OK();
    }
    size_t num_rows = columns[0]->size();
    DCHECK_EQ(num_rows, small_perm->size());
    Tie tie(num_rows, 1);
    std::pair<int, int> range{0, num_rows};

    for (int col_index = 0; col_index < columns.size(); col_index++) {
        ColumnPtr column = columns[col_index];
        RETURN_IF_ERROR(sort_and_tie_column(cancel, column, sort_desc.get_column_desc(col_index), *small_perm, tie,
                                            range, true));
    }

    // Stable sort need extra runs of sorting on permutation
    TieIterator ti(tie);
    while (ti.next()) {
        int range_first = ti.range_first, range_last = ti.range_last;
        if (range_last - range_first > 1) {
            ::pdqsort(
                    small_perm->begin() + range_first, small_perm->begin() + range_last,
                    [](SmallPermuteItem lhs, SmallPermuteItem rhs) { return lhs.index_in_chunk < rhs.index_in_chunk; });
        }
    }

    return Status::OK();
}

Status sort_vertical_columns(const std::atomic<bool>& cancel, const std::vector<ColumnPtr>& columns,
                             const SortDesc& sort_desc, Permutation& permutation, Tie& tie, std::pair<int, int> range,
                             const bool build_tie, const size_t limit, size_t* limited) {
    DCHECK_GT(columns.size(), 0);
    DCHECK_GT(permutation.size(), 0);

    for (auto& col : columns) {
        if (col->is_nullable() && !col->is_constant()) {
            ColumnHelper::as_column<NullableColumn>(col)->fill_null_with_default();
        }
    }

    VerticalColumnSorter sorter(cancel, columns, sort_desc, permutation, tie, range, build_tie, limit);
    RETURN_IF_ERROR(columns[0]->accept(&sorter));
    if (limit > 0 && limited != nullptr) {
        *limited = std::min(*limited, sorter.get_limited());
    }
    return Status::OK();
}

Status sort_vertical_chunks(const std::atomic<bool>& cancel, const std::vector<Columns>& vertical_chunks,
                            const SortDescs& sort_desc, Permutation& perm, const size_t limit,
                            const bool is_limit_by_rank) {
    if (vertical_chunks.empty() || perm.empty()) {
        return Status::OK();
    }

    size_t num_columns = vertical_chunks[0].size();
    size_t num_rows = perm.size();
    Tie tie(num_rows, 1);

    DCHECK_EQ(num_columns, sort_desc.num_columns());

    for (int col = 0; col < num_columns; col++) {
        // TODO: use the flag directly
        bool build_tie = col != num_columns - 1;
        std::pair<int, int> range(0, perm.size());
        DCHECK_GT(perm.size(), 0);

        std::vector<ColumnPtr> vertical_columns;
        vertical_columns.reserve(vertical_chunks.size());
        for (const auto& columns : vertical_chunks) {
            vertical_columns.push_back(columns[col]);
        }

        RETURN_IF_ERROR(sort_vertical_columns(cancel, vertical_columns, sort_desc.get_column_desc(col), perm, tie,
                                              range, build_tie, limit));
    }

    if (limit < perm.size()) {
        if (is_limit_by_rank) {
            // Given that
            // * number array `[1, 1, 1, 1, 1, 1, 1, 2, 2, 3, 4, 5]`
            // * rank array `[1, 1, 1, 1, 1, 1, 1, 8, 8, 10, 11, 12]`
            // * tie array `[1, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, 1]`
            //
            // Suppose we need to find rank-5, the 5-th of number array is `1`, so we need to consider the trailing equal values,
            // but how to find the end of the trailing equal values? Use the tie structure, we can find the first non-equal value by `SIMD::find_zero(tie, limit)`.
            // In this case, the first non-equals value is 2, index is 7, so we can get the limied array through truncating by 7
            int first_non_equal_pos = SIMD::find_zero(tie, limit);
            if (first_non_equal_pos < perm.size()) {
                perm.resize(first_non_equal_pos);
            }
        } else {
            perm.resize(limit);
        }
    }

    return Status::OK();
}

} // namespace starrocks
