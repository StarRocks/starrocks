// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column.h"
#include "column/column_visitor_adapter.h"
#include "column/const_column.h"
#include "column/fixed_length_column_base.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "exec/vectorized/sorting/sort_helper.h"
#include "exec/vectorized/sorting/sort_permute.h"
#include "exec/vectorized/sorting/sorting.h"
#include "util/orlp/pdqsort.h"

namespace starrocks::vectorized {

class ColumnSorter final : public ColumnVisitorAdapter<ColumnSorter> {
public:
    explicit ColumnSorter(const bool& cancel, bool is_asc_order, bool is_null_first, SmallPermutation& permutation,
                          Tie& tie, std::pair<int, int> range, bool build_tie)
            : ColumnVisitorAdapter(this),
              _cancel(cancel),
              _is_asc_order(is_asc_order),
              _is_null_first(is_null_first),
              _permutation(permutation),
              _tie(tie),
              _range(range),
              _build_tie(build_tie) {}

    Status do_visit(const vectorized::NullableColumn& column) {
        const NullData& null_data = column.immutable_null_column_data();
        auto null_pred = [&](const SmallPermuteItem& item) -> bool {
            if (_is_null_first) {
                return null_data[item.index_in_chunk] == 1;
            } else {
                return null_data[item.index_in_chunk] != 1;
            }
        };

        return sort_and_tie_helper_nullable(_cancel, &column, column.data_column(), null_pred, _is_asc_order,
                                            _is_null_first, _permutation, _tie, _range, _build_tie);
    }

    Status do_visit(const vectorized::ConstColumn& column) {
        // noop
        return Status::OK();
    }

    Status do_visit(const vectorized::ArrayColumn& column) {
        auto cmp = [&](const SmallPermuteItem& lhs, const SmallPermuteItem& rhs) {
            return column.compare_at(lhs.index_in_chunk, rhs.index_in_chunk, column, _is_null_first ? -1 : 1);
        };

        return sort_and_tie_helper(_cancel, &column, _is_asc_order, _permutation, _tie, cmp, _range, _build_tie);
    }

    Status do_visit(const vectorized::BinaryColumn& column) {
        DCHECK_GE(column.size(), _permutation.size());
        using ItemType = InlinePermuteItem<Slice>;
        auto cmp = [&](const ItemType& lhs, const ItemType& rhs) -> int {
            return lhs.inline_value.compare(rhs.inline_value);
        };

        auto inlined = create_inline_permutation<Slice>(_permutation, column.get_data());
        RETURN_IF_ERROR(sort_and_tie_helper(_cancel, &column, _is_asc_order, inlined, _tie, cmp, _range, _build_tie));
        restore_inline_permutation(inlined, _permutation);

        return Status::OK();
    }

    template <typename T>
    Status do_visit(const vectorized::FixedLengthColumnBase<T>& column) {
        DCHECK_GE(column.size(), _permutation.size());
        using ItemType = InlinePermuteItem<T>;

        auto cmp = [&](const ItemType& lhs, const ItemType& rhs) {
            return SorterComparator<T>::compare(lhs.inline_value, rhs.inline_value);
        };

        auto inlined = create_inline_permutation<T>(_permutation, column.get_data());
        RETURN_IF_ERROR(sort_and_tie_helper(_cancel, &column, _is_asc_order, inlined, _tie, cmp, _range, _build_tie));
        restore_inline_permutation(inlined, _permutation);

        return Status::OK();
    }

    template <typename T>
    Status do_visit(const vectorized::ObjectColumn<T>& column) {
        DCHECK(false) << "not support object column sort_and_tie";

        return Status::NotSupported("not support object column sort_and_tie");
    }

    Status do_visit(const vectorized::JsonColumn& column) {
        auto cmp = [&](const SmallPermuteItem& lhs, const SmallPermuteItem& rhs) {
            return column.get_object(lhs.index_in_chunk)->compare(*column.get_object(rhs.index_in_chunk));
        };

        return sort_and_tie_helper(_cancel, &column, _is_asc_order, _permutation, _tie, cmp, _range, _build_tie);
    }

private:
    const bool& _cancel;
    bool _is_asc_order;
    bool _is_null_first;
    SmallPermutation& _permutation;
    Tie& _tie;
    std::pair<int, int> _range;
    bool _build_tie;
};

class VerticalColumnSorter final : public ColumnVisitorAdapter<VerticalColumnSorter> {
public:
    explicit VerticalColumnSorter(const bool& cancel, const std::vector<ColumnPtr>& columns, bool is_asc_order,
                                  bool is_null_first, Permutation& permutation, Tie& tie, std::pair<int, int> range,
                                  bool build_tie)
            : ColumnVisitorAdapter(this),
              _cancel(cancel),
              _is_asc_order(is_asc_order),
              _is_null_first(is_null_first),
              _vertical_columns(columns),
              _permutation(permutation),
              _tie(tie),
              _range(range),
              _build_tie(build_tie) {}

    Status do_visit(const vectorized::NullableColumn& column) {
        // TODO: optimize it
        std::vector<NullData> null_datas;
        std::vector<ColumnPtr> data_columns;
        for (auto& col : _vertical_columns) {
            auto real = down_cast<const NullableColumn*>(col.get());
            null_datas.push_back(real->immutable_null_column_data());
            data_columns.push_back(real->data_column());
        }

        auto null_pred = [&](const PermutationItem& item) -> bool {
            if (_is_null_first) {
                return null_datas[item.chunk_index][item.index_in_chunk] == 1;
            } else {
                return null_datas[item.chunk_index][item.index_in_chunk] != 1;
            }
        };

        return sort_and_tie_helper_nullable_vertical(_cancel, data_columns, null_pred, _is_asc_order, _is_null_first,
                                                     _permutation, _tie, _range, _build_tie);
    }

    Status do_visit(const vectorized::BinaryColumn& column) {
        using ItemType = InlineChunkItem<Slice>;
        using Container = std::vector<Slice>;

        auto cmp = [&](const ItemType& lhs, const ItemType& rhs) -> int {
            return lhs.inline_value.compare(rhs.inline_value);
        };

        std::vector<const Container*> containers;
        for (const auto& col : _vertical_columns) {
            const auto real = down_cast<const BinaryColumn*>(col.get());
            containers.push_back(&real->get_data());
        }

        auto inlined = _create_inlined_permutation<Slice>(containers);
        RETURN_IF_ERROR(sort_and_tie_helper(_cancel, &column, _is_asc_order, inlined, _tie, cmp, _range, _build_tie));
        _restore_inlined_permutation(inlined);

        return Status::OK();
    }

    template <typename T>
    Status do_visit(const vectorized::FixedLengthColumnBase<T>& column) {
        using ColumnType = FixedLengthColumnBase<T>;
        using ItemType = InlineChunkItem<T>;
        using Container = typename FixedLengthColumnBase<T>::Container;

        auto cmp = [&](const ItemType& lhs, const ItemType& rhs) {
            return SorterComparator<T>::compare(lhs.inline_value, rhs.inline_value);
        };

        std::vector<const Container*> containers;
        for (const auto& col : _vertical_columns) {
            const auto real = down_cast<const ColumnType*>(col.get());
            containers.emplace_back(&real->get_data());
        }
        auto inlined = _create_inlined_permutation<T>(containers);
        RETURN_IF_ERROR(sort_and_tie_helper(_cancel, &column, _is_asc_order, inlined, _tie, cmp, _range, _build_tie));
        _restore_inlined_permutation(inlined);

        return Status::OK();
    }

    Status do_visit(const vectorized::ConstColumn& column) {
        // noop
        return Status::OK();
    }

    Status do_visit(const vectorized::ArrayColumn& column) {
        auto cmp = [&](const PermutationItem& lhs, const PermutationItem& rhs) {
            auto& lhs_col = _vertical_columns[lhs.chunk_index];
            auto& rhs_col = _vertical_columns[rhs.chunk_index];
            return lhs_col->compare_at(lhs.index_in_chunk, rhs.index_in_chunk, *rhs_col, _is_null_first ? -1 : 1);
        };

        return sort_and_tie_helper(_cancel, &column, _is_asc_order, _permutation, _tie, cmp, _range, _build_tie);
    }

    template <typename T>
    Status do_visit(const vectorized::ObjectColumn<T>& column) {
        DCHECK(false) << "not supported";
        return Status::NotSupported("TOOD");
    }

    Status do_visit(const vectorized::JsonColumn& column) {
        auto cmp = [&](const PermutationItem& lhs, const PermutationItem& rhs) {
            auto& lhs_col = _vertical_columns[lhs.chunk_index];
            auto& rhs_col = _vertical_columns[rhs.chunk_index];
            return lhs_col->compare_at(lhs.index_in_chunk, rhs.index_in_chunk, *rhs_col, _is_null_first ? -1 : 1);
        };

        return sort_and_tie_helper(_cancel, &column, _is_asc_order, _permutation, _tie, cmp, _range, _build_tie);
    }

private:
    template <class T>
    struct InlineChunkItem {
        uint32_t chunk_index;
        uint32_t index_in_chunk;

        T inline_value;
    };

    template <class T>
    using InlinedChunkPermutation = std::vector<InlineChunkItem<T>>;

    template <class T, class DataContainer>
    InlinedChunkPermutation<T> _create_inlined_permutation(const std::vector<DataContainer>& containers) {
        InlinedChunkPermutation<T> result(_permutation.size());
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
    void _restore_inlined_permutation(const InlinedChunkPermutation<T> inlined) {
        for (size_t i = 0; i < inlined.size(); i++) {
            _permutation[i].chunk_index = inlined[i].chunk_index;
            _permutation[i].index_in_chunk = inlined[i].index_in_chunk;
        }
    }

    const bool& _cancel;
    const bool _is_asc_order;
    const bool _is_null_first;

    std::vector<ColumnPtr> _vertical_columns;
    Permutation& _permutation;
    Tie& _tie;
    std::pair<int, int> _range;
    bool _build_tie;
};

Status sort_and_tie_column(const bool& cancel, const ColumnPtr column, bool is_asc_order, bool is_null_first,
                           SmallPermutation& permutation, Tie& tie, std::pair<int, int> range, bool build_tie) {
    ColumnSorter column_sorter(cancel, is_asc_order, is_null_first, permutation, tie, range, build_tie);
    return column->accept(&column_sorter);
}

Status sort_and_tie_columns(const bool& cancel, const Columns& columns, const std::vector<int>& sort_orders,
                            const std::vector<int>& null_firsts, Permutation* permutation) {
    if (columns.size() < 1) {
        return Status::OK();
    }
    size_t num_rows = columns[0]->size();
    Tie tie(num_rows, 1);
    std::pair<int, int> range{0, num_rows};
    SmallPermutation small_perm = create_small_permutation(num_rows);

    for (int col_index = 0; col_index < columns.size(); col_index++) {
        ColumnPtr column = columns[col_index];
        bool is_asc_order = (sort_orders[col_index] == 1);
        bool is_null_first = is_asc_order ? (null_firsts[col_index] == -1) : (null_firsts[col_index] == 1);
        bool build_tie = col_index != columns.size() - 1;
        RETURN_IF_ERROR(
                sort_and_tie_column(cancel, column, is_asc_order, is_null_first, small_perm, tie, range, build_tie));
    }

    restore_small_permutation(small_perm, *permutation);

    return Status::OK();
}

Status stable_sort_and_tie_columns(const bool& cancel, const Columns& columns, const std::vector<int>& sort_orders,
                                   const std::vector<int>& null_firsts, SmallPermutation* small_perm) {
    if (columns.size() < 1) {
        return Status::OK();
    }
    size_t num_rows = columns[0]->size();
    DCHECK_EQ(num_rows, small_perm->size());
    Tie tie(num_rows, 1);
    std::pair<int, int> range{0, num_rows};

    for (int col_index = 0; col_index < columns.size(); col_index++) {
        ColumnPtr column = columns[col_index];
        bool is_asc_order = (sort_orders[col_index] == 1);
        bool is_null_first = is_asc_order ? (null_firsts[col_index] == -1) : (null_firsts[col_index] == 1);
        RETURN_IF_ERROR(
                sort_and_tie_column(cancel, column, is_asc_order, is_null_first, *small_perm, tie, range, true));
    }

    // Stable sort need extra runs of sorting on permutation
    TieIterator ti(tie);
    while (ti.next()) {
        int range_first = ti.range_first, range_last = ti.range_last;
        if (range_last - range_first > 1) {
            ::pdqsort(
                    cancel, small_perm->begin() + range_first, small_perm->begin() + range_last,
                    [](SmallPermuteItem lhs, SmallPermuteItem rhs) { return lhs.index_in_chunk < rhs.index_in_chunk; });
        }
    }
    
    return Status::OK();
}

Status sort_and_tie_vertical_columns(const bool& cancel, const std::vector<ColumnPtr>& columns, bool is_asc_order,
                                     bool is_null_first, Permutation& permutation, Tie& tie, std::pair<int, int> range,
                                     bool build_tie) {
    DCHECK_GT(columns.size(), 0);
    VerticalColumnSorter sorter(cancel, columns, is_asc_order, is_null_first, permutation, tie, range, build_tie);
    return columns[0]->accept(&sorter);
}

Status sort_chunks_columnwise(const bool& cancel, const std::vector<Columns>& vertical_chunks,
                              const std::vector<int>& sort_orders, const std::vector<int>& null_firsts,
                              Permutation& perm, int limit) {
    if (vertical_chunks.empty()) {
        return Status::OK();
    }
    if (perm.empty()) {
        return Status::OK();
    }
    // TODO: check more

    // TODO: support partial sort
    size_t num_columns = vertical_chunks[0].size();
    size_t num_rows = perm.size();
    Tie tie(num_rows, 1);
    std::pair<int, int> range(0, num_rows);

    for (int col = 0; col < num_columns; col++) {
        bool is_asc_order = (sort_orders[col] == 1);
        bool is_null_first = is_asc_order ? (null_firsts[col] == -1) : (null_firsts[col] == 1);
        bool build_tie = col != num_columns - 1;

        std::vector<ColumnPtr> vertical_columns;
        for (const auto& columns : vertical_chunks) {
            vertical_columns.push_back(columns[col]);
        }

        RETURN_IF_ERROR(sort_and_tie_vertical_columns(cancel, vertical_columns, is_asc_order, is_null_first, perm, tie,
                                                      range, build_tie));
    }

    if (limit < perm.size()) {
        perm.resize(limit);
    }

    return Status::OK();
}

} // namespace starrocks::vectorized