// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "column/array_column.h"
#include "column/column.h"
#include "column/column_visitor_adapter.h"
#include "column/const_column.h"
#include "column/json_column.h"
#include "exec/vectorized/sorting/sort_helper.h"
#include "exec/vectorized/sorting/sort_permute.h"
#include "exec/vectorized/sorting/sorting.h"

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
        return sort_and_tie_helper_nullable(_cancel, &column, _is_asc_order, _is_null_first, _permutation, _tie, _range,
                                            _build_tie);
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

} // namespace starrocks::vectorized