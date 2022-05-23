// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include <algorithm>

#include "column/array_column.h"
#include "column/binary_column.h"
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

// Sort a column by permtuation
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
        // Fastpath
        if (!column.has_null()) {
            return column.data_column_ref().accept(this);
        }

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

    template <typename T>
    Status do_visit(const vectorized::BinaryColumnBase<T>& column) {
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

// Sort multiple a column from multiple chunks(vertical column)
class VerticalColumnSorter final : public ColumnVisitorAdapter<VerticalColumnSorter> {
public:
    explicit VerticalColumnSorter(const std::atomic<bool>& cancel, const std::vector<ColumnPtr>& columns,
                                  bool is_asc_order, bool is_null_first, Permutation& permutation, Tie& tie,
                                  std::pair<int, int> range, bool build_tie, size_t limit)
            : ColumnVisitorAdapter(this),
              _cancel(cancel),
              _is_asc_order(is_asc_order),
              _is_null_first(is_null_first),
              _vertical_columns(columns),
              _permutation(permutation),
              _tie(tie),
              _range(range),
              _build_tie(build_tie),
              _limit(limit),
              _pruned_limit(permutation.size()) {}

    size_t get_limited() const { return _pruned_limit; }

    Status do_visit(const vectorized::NullableColumn& column) {
        std::vector<const NullData*> null_datas;
        std::vector<ColumnPtr> data_columns;
        for (auto& col : _vertical_columns) {
            auto real = down_cast<const NullableColumn*>(col.get());
            null_datas.push_back(&real->immutable_null_column_data());
            data_columns.push_back(real->data_column());
        }

        if (_is_null_first) {
            auto null_pred = [&](const PermutationItem& item) -> bool {
                return (*null_datas[item.chunk_index])[item.index_in_chunk] == 1;
            };

            RETURN_IF_ERROR(sort_and_tie_helper_nullable_vertical(_cancel, data_columns, null_pred, _is_asc_order,
                                                                  _is_null_first, _permutation, _tie, _range,
                                                                  _build_tie, _limit, &_pruned_limit));
        } else {
            auto null_pred = [&](const PermutationItem& item) -> bool {
                return (*null_datas[item.chunk_index])[item.index_in_chunk] != 1;
            };

            RETURN_IF_ERROR(sort_and_tie_helper_nullable_vertical(_cancel, data_columns, null_pred, _is_asc_order,
                                                                  _is_null_first, _permutation, _tie, _range,
                                                                  _build_tie, _limit, &_pruned_limit));
        }

        _prune_limit();

        return Status::OK();
    }

    template <typename T>
    Status do_visit(const vectorized::BinaryColumnBase<T>& column) {
        using ColumnType = BinaryColumnBase<T>;

        if (_need_inline_value()) {
            using ItemType = CompactChunkItem<Slice>;
            using Container = std::vector<Slice>;

            auto cmp = [&](const ItemType& lhs, const ItemType& rhs) -> int {
                return lhs.inline_value.compare(rhs.inline_value);
            };

            std::vector<const Container*> containers;
            for (const auto& col : _vertical_columns) {
                const auto real = down_cast<const ColumnType*>(col.get());
                containers.push_back(&real->get_data());
            }

            auto inlined = _create_inlined_permutation<Slice>(containers);
            RETURN_IF_ERROR(sort_and_tie_helper(_cancel, &column, _is_asc_order, inlined, _tie, cmp, _range, _build_tie,
                                                _limit, &_pruned_limit));
            _restore_inlined_permutation(inlined);
        } else {
            auto cmp = [&](const PermutationItem& lhs, const PermutationItem& rhs) {
                auto left_column = down_cast<const ColumnType*>(_vertical_columns[lhs.chunk_index].get());
                auto right_column = down_cast<const ColumnType*>(_vertical_columns[rhs.chunk_index].get());
                auto left_value = left_column->get_data()[lhs.index_in_chunk];
                auto right_value = right_column->get_data()[rhs.index_in_chunk];
                return SorterComparator<Slice>::compare(left_value, right_value);
            };

            RETURN_IF_ERROR(sort_and_tie_helper(_cancel, &column, _is_asc_order, _permutation, _tie, cmp, _range,
                                                _build_tie, _limit, &_pruned_limit));
        }
        _prune_limit();
        return Status::OK();
    }

    template <typename T>
    Status do_visit(const vectorized::FixedLengthColumnBase<T>& column) {
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
            RETURN_IF_ERROR(sort_and_tie_helper(_cancel, &column, _is_asc_order, inlined, _tie, cmp, _range, _build_tie,
                                                _limit, &_pruned_limit));
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

            RETURN_IF_ERROR(sort_and_tie_helper(_cancel, &column, _is_asc_order, _permutation, _tie, cmp, _range,
                                                _build_tie, _limit, &_pruned_limit));
        }
        _prune_limit();
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

        RETURN_IF_ERROR(sort_and_tie_helper(_cancel, &column, _is_asc_order, _permutation, _tie, cmp, _range,
                                            _build_tie, _limit, &_pruned_limit));
        _prune_limit();
        return Status::OK();
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

        RETURN_IF_ERROR(sort_and_tie_helper(_cancel, &column, _is_asc_order, _permutation, _tie, cmp, _range,
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
    const bool _is_asc_order;
    const bool _is_null_first;

    std::vector<ColumnPtr> _vertical_columns;
    Permutation& _permutation;
    Tie& _tie;
    std::pair<int, int> _range;
    bool _build_tie;
    size_t _limit;        // The requested limit
    size_t _pruned_limit; // The pruned limit during partial sorting
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

Status sort_vertical_columns(const std::atomic<bool>& cancel, const std::vector<ColumnPtr>& columns, bool is_asc_order,
                             bool is_null_first, Permutation& permutation, Tie& tie, std::pair<int, int> range,
                             bool build_tie, size_t limit, size_t* limited) {
    DCHECK_GT(columns.size(), 0);
    DCHECK_GT(permutation.size(), 0);
    VerticalColumnSorter sorter(cancel, columns, is_asc_order, is_null_first, permutation, tie, range, build_tie,
                                limit);
    RETURN_IF_ERROR(columns[0]->accept(&sorter));
    if (limit > 0 && limited != nullptr) {
        *limited = std::min(*limited, sorter.get_limited());
    }
    return Status::OK();
}

Status sort_vertical_chunks(const std::atomic<bool>& cancel, const std::vector<Columns>& vertical_chunks,
                            const std::vector<int>& sort_orders, const std::vector<int>& null_firsts, Permutation& perm,
                            size_t limit, bool is_limit_by_rank) {
    if (vertical_chunks.empty() || perm.empty()) {
        return Status::OK();
    }

    size_t num_columns = vertical_chunks[0].size();
    size_t num_rows = perm.size();
    Tie tie(num_rows, 1);

    DCHECK_EQ(num_columns, sort_orders.size());
    DCHECK_EQ(num_columns, null_firsts.size());

    for (int col = 0; col < num_columns; col++) {
        // TODO: use the flag directly
        bool is_asc_order = (sort_orders[col] == 1);
        bool is_null_first = is_asc_order ? (null_firsts[col] == -1) : (null_firsts[col] == 1);
        bool build_tie = col != num_columns - 1;
        std::pair<int, int> range(0, perm.size());
        DCHECK_GT(perm.size(), 0);

        std::vector<ColumnPtr> vertical_columns;
        for (const auto& columns : vertical_chunks) {
            vertical_columns.push_back(columns[col]);
        }

        RETURN_IF_ERROR(sort_vertical_columns(cancel, vertical_columns, is_asc_order, is_null_first, perm, tie, range,
                                              build_tie, limit));
    }

    if (limit < perm.size()) {
        if (is_limit_by_rank) {
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

void append_by_permutation(Chunk* dst, const std::vector<ChunkPtr>& chunks, const Permutation& perm) {
    std::vector<const Chunk*> src;
    src.reserve(chunks.size());
    for (auto& chunk : chunks) {
        src.push_back(chunk.get());
    }
    DCHECK_LT(std::max_element(perm.begin(), perm.end(),
                               [](auto& lhs, auto& rhs) { return lhs.chunk_index < rhs.chunk_index; })
                      ->chunk_index,
              chunks.size());
    append_by_permutation(dst, src, perm);
}

void append_by_permutation(Chunk* dst, const std::vector<const Chunk*>& chunks, const Permutation& perm) {
    if (chunks.empty() || perm.empty()) {
        return;
    }

    DCHECK_EQ(dst->num_columns(), chunks[0]->columns().size());
    for (size_t col_index = 0; col_index < dst->columns().size(); col_index++) {
        Columns tmp_columns;
        tmp_columns.reserve(chunks.size());
        for (auto chunk : chunks) {
            tmp_columns.push_back(chunk->get_column_by_index(col_index));
        }
        append_by_permutation(dst->get_column_by_index(col_index).get(), tmp_columns, perm);
    }
}

} // namespace starrocks::vectorized
