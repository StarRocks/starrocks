// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_visitor_adapter.h"
#include "column/const_column.h"
#include "column/datum.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "exec/vectorized/sorting/sort_helper.h"
#include "exec/vectorized/sorting/sort_permute.h"
#include "exec/vectorized/sorting/sorting.h"

namespace starrocks::vectorized {

// Compare a column with datum, store result into a vector
// For column-wise compare, only consider rows that are equal at previous columns
// @return number of rows that are equal
template <class DataComparator>
static inline int compare_column_helper(CompareVector& cmp_vector, DataComparator cmp) {
    DCHECK(std::all_of(cmp_vector.begin(), cmp_vector.end(), [](int8_t x) { return x == 0 || x == -1 || x == 1; }));

    // TODO(mofei) optimize the compare with SIMD
    // For sparse array, use SIMD to skip data
    // For dense array, just iterate all bytes
    if (SIMD::count_zero(cmp_vector) > cmp_vector.size() / 8) {
        for (size_t i = 0; i < cmp_vector.size(); i++) {
            // Only consider rows that are queal at previous columns
            if (cmp_vector[i] == 0) {
                cmp_vector[i] = cmp(i);
            }
        }
    } else {
        int idx = 0;
        while (true) {
            int pos = SIMD::find_zero(cmp_vector, idx);
            if (pos >= cmp_vector.size()) {
                break;
            }

            cmp_vector[pos] = cmp(pos);
            idx = pos + 1;
        }
    }

    return SIMD::count_zero(cmp_vector);
}

// ColumnCompare compare a chunk with a row
// A plain implementation is iterate all rows in chunk, and compare the left row with right row.
// The problem of this way is the compare overhead, since it must implement as virtual function call.
// So here we introduce the column-wise compare algorithm:
// 1. compare the first left jcolumn with the first value in right row, save result in cmp_vector
// 2. compare the second left column only if the first left column is equal to first right value
// 3. repeat the second step until the last column
//
// In this way, the compare procedure is conducted in column-wise style, which eliminated the compare
// function call.
// And the compare result for each column is represented as a vector of int8_t, which takes only a little
// bit of memory footprint and could support fast navigate.
class ColumnCompare final : public ColumnVisitorAdapter<ColumnCompare> {
public:
    explicit ColumnCompare(CompareVector& cmp_vector, Datum rhs_value, int sort_order, int null_first)
            : ColumnVisitorAdapter(this),
              _cmp_vector(cmp_vector),
              _rhs_value(rhs_value),
              _sort_order(sort_order),
              _null_first(null_first) {
        DCHECK(sort_order == 1 || sort_order == -1);
        DCHECK(null_first == 1 || null_first == -1);
    }

    Status do_visit(const vectorized::NullableColumn& column) {
        // Two step compare:
        // 1. Compare null values, store at temporary result
        // 2. Mask notnull values, and compare not-null values
        const NullData& null_data = column.immutable_null_column_data();

        // Set byte to 0 when it's null/null byte is 1
        CompareVector null_vector(null_data.size());
        for (size_t i = 0; i < null_data.size(); i++) {
            null_vector[i] = (null_data[i] == 1) ? 0 : 1;
        }
        auto null_cmp = [&](int lhs_row) -> int {
            DCHECK(null_data[lhs_row] == 1);
            return _rhs_value.is_null() ? 0 : _null_first;
        };
        int null_equal_count = compare_column_helper(null_vector, null_cmp);

        int notnull_equal_count = 0;
        if (_rhs_value.is_null()) {
            for (size_t i = 0; i < null_data.size(); i++) {
                if (null_data[i] == 0) {
                    _cmp_vector[i] = -_null_first;
                } else {
                    _cmp_vector[i] = null_vector[i];
                }
            }
        } else {
            // 0 means not null, so compare it
            // 1 means null, not compare it for not-null values
            CompareVector notnull_vector(null_data.size());
            for (size_t i = 0; i < null_data.size(); i++) {
                notnull_vector[i] = null_data[i];
            }

            notnull_equal_count =
                    compare_column(column.data_column(), notnull_vector, _rhs_value, _sort_order, _null_first);
            for (size_t i = 0; i < null_data.size(); i++) {
                if (null_data[i] == 0) {
                    _cmp_vector[i] = notnull_vector[i];
                } else {
                    _cmp_vector[i] = null_vector[i];
                }
            }
        }

        _equal_count = null_equal_count + notnull_equal_count;

        return Status::OK();
    }

    Status do_visit(const vectorized::ConstColumn& column) {
        _equal_count = compare_column(column.data_column(), _cmp_vector, _rhs_value, _sort_order, _null_first);

        return Status::OK();
    }

    Status do_visit(const vectorized::ArrayColumn& column) {
        // Convert the datum to a array column
        auto rhs_column = column.elements().clone_empty();
        auto& datum_array = _rhs_value.get_array();
        for (auto& x : datum_array) {
            rhs_column->append_datum(x);
        }
        auto cmp = [&](int lhs_index) {
            return column.compare_at(lhs_index, 0, *rhs_column, _null_first) * _sort_order;
        };
        _equal_count = compare_column_helper(_cmp_vector, cmp);
        return Status::OK();
    }

    Status do_visit(const vectorized::BinaryColumn& column) {
        const BinaryColumn::Container& lhs_datas = column.get_data();
        Slice rhs_data = _rhs_value.get<Slice>();

        if (_sort_order == 1) {
            auto cmp = [&](int lhs_row) { return SorterComparator<Slice>::compare(lhs_datas[lhs_row], rhs_data); };
            _equal_count = compare_column_helper(_cmp_vector, cmp);
        } else {
            auto cmp = [&](int lhs_row) { return -1 * SorterComparator<Slice>::compare(lhs_datas[lhs_row], rhs_data); };
            _equal_count = compare_column_helper(_cmp_vector, cmp);
        }

        return Status::OK();
    }

    template <typename T>
    Status do_visit(const vectorized::FixedLengthColumnBase<T>& column) {
        T rhs_data = _rhs_value.get<T>();
        auto& lhs_data = column.get_data();

        if (_sort_order == 1) {
            auto cmp = [&](int lhs_row) { return SorterComparator<T>::compare(lhs_data[lhs_row], rhs_data); };
            _equal_count = compare_column_helper(_cmp_vector, cmp);
        } else {
            auto cmp = [&](int lhs_row) { return -1 * SorterComparator<T>::compare(lhs_data[lhs_row], rhs_data); };
            _equal_count = compare_column_helper(_cmp_vector, cmp);
        }

        return Status::OK();
    }

    template <typename T>
    Status do_visit(const vectorized::ObjectColumn<T>& column) {
        DCHECK(false) << "not support object column sort_and_tie";

        return Status::NotSupported("not support object column sort_and_tie");
    }

    Status do_visit(const vectorized::JsonColumn& column) {
        auto& lhs_data = column.get_data();
        const JsonValue& rhs_json = *_rhs_value.get_json();

        if (_sort_order == 1) {
            auto cmp = [&](int lhs_row) { return lhs_data[lhs_row]->compare(rhs_json); };
            _equal_count = compare_column_helper(_cmp_vector, cmp);

        } else {
            auto cmp = [&](int lhs_row) { return -1 * lhs_data[lhs_row]->compare(rhs_json); };
            _equal_count = compare_column_helper(_cmp_vector, cmp);
        }
        return Status::OK();
    }

    int get_equal_count() { return _equal_count; }

private:
    CompareVector& _cmp_vector;
    Datum _rhs_value;
    int _sort_order;
    int _null_first;

    int _equal_count = 0; // Output equal count of compare
};

class ColumnTieBuilder final : public ColumnVisitorAdapter<ColumnTieBuilder> {
public:
    explicit ColumnTieBuilder(const ColumnPtr column, Tie* tie)
            : ColumnVisitorAdapter(this), _column(column), _tie(tie) {}

    Status do_visit(const vectorized::NullableColumn& column) {
        // TODO: maybe could skip compare rows that contains null
        // 1. Compare the null flag
        // 2. Compare the value if both are not null. Since value for null is just default value,
        //    which are equal, so just compare the value directly
        const NullData& null_data = column.immutable_null_column_data();
        for (size_t i = 1; i < column.size(); i++) {
            (*_tie)[i] &= (null_data[i - 1] == null_data[i]);
        }

        build_tie_for_column(column.data_column(), _tie);
        return Status::OK();
    }

    Status do_visit(const vectorized::BinaryColumn& column) {
        auto& data = column.get_data();
        for (size_t i = 1; i < column.size(); i++) {
            (*_tie)[i] &= SorterComparator<Slice>::compare(data[i - 1], data[i]) == 0;
        }
        return Status::OK();
    }

    template <typename T>
    Status do_visit(const vectorized::FixedLengthColumnBase<T>& column) {
        auto& data = column.get_data();
        for (size_t i = 1; i < column.size(); i++) {
            (*_tie)[i] &= SorterComparator<T>::compare(data[i - 1], data[i]) == 0;
        }
        return Status::OK();
    }

    Status do_visit(const vectorized::ConstColumn& column) { return Status::NotSupported("not support"); }
    Status do_visit(const vectorized::ArrayColumn& column) { return Status::NotSupported("not support"); }
    template <typename T>
    Status do_visit(const vectorized::ObjectColumn<T>& column) {
        return Status::NotSupported("not support");
    }

private:
    const ColumnPtr _column;
    Tie* _tie;
};

int compare_column(const ColumnPtr column, CompareVector& cmp_vector, Datum rhs_value, int sort_order, int null_first) {
    ColumnCompare compare(cmp_vector, rhs_value, sort_order, null_first);

    [[maybe_unused]] Status st = column->accept(&compare);
    DCHECK(st.ok());
    return compare.get_equal_count();
}

void compare_columns(const Columns columns, std::vector<int8_t>& cmp_vector, const std::vector<Datum>& rhs_values,
                     const std::vector<int>& sort_orders, const std::vector<int>& null_firsts) {
    if (columns.empty()) {
        return;
    }
    DCHECK_EQ(columns.size(), rhs_values.size());
    DCHECK_EQ(columns.size(), sort_orders.size());
    DCHECK_EQ(columns.size(), null_firsts.size());
    DCHECK_EQ(columns[0]->size(), cmp_vector.size());
    DCHECK(std::all_of(cmp_vector.begin(), cmp_vector.end(), [](int8_t x) { return x == 1 || x == -1 || x == 0; }));

    for (size_t col_idx = 0; col_idx < columns.size(); col_idx++) {
        int sort_order = sort_orders[col_idx];
        int null_first = null_firsts[col_idx];
        Datum rhs_value = rhs_values[col_idx];

        int equal_count = compare_column(columns[col_idx], cmp_vector, rhs_value, sort_order, null_first);
        if (equal_count == 0) {
            break;
        }
    }
}

void build_tie_for_column(const ColumnPtr column, Tie* tie) {
    DCHECK(!!tie);
    DCHECK_EQ(column->size(), tie->size());

    ColumnTieBuilder tie_builder(column, tie);
    [[maybe_unused]] Status st = column->accept(&tie_builder);
    DCHECK(st.ok());
}

struct SortedRun {
    const Chunk* chunk;
    std::pair<size_t, size_t> range;

    SortedRun(const Chunk* ichunk, std::pair<size_t, size_t> irange) : chunk(ichunk), range(irange) {}

    size_t num_columns() const { return chunk->num_columns(); }
    size_t num_rows() const { return range.second - range.first; }
};

struct EqualRange {
    using Range = std::pair<size_t, size_t>;

    Range left_range;
    Range right_range;

    EqualRange(Range left, Range right) : left_range(left), right_range(right) {}
};

// Merge two-way sorted run into on sorted run
// TODO: specify the ordering
class MergeSortedTwoWay {
public:
    MergeSortedTwoWay(const SortedRun& left, const SortedRun& right, Permutation* perm)
            : _left(left), _right(right), _perm(perm) {}

    void run() {
        constexpr uint32_t kLeftIndex = 0;
        constexpr uint32_t kRightIndex = 1;

        // TODO: optimize with tie
        std::vector<EqualRange> equal_ranges;
        _perm->resize(_left.num_rows() + _right.num_rows());

        // The first column
        equal_ranges.emplace_back(_left.range, _right.range);

        // Iterate each column
        for (int col = 0; col < _left.num_columns(); col++) {
            const Column* left_col = _left.chunk->get_column_by_index(col).get();
            const Column* right_col = _right.chunk->get_column_by_index(col).get();
            std::vector<EqualRange> next_ranges;

            // Iterate each equal-range
            for (auto equal_range : equal_ranges) {
                size_t lhs = equal_range.left_range.first;
                size_t rhs = equal_range.right_range.first;
                size_t lhs_end = equal_range.left_range.second;
                size_t rhs_end = equal_range.right_range.second;

                size_t output_index = lhs + rhs;
                DCHECK_LT(lhs, lhs_end);
                DCHECK_LT(rhs, rhs_end);

                // Merge rows in the equal-range
                while (lhs < lhs_end || rhs < rhs_end) {
                    auto left_range = fetch_left(left_col, lhs, lhs_end);
                    auto right_range = fetch_right(right_col, rhs, rhs_end);

                    // TODO: optimize the compare
                    int x = 0;
                    if (lhs < lhs_end && rhs < rhs_end) {
                        x = left_col->compare_at(left_range.first, right_range.first, *right_col, 1);
                    } else if (lhs < lhs_end) {
                        x = -1;
                    } else if (rhs < rhs_end) {
                        x = 1;
                    }

                    if (x <= 0) {
                        fmt::print("merge left [{}, {}]\n", left_range.first, left_range.second);
                        lhs = left_range.second;
                        for (size_t i = left_range.first; i < left_range.second; i++) {
                            (*_perm)[output_index++] = PermutationItem(kLeftIndex, i);
                        }
                    } else if (x >= 0) {
                        fmt::print("merge right [{}, {}]\n", right_range.first, right_range.second);
                        rhs = right_range.second;
                        for (size_t i = right_range.first; i < right_range.second; i++) {
                            (*_perm)[output_index++] = PermutationItem(kRightIndex, i);
                        }
                    }

                    if (x == 0) {
                        fmt::print("merge equal [{}, {}) + [{}, {})\n", left_range.first, left_range.second,
                                   right_range.first, right_range.second);
                        next_ranges.emplace_back(left_range, right_range);
                    }
                }

                DCHECK_EQ(lhs, lhs_end);
                DCHECK_EQ(rhs, rhs_end);
            }

            equal_ranges.swap(next_ranges);
        }
    }

    std::pair<size_t, size_t> fetch_left(const Column* left_col, size_t lhs, size_t lhs_end) {
        size_t first = lhs;
        size_t last = lhs + 1;
        while (last < lhs_end && left_col->compare_at(last - 1, last, *left_col, 1) == 0) {
            last++;
        }
        return {first, last};
    }

    std::pair<size_t, size_t> fetch_right(const Column* right_col, size_t rhs, size_t rhs_end) {
        size_t first = rhs;
        size_t last = rhs + 1;
        while (last < rhs_end && right_col->compare_at(last - 1, last, *right_col, 1) == 0) {
            last++;
        }
        return {first, last};
    }

private:
    const SortedRun& _left;
    const SortedRun& _right;
    Permutation* _perm;
};

Status merge_sorted_chunks_two_way(const Chunk* left, const Chunk* right, Permutation* output) {
    DCHECK_EQ(left->num_columns(), right->num_columns());

    SortedRun left_run(left, std::make_pair(0, left->num_rows()));
    SortedRun right_run(right, std::make_pair(0, right->num_rows()));
    MergeSortedTwoWay merger(left_run, right_run, output);
    merger.run();
    return Status::OK();
}

Status merge_sorted_chunks_heap_based(const std::vector<SortedRun>& runs, Chunk* output) {
    return Status::NotSupported("TODO");
}

Status merge_sorted_chunks(const std::vector<ChunkPtr>& chunks, Chunk* output) {
    if (chunks.empty()) {
        return Status::OK();
    }

    std::vector<SortedRun> runs;
    for (auto& chunk : chunks) {
        runs.emplace_back(chunk.get(), std::make_pair(0, chunk->num_rows()));
    }

    int num_columns = chunks[0]->num_columns();
    for (int i = 0; i < num_columns; i++) {
    }

    return Status::OK();
}

} // namespace starrocks::vectorized