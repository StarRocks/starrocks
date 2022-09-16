// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "column/array_column.h"
#include "column/column_visitor_adapter.h"
#include "column/const_column.h"
#include "column/datum.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "exec/vectorized/sorting/sort_helper.h"
#include "exec/vectorized/sorting/sort_permute.h"
#include "exec/vectorized/sorting/sorting.h"
#include "glog/logging.h"
#include "runtime/primitive_type.h"
#include "simd/selector.h"

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

        CompareVector cmp_vector(null_data.size());
        // TODO: use IMD select_if
        auto merge_cmp_vector = [](CompareVector& a, CompareVector& b) {
            DCHECK_EQ(a.size(), b.size());
            for (size_t i = 0; i < a.size(); ++i) {
                a[i] = a[i] == 0 ? b[i] : a[i];
            }
        };
        if (_rhs_value.is_null()) {
            for (size_t i = 0; i < null_data.size(); i++) {
                if (null_data[i] == 0) {
                    cmp_vector[i] = -_null_first;
                } else {
                    cmp_vector[i] = null_vector[i];
                }
            }
            // merge cmp_vector
            // _cmp_vector[i] = _cmp_vector[i] == 0 ? cmp_vector[i]: _cmp_vector[i];
            merge_cmp_vector(_cmp_vector, cmp_vector);
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
                    cmp_vector[i] = notnull_vector[i];
                } else {
                    cmp_vector[i] = null_vector[i];
                }
            }
            merge_cmp_vector(_cmp_vector, cmp_vector);
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

    template <typename T>
    Status do_visit(const vectorized::BinaryColumnBase<T>& column) {
        const auto& lhs_datas = column.get_data();
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

    template <typename T>
    Status do_visit(const vectorized::BinaryColumnBase<T>& column) {
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

} // namespace starrocks::vectorized