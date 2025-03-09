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

#include <utility>

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/column_visitor_adapter.h"
#include "column/const_column.h"
#include "column/datum.h"
#include "column/json_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "exec/sorting/sort_helper.h"
#include "exec/sorting/sort_permute.h"
#include "exec/sorting/sorting.h"
#include "glog/logging.h"
#include "simd/selector.h"
#include "types/logical_type.h"

namespace starrocks {

// Compare a column with datum, store result into a vector
// For column-wise compare, only consider rows that are equal at previous columns
// @return number of rows that are equal
template <class DataComparator>
static inline size_t compare_column_helper(CompareVector& cmp_vector, DataComparator cmp) {
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
        size_t idx = 0;
        while (true) {
            size_t pos = SIMD::find_zero(cmp_vector, idx);
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
    explicit ColumnCompare(CompareVector& cmp_vector, Datum rhs_value, const SortDesc& sort_desc)
            : ColumnVisitorAdapter(this),
              _cmp_vector(cmp_vector),
              _rhs_value(std::move(rhs_value)),
              _sort_order(sort_desc.sort_order),
              _null_first(sort_desc.null_first) {}

    Status do_visit(const NullableColumn& column) {
        // Two step compare:
        // 1. Compare null values, store at temporary result
        // 2. Mask notnull values, and compare not-null values
        const NullData& null_data = column.immutable_null_column_data();

        int nan_direction = _sort_order * _null_first;

        // Set byte to 0 when it's null/null byte is 1
        CompareVector null_vector(null_data.size());
        for (size_t i = 0; i < null_data.size(); i++) {
            null_vector[i] = (null_data[i] == 1) ? 0 : 1;
        }
        auto null_cmp = [&](int lhs_row) -> int {
            DCHECK(null_data[lhs_row] == 1);
            return _rhs_value.is_null() ? 0 : nan_direction;
        };
        size_t null_equal_count = compare_column_helper(null_vector, null_cmp);

        int notnull_equal_count = 0;

        CompareVector cmp_vector(null_data.size());
        auto merge_cmp_vector = [](CompareVector& a, CompareVector& b) {
            DCHECK_EQ(a.size(), b.size());
            SIMD_selector<TYPE_TINYINT>::select_if((uint8_t*)a.data(), a, a, b);
        };
        if (_rhs_value.is_null()) {
            for (size_t i = 0; i < null_data.size(); i++) {
                if (null_data[i] == 0) {
                    cmp_vector[i] = -nan_direction;
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

            notnull_equal_count = compare_column(column.data_column(), notnull_vector, _rhs_value,
                                                 SortDesc(_sort_order, _null_first));
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

    Status do_visit(const ConstColumn& column) {
        _equal_count =
                compare_column(column.data_column(), _cmp_vector, _rhs_value, SortDesc(_sort_order, _null_first));

        return Status::OK();
    }

    Status do_visit(const ArrayColumn& column) {
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

    Status do_visit(const MapColumn& column) {
        // Convert the datum to a array column
        auto rhs_column = column.clone_empty();
        rhs_column->append_datum(_rhs_value);
        auto cmp = [&](int lhs_index) {
            return column.compare_at(lhs_index, 0, *rhs_column, _null_first) * _sort_order;
        };
        _equal_count = compare_column_helper(_cmp_vector, cmp);
        return Status::OK();
    }

    Status do_visit(const StructColumn& column) {
        //TODO(SmithCruise)
        return Status::NotSupported("Not support");
    }

    template <typename T>
    Status do_visit(const BinaryColumnBase<T>& column) {
        const auto& lhs_datas = column.get_proxy_data();
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
    Status do_visit(const FixedLengthColumnBase<T>& column) {
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
    Status do_visit(const ObjectColumn<T>& column) {
        DCHECK(false) << "not support object column sort_and_tie";

        return Status::NotSupported("not support object column sort_and_tie");
    }

    Status do_visit(const JsonColumn& column) {
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

    size_t get_equal_count() const { return _equal_count; }

private:
    CompareVector& _cmp_vector;
    Datum _rhs_value;
    int _sort_order;
    int _null_first;

    size_t _equal_count = 0; // Output equal count of compare
};

class ColumnTieBuilder final : public ColumnVisitorAdapter<ColumnTieBuilder> {
public:
    explicit ColumnTieBuilder(ColumnPtr column, Tie* tie)
            : ColumnVisitorAdapter(this), _column(std::move(column)), _tie(tie), _nullable_column(nullptr) {}

    explicit ColumnTieBuilder(ColumnPtr column, Tie* tie, NullColumnPtr nullable_column)
            : ColumnVisitorAdapter(this),
              _column(std::move(column)),
              _tie(tie),
              _nullable_column(std::move(nullable_column)) {}

    Status do_visit(const NullableColumn& column) {
        // TODO: maybe could skip compare rows that contains null
        // 1. Compare the null flag
        // 2. Compare the value if both are not null. Since value for null is just default value,
        //    which are equal, so just compare the value directly
        const NullData& null_data = column.immutable_null_column_data();
        for (size_t i = 1; i < column.size(); i++) {
            (*_tie)[i] &= (null_data[i - 1] == null_data[i]);
        }

        build_tie_for_column(column.data_column(), _tie, column.null_column());
        return Status::OK();
    }

    template <typename T>
    Status do_visit(const BinaryColumnBase<T>& column) {
        auto& data = column.get_proxy_data();
        const NullData* null_data = nullptr;
        if (_nullable_column != nullptr) {
            null_data = &_nullable_column->get_data();
        }
        for (size_t i = 1; i < column.size(); i++) {
            if ((null_data == nullptr) || ((*null_data)[i - 1] != 1 && (*null_data)[i] != 1)) {
                (*_tie)[i] &= SorterComparator<Slice>::compare(data[i - 1], data[i]) == 0;
            }
        }
        return Status::OK();
    }

    template <typename T>
    Status do_visit(const FixedLengthColumnBase<T>& column) {
        auto& data = column.get_data();
        const NullData* null_data = nullptr;
        if (_nullable_column != nullptr) {
            null_data = &_nullable_column->get_data();
        }
        for (size_t i = 1; i < column.size(); i++) {
            if ((null_data == nullptr) || ((*null_data)[i - 1] != 1 && (*null_data)[i] != 1)) {
                (*_tie)[i] &= SorterComparator<T>::compare(data[i - 1], data[i]) == 0;
            }
        }
        return Status::OK();
    }

    Status do_visit(const ConstColumn& column) { return Status::NotSupported("Not support"); }
    Status do_visit(const ArrayColumn& column) { return Status::NotSupported("Not support"); }
    Status do_visit(const MapColumn& column) { return Status::NotSupported("Not support"); }
    Status do_visit(const StructColumn& column) { return Status::NotSupported("Not support"); }
    template <typename T>
    Status do_visit(const ObjectColumn<T>& column) {
        return Status::NotSupported("not support");
    }

private:
    const ColumnPtr _column;
    Tie* _tie;
    const NullColumnPtr _nullable_column;
};

int compare_column(const ColumnPtr& column, CompareVector& cmp_vector, Datum rhs_value, const SortDesc& desc) {
    ColumnCompare compare(cmp_vector, std::move(rhs_value), desc);

    [[maybe_unused]] Status st = column->accept(&compare);
    DCHECK(st.ok());
    return compare.get_equal_count();
}

void compare_columns(const Columns& columns, CompareVector& cmp_vector, const Buffer<Datum>& rhs_values,
                     const SortDescs& sort_desc) {
    if (columns.empty()) {
        return;
    }
    DCHECK_EQ(columns.size(), rhs_values.size());
    DCHECK_EQ(columns.size(), sort_desc.num_columns());
    DCHECK_EQ(columns[0]->size(), cmp_vector.size());
    DCHECK(std::all_of(cmp_vector.begin(), cmp_vector.end(), [](int8_t x) { return x == 1 || x == -1 || x == 0; }));

    for (size_t col_idx = 0; col_idx < columns.size(); col_idx++) {
        const Datum& rhs_value = rhs_values[col_idx];

        int equal_count = compare_column(columns[col_idx], cmp_vector, rhs_value, sort_desc.get_column_desc(col_idx));
        if (equal_count == 0) {
            break;
        }
    }
}

void build_tie_for_column(const ColumnPtr& column, Tie* tie, const NullColumnPtr& null_column) {
    DCHECK(!!tie);
    DCHECK_EQ(column->size(), tie->size());

    ColumnTieBuilder tie_builder(column, tie, null_column);
    [[maybe_unused]] Status st = column->accept(&tie_builder);
    DCHECK(st.ok());
}

} // namespace starrocks
