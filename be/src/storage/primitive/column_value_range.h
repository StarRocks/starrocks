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

#pragma once

#include <boost/container/flat_set.hpp>
#include <cstddef>
#include <cstdint>
#include <string>
#include <variant>
#include <vector>

#include "base/string/slice.h"
#include "column/runtime_type_traits.h"
#include "common/status.h"
#include "storage/primitive/filter_condition.h"
#include "storage/primitive/filter_op.h"
#include "types/date_value.h"
#include "types/datetime_value.h"
#include "types/decimalv2_value.h"
#include "types/timestamp_value.h"

namespace starrocks {

// There are two types of value range: Fixed Value Range and Range Value Range
// I know "Range Value Range" sounds bad, but it's hard to turn over the de facto.
// Fixed Value Range means discrete values in the set, like "IN (1,2,3)"
// Range Value Range means range values like ">= 10 && <= 20"
/**
 * @brief Column's value range
 **/
template <class T>
class ColumnValueRange {
public:
    using RangeValueType = T;
    using ValuesContainer = boost::container::flat_set<T>;
    using iterator_type = typename ValuesContainer::iterator;

    ColumnValueRange();
    ColumnValueRange(std::string col_name, LogicalType type, T min, T max);
    ColumnValueRange(std::string col_name, LogicalType type, T type_min, T type_max, T min, T max);

    Status add_fixed_values(SQLFilterOp op, const ValuesContainer& values);

    Status add_range(SQLFilterOp op, T value);

    void set_precision(int precision);

    void set_scale(int scale);

    int precision() const;

    int scale() const;

    bool is_fixed_value_range() const;

    bool is_empty_value_range() const;

    bool is_full_value_range() const;

    bool is_init_state() const { return _is_init_state; }

    bool is_fixed_value_convertible() const;

    SQLFilterOp fixed_value_operator() const { return _fixed_op; }

    bool is_range_value_convertible() const;

    size_t get_convertible_fixed_value_size() const;

    void convert_to_fixed_value();

    void convert_to_range_value();

    const ValuesContainer& get_fixed_value_set() const { return _fixed_values; }

    T get_range_max_value() const { return _high_value; }

    T get_range_min_value() const { return _low_value; }

    bool is_low_value_mininum() const { return _low_value == _type_min; }

    bool is_high_value_maximum() const { return _high_value == _type_max; }

    bool is_begin_include() const { return _low_op == FILTER_LARGER_OR_EQUAL; }

    bool is_end_include() const { return _high_op == FILTER_LESS_OR_EQUAL; }

    LogicalType type() const { return _column_type; }

    size_t get_fixed_value_size() const { return _fixed_values.size(); }

    void set_index_filter_only(bool is_index_only) { _is_index_filter_only = is_index_only; }

    template <typename ConditionType, bool Negative = false>
    void to_olap_filter(std::vector<ConditionType>& filters);

    template <typename ConditionType>
    ConditionType to_olap_not_null_filter() const;

    void clear();
    void clear_to_empty();

private:
    std::string _column_name;
    LogicalType _column_type{TYPE_UNKNOWN}; // Column type (eg: TINYINT,SMALLINT,INT,BIGINT)
    int _precision;                         // casting a decimalv3-typed value into string need precision
    int _scale;                             // casting a decimalv3-typed value into string need scale
    T _type_min;                            // Column type's min value
    T _type_max;                            // Column type's max value
    T _low_value;                           // Column's low value, closed interval at left
    T _high_value;                          // Column's high value, open interval at right
    SQLFilterOp _low_op;
    SQLFilterOp _high_op;
    ValuesContainer _fixed_values; // Column's fixed values
    SQLFilterOp _fixed_op;
    // Whether this condition only used to filter index, not filter chunk row in storage engine
    bool _is_index_filter_only = false;
    // ColumnValueRange don't call add_range or add_fixed_values
    bool _is_init_state = true;

    bool _empty_range = false;
};

// clang-format off
using ColumnValueRangeType =  std::variant<
        ColumnValueRange<int8_t>,
        ColumnValueRange<uint8_t>,  // vectorized boolean
        ColumnValueRange<int16_t>,
        ColumnValueRange<int32_t>,
        ColumnValueRange<int64_t>,
        ColumnValueRange<__int128>,
        ColumnValueRange<int256_t>,
        ColumnValueRange<Slice>,
        ColumnValueRange<DecimalV2Value>,
        ColumnValueRange<bool>,
        ColumnValueRange<DateValue>,
        ColumnValueRange<TimestampValue>>;
// clang-format on

} // namespace starrocks
