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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/olap_common.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <column/type_traits.h>

#include <boost/lexical_cast.hpp>
#include <boost/variant.hpp>
#include <cstdint>
#include <map>
#include <sstream>
#include <string>
#include <utility>

#include "exec/olap_utils.h"
#include "exec/scan_node.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gutil/stl_util.h"
#include "gutil/strings/substitute.h"
#include "runtime/datetime_value.h"
#include "runtime/descriptors.h"
#include "runtime/string_value.hpp"
#include "storage/tuple.h"
#include "types/date_value.hpp"
#include "types/timestamp_value.h"
#include "util/slice.h"

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
    typedef typename std::set<T>::iterator iterator_type;
    ColumnValueRange();
    ColumnValueRange(std::string col_name, LogicalType type, T min, T max);
    ColumnValueRange(std::string col_name, LogicalType type, T type_min, T type_max, T min, T max);

    Status add_fixed_values(SQLFilterOp op, const std::set<T>& values);

    Status add_range(SQLFilterOp op, T value);

    void set_precision(int precision);

    void set_scale(int scale);

    int precision() const;

    int scale() const;

    bool is_fixed_value_range() const;

    bool is_empty_value_range() const;

    bool is_init_state() const { return _is_init_state; }

    bool is_fixed_value_convertible() const;

    SQLFilterOp fixed_value_operator() const { return _fixed_op; }

    bool is_range_value_convertible() const;

    size_t get_convertible_fixed_value_size() const;

    void convert_to_fixed_value();

    void convert_to_range_value();

    const std::set<T>& get_fixed_value_set() const { return _fixed_values; }

    T get_range_max_value() const { return _high_value; }

    T get_range_min_value() const { return _low_value; }

    bool is_low_value_mininum() const { return _low_value == _type_min; }

    bool is_high_value_maximum() const { return _high_value == _type_max; }

    bool is_begin_include() const { return _low_op == FILTER_LARGER_OR_EQUAL; }

    bool is_end_include() const { return _high_op == FILTER_LESS_OR_EQUAL; }

    LogicalType type() const { return _column_type; }

    size_t get_fixed_value_size() const { return _fixed_values.size(); }

    void set_index_filter_only(bool is_index_only) { _is_index_filter_only = is_index_only; }

    template <bool Negative = false>
    void to_olap_filter(std::vector<TCondition>& filters);

    TCondition to_olap_not_null_filter() const;

    void clear();

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
    std::set<T> _fixed_values; // Column's fixed values
    SQLFilterOp _fixed_op;
    // Whether this condition only used to filter index, not filter chunk row in storage engine
    bool _is_index_filter_only = false;
    // ColumnValueRange don't call add_range or add_fixed_values
    bool _is_init_state = true;

    bool _empty_range = false;
};

class OlapScanKeys {
public:
    OlapScanKeys() = default;

    template <class T>
    Status extend_scan_key(ColumnValueRange<T>& range, int32_t max_scan_key_num);

    Status get_key_range(std::vector<std::unique_ptr<OlapScanRange>>* key_range);

    bool has_range_value() const { return _has_range_value; }

    void clear() {
        _has_range_value = false;
        _begin_scan_keys.clear();
        _end_scan_keys.clear();
    }

    std::string debug_string() {
        std::stringstream ss;
        DCHECK(_begin_scan_keys.size() == _end_scan_keys.size());
        ss << "ScanKeys:";

        for (int i = 0; i < _begin_scan_keys.size(); ++i) {
            ss << "ScanKey=" << (_begin_include ? "[" : "(") << _begin_scan_keys[i] << " : " << _end_scan_keys[i]
               << (_end_include ? "]" : ")");
        }
        return ss.str();
    }

    size_t size() {
        DCHECK(_begin_scan_keys.size() == _end_scan_keys.size());
        return _begin_scan_keys.size();
    }

    bool begin_include() const { return _begin_include; }

    bool end_include() const { return _end_include; }

    void set_is_convertible(bool is_convertible) { _is_convertible = is_convertible; }

private:
    std::vector<OlapTuple> _begin_scan_keys;
    std::vector<OlapTuple> _end_scan_keys;
    bool _has_range_value{false};
    bool _begin_include{true};
    bool _end_include{true};
    bool _is_convertible{true};
};

// clang-format off
using ColumnValueRangeType =  std::variant<
        ColumnValueRange<int8_t>,
        ColumnValueRange<uint8_t>,  // vectorized boolean
        ColumnValueRange<int16_t>,
        ColumnValueRange<int32_t>,
        ColumnValueRange<int64_t>,
        ColumnValueRange<__int128>,
        ColumnValueRange<StringValue>, // TODO: remove
        ColumnValueRange<Slice>,
        ColumnValueRange<DateTimeValue>,
        ColumnValueRange<DecimalV2Value>,
        ColumnValueRange<bool>,
        ColumnValueRange<DateValue>,
        ColumnValueRange<TimestampValue>>;
// clang-format on

} // namespace starrocks
