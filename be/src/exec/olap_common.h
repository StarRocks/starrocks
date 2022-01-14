// This file is made available under Elastic License 2.0.
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

#ifndef STARROCKS_BE_SRC_QUERY_EXEC_OLAP_COMMON_H
#define STARROCKS_BE_SRC_QUERY_EXEC_OLAP_COMMON_H

#include <column/type_traits.h>

#include <boost/lexical_cast.hpp>
#include <boost/variant.hpp>
#include <cstdint>
#include <map>
#include <sstream>
#include <string>
#include <utility>

#include "common/logging.h"
#include "exec/olap_utils.h"
#include "exec/scan_node.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gutil/stl_util.h"
#include "gutil/strings/substitute.h"
#include "runtime/date_value.hpp"
#include "runtime/datetime_value.h"
#include "runtime/descriptors.h"
#include "runtime/string_value.hpp"
#include "runtime/timestamp_value.h"
#include "storage/tuple.h"
#include "util/slice.h"

namespace starrocks {

template <class T>
inline std::string cast_to_string(T value) {
    return boost::lexical_cast<std::string>(value);
}

template <>
inline std::string cast_to_string<vectorized::DateValue>(vectorized::DateValue value) {
    return value.to_string();
}

template <>
inline std::string cast_to_string<vectorized::TimestampValue>(vectorized::TimestampValue value) {
    return value.to_string();
}

// for decimal32/64/128, their underlying type is int32/64/128, so the decimal point
// depends on precision and scale when they are casted into strings
template <class T>
inline std::string cast_to_string(T value, [[maybe_unused]] PrimitiveType pt, [[maybe_unused]] int precision,
                                  [[maybe_unused]] int scale) {
    switch (pt) {
    case TYPE_DECIMAL32: {
        using CppType = vectorized::RunTimeCppType<TYPE_DECIMAL32>;
        return DecimalV3Cast::to_string<CppType>(*reinterpret_cast<CppType*>(&value), precision, scale);
    }
    case TYPE_DECIMAL64: {
        using CppType = vectorized::RunTimeCppType<TYPE_DECIMAL64>;
        return DecimalV3Cast::to_string<CppType>(*reinterpret_cast<CppType*>(&value), precision, scale);
    }
    case TYPE_DECIMAL128: {
        using CppType = vectorized::RunTimeCppType<TYPE_DECIMAL128>;
        return DecimalV3Cast::to_string<CppType>(*reinterpret_cast<CppType*>(&value), precision, scale);
    }
    default:
        return cast_to_string<T>(value);
    }
}

namespace helper {

template <typename T>
inline size_t difference(const T& low, const T& high) {
    return high - low;
}

template <>
inline size_t difference<Slice>(const Slice& low, const Slice& high) {
    return 0;
}

template <>
inline size_t difference<vectorized::DateValue>(const vectorized::DateValue& low, const vectorized::DateValue& high) {
    DCHECK_LE(low, high);
    return high.julian() - low.julian();
}

template <>
inline size_t difference<vectorized::TimestampValue>(const vectorized::TimestampValue& low,
                                                     const vectorized::TimestampValue& high) {
    DCHECK_LE(low, high);
    return high.timestamp() - low.timestamp();
}

template <typename T>
inline void increase(T& value) {
    ++value;
}

template <>
inline void increase(vectorized::DateValue& value) {
    value = value.add<vectorized::TimeUnit::DAY>(1);
}

template <>
inline void increase(vectorized::TimestampValue& value) {
    value = value.add<vectorized::TimeUnit::SECOND>(1);
}

} // namespace helper

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
    ColumnValueRange(std::string col_name, PrimitiveType type, T min, T max);

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

    PrimitiveType type() const { return _column_type; }

    size_t get_fixed_value_size() const { return _fixed_values.size(); }

    void set_index_filter_only(bool is_index_only) { _is_index_filter_only = is_index_only; }

    void to_olap_filter(std::vector<TCondition>& filters) {
        // If we have fixed range value, we generate in/not-in predicates.
        if (is_fixed_value_range()) {
            DCHECK(_fixed_op == FILTER_IN || _fixed_op == FILTER_NOT_IN);
            bool filter_in = (_fixed_op == FILTER_IN) ? true : false;
            const std::string op = (filter_in) ? "*=" : "!=";

            TCondition condition;
            condition.__set_is_index_filter_only(_is_index_filter_only);
            condition.__set_column_name(_column_name);
            condition.__set_condition_op(op);
            for (auto value : _fixed_values) {
                condition.condition_values.push_back(cast_to_string(value, type(), precision(), scale()));
            }

            bool can_push = true;
            if (condition.condition_values.empty()) {
                // If we use IN clause, we wish to include empty set.
                if (filter_in && _empty_range) {
                    can_push = true;
                } else {
                    can_push = false;
                }
            }

            if (can_push) {
                filters.push_back(condition);
            }
        } else {
            TCondition low;
            low.__set_is_index_filter_only(_is_index_filter_only);
            if (_type_min != _low_value || FILTER_LARGER_OR_EQUAL != _low_op) {
                low.__set_column_name(_column_name);
                low.__set_condition_op((_low_op == FILTER_LARGER_OR_EQUAL ? ">=" : ">>"));
                low.condition_values.push_back(cast_to_string(_low_value, type(), precision(), scale()));
            }

            if (!low.condition_values.empty()) {
                filters.push_back(low);
            }

            TCondition high;
            high.__set_is_index_filter_only(_is_index_filter_only);
            if (_type_max != _high_value || FILTER_LESS_OR_EQUAL != _high_op) {
                high.__set_column_name(_column_name);
                high.__set_condition_op((_high_op == FILTER_LESS_OR_EQUAL ? "<=" : "<<"));
                high.condition_values.push_back(cast_to_string(_high_value, type(), precision(), scale()));
            }

            if (!high.condition_values.empty()) {
                filters.push_back(high);
            }
        }
    }

    void clear() {
        _fixed_values.clear();
        _low_value = _type_min;
        _high_value = _type_max;
        _low_op = FILTER_LARGER_OR_EQUAL;
        _high_op = FILTER_LESS_OR_EQUAL;
        _fixed_op = FILTER_IN;
        _empty_range = false;
    }

private:
    std::string _column_name;
    PrimitiveType _column_type{INVALID_TYPE}; // Column type (eg: TINYINT,SMALLINT,INT,BIGINT)
    int _precision;                           // casting a decimalv3-typed value into string need precision
    int _scale;                               // casting a decimalv3-typed value into string need scale
    T _type_min;                              // Column type's min value
    T _type_max;                              // Column type's max value
    T _low_value;                             // Column's low value, closed interval at left
    T _high_value;                            // Column's high value, open interval at right
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
    OlapScanKeys() {}

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

    void set_begin_include(bool begin_include) { _begin_include = begin_include; }

    bool begin_include() const { return _begin_include; }

    void set_end_include(bool end_include) { _end_include = end_include; }

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
typedef boost::variant<
        ColumnValueRange<int8_t>,
        ColumnValueRange<uint8_t>,  // vectorized boolean
        ColumnValueRange<int16_t>,
        ColumnValueRange<int32_t>,
        ColumnValueRange<int64_t>,
        ColumnValueRange<__int128>,
        ColumnValueRange<StringValue>, // TODO: remove
        ColumnValueRange<Slice>,
        ColumnValueRange<DateTimeValue>,
        ColumnValueRange<DecimalValue>,
        ColumnValueRange<DecimalV2Value>,
        ColumnValueRange<bool>,
        ColumnValueRange<vectorized::DateValue>,
        ColumnValueRange<vectorized::TimestampValue>>
        ColumnValueRangeType;
// clang-format on

template <class T>
inline ColumnValueRange<T>::ColumnValueRange() {}

template <class T>
inline ColumnValueRange<T>::ColumnValueRange(std::string col_name, PrimitiveType type, T min, T max)
        : _column_name(std::move(col_name)),
          _column_type(type),
          _type_min(min),
          _type_max(max),
          _low_value(min),
          _high_value(max),
          _low_op(FILTER_LARGER_OR_EQUAL),
          _high_op(FILTER_LESS_OR_EQUAL),
          _fixed_op(FILTER_IN) {}

template <class T>
inline Status ColumnValueRange<T>::add_fixed_values(SQLFilterOp op, const std::set<T>& values) {
    if (INVALID_TYPE == _column_type) {
        return Status::InternalError("AddFixedValue failed, Invalid type");
    }
    if (op == FILTER_IN) {
        if (is_empty_value_range()) {
            // nothing to do
            _fixed_op = op;
        } else if (is_fixed_value_range() && _fixed_op == FILTER_NOT_IN) {
            std::set<T> not_in_operands = STLSetDifference(_fixed_values, values);
            std::set<T> in_operands = STLSetDifference(values, _fixed_values);
            if (!not_in_operands.empty() && !in_operands.empty()) {
                // X in (1,2) and X not in (3) equivalent to X in (1,2)
                _fixed_values.swap(in_operands);
                _fixed_op = FILTER_IN;
            } else if (!in_operands.empty()) {
                // X in (1, 3) and X not in (3)
                // --> X in (1)
                _fixed_values.swap(in_operands);
                _fixed_op = FILTER_IN;
            } else {
                // X in (2) and X not in (2, 3)
                // -> false
                // X in (1, 2) and X not in (1, 2)
                // -> false
                _fixed_values.clear();
                _fixed_op = FILTER_IN;
                _empty_range = true;
            }
        } else if (is_fixed_value_range()) {
            DCHECK_EQ(FILTER_IN, _fixed_op);
            _fixed_values = STLSetIntersection(_fixed_values, values);
            _empty_range = _fixed_values.empty();
            _fixed_op = op;
        } else if (!values.empty()) {
            _fixed_values = values;
            // `add_range` will change _high_value, backup it first.
            SQLFilterOp high_op = _high_op;
            T high_value = _high_value;
            RETURN_IF_ERROR(add_range(_low_op, _low_value));
            RETURN_IF_ERROR(add_range(high_op, high_value));
            _fixed_op = op;
        } else {
            clear();
            _fixed_op = op;
        }
    } else if (op == FILTER_NOT_IN) {
        if (is_empty_value_range()) {
            _fixed_op = FILTER_IN;
        } else if (is_fixed_value_range() && _fixed_op == FILTER_NOT_IN) {
            _fixed_values = STLSetUnion(_fixed_values, values);
            _fixed_op = FILTER_NOT_IN;
        } else if (is_fixed_value_range()) {
            DCHECK_EQ(FILTER_IN, _fixed_op);
            std::set<T> not_in_operands = STLSetDifference(values, _fixed_values);
            std::set<T> in_operands = STLSetDifference(_fixed_values, values);
            if (!not_in_operands.empty() && !in_operands.empty()) {
                // X in (1,2) and X not in (3) equivalent to X in (1,2)
                // X in (1,2,3,4) and X not in (1,3,5,7) equivalent to X in (2,4)
                _fixed_values.swap(in_operands);
                _fixed_op = FILTER_IN;
            } else if (!in_operands.empty()) {
                // X in (1, 3) and X not in (3)
                // --> X in (1)
                _fixed_values.swap(in_operands);
                _fixed_op = FILTER_IN;
            } else {
                // X in (2) and X not in (2, 3)
                // -> false
                // X in (1, 2) and X not in (1, 2)
                // -> false
                _fixed_values.clear();
                _empty_range = true;
                _fixed_op = FILTER_IN;
            }
        } else if (is_low_value_mininum() && _low_op == FILTER_LARGER_OR_EQUAL && is_high_value_maximum() &&
                   _high_op == FILTER_LESS_OR_EQUAL) {
            if (!values.empty()) {
                _fixed_values = values;
                _fixed_op = FILTER_NOT_IN;
            } else {
                return Status::NotSupported("empty not-in operands");
            }
        } else {
            return Status::NotSupported("not-in operator cannot be combined with others");
        }
    } else {
        return Status::InvalidArgument("invalid fixed value operator");
    }
    _is_init_state = false;
    return Status::OK();
}

template <class T>
inline bool ColumnValueRange<T>::is_fixed_value_range() const {
    return _fixed_values.size() != 0 || _empty_range;
}

template <class T>
inline bool ColumnValueRange<T>::is_empty_value_range() const {
    if (INVALID_TYPE == _column_type) {
        return true;
    }
    // TODO(yan): sometimes we don't have Fixed Value Range, but have
    // following value range like > 10 && < 5, which is also empty value range.
    // Maybe we can add that check later. Without that check, there is no correctness problem
    // but only performance performance.
    return _fixed_values.empty() && _empty_range;
}

template <class T>
inline bool ColumnValueRange<T>::is_fixed_value_convertible() const {
    if (is_fixed_value_range()) {
        return false;
    }
    return is_enumeration_type(_column_type);
}

template <class T>
inline bool ColumnValueRange<T>::is_range_value_convertible() const {
    if (!is_fixed_value_range() || _fixed_op != FILTER_IN) {
        return false;
    }
    return !(TYPE_NULL == _column_type || TYPE_BOOLEAN == _column_type);
}

template <class T>
inline size_t ColumnValueRange<T>::get_convertible_fixed_value_size() const {
    return is_fixed_value_convertible() ? helper::difference(_low_value, _high_value) : 0;
}

template <>
void ColumnValueRange<StringValue>::convert_to_fixed_value();

template <>
void ColumnValueRange<Slice>::convert_to_fixed_value();

template <>
void ColumnValueRange<DecimalValue>::convert_to_fixed_value();

template <>
void ColumnValueRange<DecimalV2Value>::convert_to_fixed_value();

template <>
void ColumnValueRange<__int128>::convert_to_fixed_value();

template <>
void ColumnValueRange<bool>::convert_to_fixed_value();

template <class T>
inline void ColumnValueRange<T>::convert_to_fixed_value() {
    if (!is_fixed_value_convertible()) {
        return;
    }

    if (_low_op == FILTER_LARGER) {
        // if _low_value was type::max(), _low_value + 1 will overflow to type::min(),
        // there will be a very large number of elements added to the _fixed_values set.
        // If there is a condition > type::max we simply return an empty scan range.
        if (_low_value == _type_max) {
            _fixed_values.clear();
            return;
        }
        helper::increase(_low_value);
    }

    if (_high_op == FILTER_LESS) {
        for (T v = _low_value; v < _high_value; helper::increase(v)) {
            _fixed_values.insert(v);
        }
        _fixed_op = FILTER_IN;
    } else {
        // if _low_value == _high_value == type::max
        // v will overflow after increase, so we have to
        // do some special treatment
        if (_low_value <= _high_value) {
            _fixed_values.insert(_high_value);
        }
        for (T v = _low_value; v < _high_value; helper::increase(v)) {
            _fixed_values.insert(v);
        }

        _fixed_op = FILTER_IN;
    }
}

template <class T>
inline void ColumnValueRange<T>::convert_to_range_value() {
    if (!is_range_value_convertible()) {
        return;
    }

    if (!_fixed_values.empty()) {
        _low_value = *_fixed_values.begin();
        _low_op = FILTER_LARGER_OR_EQUAL;
        _high_value = *_fixed_values.rbegin();
        _high_op = FILTER_LESS_OR_EQUAL;
        _fixed_values.clear();
    }
}

template <class T>
inline void ColumnValueRange<T>::set_precision(int precision) {
    this->_precision = precision;
}

template <class T>
inline void ColumnValueRange<T>::set_scale(int scale) {
    this->_scale = scale;
}

template <class T>
inline int ColumnValueRange<T>::precision() const {
    return this->_precision;
}

template <class T>
inline int ColumnValueRange<T>::scale() const {
    return this->_scale;
}

template <class T>
inline Status ColumnValueRange<T>::add_range(SQLFilterOp op, T value) {
    if (INVALID_TYPE == _column_type) {
        return Status::InternalError("AddRange failed, Invalid type");
    }

    // If we already have IN value range, we can put `value` into it.
    if (is_fixed_value_range()) {
        if (_fixed_op != FILTER_IN) {
            return Status::InternalError(strings::Substitute("Add Range Fail! Unsupport SQLFilterOp $0", op));
        }
        std::pair<iterator_type, iterator_type> bound_pair = _fixed_values.equal_range(value);

        switch (op) {
        case FILTER_LARGER: {
            _fixed_values.erase(_fixed_values.begin(), bound_pair.second);
            break;
        }
        case FILTER_LARGER_OR_EQUAL: {
            _fixed_values.erase(_fixed_values.begin(), bound_pair.first);
            break;
        }
        case FILTER_LESS: {
            if (bound_pair.first == _fixed_values.find(value)) {
                _fixed_values.erase(bound_pair.first, _fixed_values.end());
            } else {
                _fixed_values.erase(bound_pair.second, _fixed_values.end());
            }
            break;
        }
        case FILTER_LESS_OR_EQUAL: {
            _fixed_values.erase(bound_pair.second, _fixed_values.end());
            break;
        }
        default: {
            return Status::InternalError(strings::Substitute("Add Range Fail! Unsupport SQLFilterOp $0", op));
        }
        }

        _empty_range = _fixed_values.empty();

    } else {
        // Otherwise we can put `value` into normal value range.
        if (_high_value > _low_value) {
            switch (op) {
            case FILTER_LARGER: {
                if (value >= _low_value) {
                    _low_value = value;
                    _low_op = op;
                } else if (UNLIKELY(value < _type_min)) {
                    return Status::NotSupported("reject value smaller than type min");
                } else {
                    // accept this value, but keep range unchanged.
                }
                break;
            }
            case FILTER_LARGER_OR_EQUAL: {
                if (value > _low_value) {
                    _low_value = value;
                    _low_op = op;
                } else if (value <= _type_min) {
                    return Status::NotSupported("reject value smaller than or equals to type min");
                } else {
                    // accept this value, but keep range unchanged.
                }
                break;
            }
            case FILTER_LESS: {
                if (value <= _high_value) {
                    _high_value = value;
                    _high_op = op;
                } else if (UNLIKELY(value > _type_max)) {
                    return Status::NotSupported("reject value larger than type max");
                } else {
                    // accept this value, but keep range unchanged.
                }
                break;
            }
            case FILTER_LESS_OR_EQUAL: {
                if (value < _high_value) {
                    _high_value = value;
                    _high_op = op;
                } else if (value >= _type_max) {
                    return Status::NotSupported("reject value larger than or equals to type max");
                } else {
                    // accept this value, but keep range unchanged.
                }
                break;
            }
            default: {
                return Status::InternalError(strings::Substitute("Add Range Fail! Unsupport SQLFilterOp $0", op));
            }
            }
        }

        if (FILTER_LARGER_OR_EQUAL == _low_op && FILTER_LESS_OR_EQUAL == _high_op && _high_value == _low_value) {
            _fixed_values.insert(_high_value);
            _fixed_op = FILTER_IN;
        } else {
            _empty_range = _low_value > _high_value;
        }
    }
    _is_init_state = false;
    return Status::OK();
}

template <class T>
inline Status OlapScanKeys::extend_scan_key(ColumnValueRange<T>& range, int32_t max_scan_key_num) {
    using namespace std;
    typedef typename set<T>::const_iterator const_iterator_type;

    // 1. clear ScanKey if some column range is empty
    if (range.is_empty_value_range()) {
        _begin_scan_keys.clear();
        _end_scan_keys.clear();
        return Status::Cancelled("empty column range");
    }

    // 2. stop extend ScanKey when it's already extend a range value
    if (_has_range_value) {
        return Status::Cancelled("already extend a range value");
    }

    if (range.is_fixed_value_range() && range.fixed_value_operator() == FILTER_NOT_IN) {
        return Status::Cancelled("stop extend scan key due to not in operator");
    }

    // if a column doesn't have any predicate, we will try to convert the range to fixed values
    // for this case, we need to add null value to fixed values
    bool has_converted = false;
    if (range.is_fixed_value_range()) {
        const size_t mul = std::max<size_t>(1, _begin_scan_keys.size());
        if (range.get_fixed_value_size() > max_scan_key_num / mul) {
            if (range.is_range_value_convertible()) {
                range.convert_to_range_value();
            } else {
                return Status::Cancelled("too many fixed values");
            }
        }
    } else if (range.is_fixed_value_convertible() && _is_convertible) {
        const size_t mul = std::max<size_t>(1, _begin_scan_keys.size());
        if (range.get_convertible_fixed_value_size() <= max_scan_key_num / mul) {
            if (range.is_low_value_mininum() && range.is_high_value_maximum()) {
                has_converted = true;
            }

            range.convert_to_fixed_value();
        }
    }

    // 3.1 extend ScanKey with FixedValueRange
    if (range.is_fixed_value_range()) {
        // 3.1.1 construct num of fixed value ScanKey (begin_key == end_key)
        if (_begin_scan_keys.empty()) {
            const set<T>& fixed_value_set = range.get_fixed_value_set();
            const_iterator_type iter = fixed_value_set.begin();

            for (; iter != fixed_value_set.end(); ++iter) {
                _begin_scan_keys.emplace_back();
                _begin_scan_keys.back().add_value(
                        cast_to_string(*iter, range.type(), range.precision(), range.scale()));
                _end_scan_keys.emplace_back();
                _end_scan_keys.back().add_value(cast_to_string(*iter, range.type(), range.precision(), range.scale()));
            }

            if (has_converted) {
                _begin_scan_keys.emplace_back();
                _begin_scan_keys.back().add_null();
                _end_scan_keys.emplace_back();
                _end_scan_keys.back().add_null();
            }
        } // 3.1.2 produces the Cartesian product of ScanKey and fixed_value
        else {
            const set<T>& fixed_value_set = range.get_fixed_value_set();
            int original_key_range_size = _begin_scan_keys.size();

            for (int i = 0; i < original_key_range_size; ++i) {
                OlapTuple start_base_key_range = _begin_scan_keys[i];
                OlapTuple end_base_key_range = _end_scan_keys[i];

                const_iterator_type iter = fixed_value_set.begin();

                for (; iter != fixed_value_set.end(); ++iter) {
                    // alter the first ScanKey in original place
                    if (iter == fixed_value_set.begin()) {
                        _begin_scan_keys[i].add_value(
                                cast_to_string(*iter, range.type(), range.precision(), range.scale()));
                        _end_scan_keys[i].add_value(
                                cast_to_string(*iter, range.type(), range.precision(), range.scale()));
                    } // append follow ScanKey
                    else {
                        _begin_scan_keys.push_back(start_base_key_range);
                        _begin_scan_keys.back().add_value(
                                cast_to_string(*iter, range.type(), range.precision(), range.scale()));
                        _end_scan_keys.push_back(end_base_key_range);
                        _end_scan_keys.back().add_value(
                                cast_to_string(*iter, range.type(), range.precision(), range.scale()));
                    }
                }

                if (has_converted) {
                    _begin_scan_keys.push_back(start_base_key_range);
                    _begin_scan_keys.back().add_null();
                    _end_scan_keys.push_back(end_base_key_range);
                    _end_scan_keys.back().add_null();
                }
            }
        }

        _begin_include = true;
        _end_include = true;
    } // Extend ScanKey with range value
    else {
        _has_range_value = true;

        if (_begin_scan_keys.empty()) {
            _begin_scan_keys.emplace_back();
            _begin_scan_keys.back().add_value(
                    cast_to_string(range.get_range_min_value(), range.type(), range.precision(), range.scale()),
                    range.is_low_value_mininum());
            _end_scan_keys.emplace_back();
            _end_scan_keys.back().add_value(
                    cast_to_string(range.get_range_max_value(), range.type(), range.precision(), range.scale()));
        } else {
            for (auto& _begin_scan_key : _begin_scan_keys) {
                _begin_scan_key.add_value(
                        cast_to_string(range.get_range_min_value(), range.type(), range.precision(), range.scale()),
                        range.is_low_value_mininum());
            }

            for (auto& _end_scan_key : _end_scan_keys) {
                _end_scan_key.add_value(
                        cast_to_string(range.get_range_max_value(), range.type(), range.precision(), range.scale()));
            }
        }

        _begin_include = range.is_begin_include();
        _end_include = range.is_end_include();
    }

    return Status::OK();
}

} // namespace starrocks

#endif

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
