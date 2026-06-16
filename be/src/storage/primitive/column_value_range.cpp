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

#include "storage/primitive/column_value_range.h"

#include <algorithm>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "gutil/stl_util.h"
#include "gutil/strings/substitute.h"

namespace starrocks {

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
inline size_t difference<int256_t>(const int256_t& low, const int256_t& high) {
    DCHECK_LE(low, high);
    if (high - low > static_cast<int256_t>(SIZE_MAX)) {
        return SIZE_MAX;
    }
    return static_cast<size_t>(high - low);
}

template <>
inline size_t difference<DateValue>(const DateValue& low, const DateValue& high) {
    DCHECK_LE(low, high);
    return high.julian() - low.julian();
}

template <>
inline size_t difference<TimestampValue>(const TimestampValue& low, const TimestampValue& high) {
    DCHECK_LE(low, high);
    return high.timestamp() - low.timestamp();
}

template <typename T>
inline void increase(T& value) {
    ++value;
}

template <>
inline void increase(DateValue& value) {
    value = value.add<TimeUnit::DAY>(1);
}

template <>
inline void increase(TimestampValue& value) {
    value = value.add<TimeUnit::SECOND>(1);
}

} // namespace helper

template <>
void ColumnValueRange<Slice>::convert_to_fixed_value() {}

template <>
void ColumnValueRange<DecimalV2Value>::convert_to_fixed_value() {}

template <>
void ColumnValueRange<__int128>::convert_to_fixed_value() {}

template <>
void ColumnValueRange<bool>::convert_to_fixed_value() {}

template <class T>
void ColumnValueRange<T>::clear() {
    _fixed_values.clear();
    _low_value = _type_min;
    _high_value = _type_max;
    _low_op = FILTER_LARGER_OR_EQUAL;
    _high_op = FILTER_LESS_OR_EQUAL;
    _fixed_op = FILTER_IN;
    _empty_range = false;
}

template <class T>
void ColumnValueRange<T>::clear_to_empty() {
    _fixed_values.clear();
    _low_value = _type_max;
    _high_value = _type_min;
    _low_op = FILTER_LARGER_OR_EQUAL;
    _high_op = FILTER_LESS_OR_EQUAL;
    _fixed_op = FILTER_IN;
    _empty_range = true;
}

template <class T>
template <typename ConditionType>
ConditionType ColumnValueRange<T>::to_olap_not_null_filter() const {
    ConditionType condition;
    condition.set_is_index_filter_only(_is_index_filter_only);
    condition.set_column_name(_column_name);
    condition.set_condition_op("IS");
    condition.set_is_null(false);

    return condition;
}

template <class T>
template <typename ConditionType, bool Negative>
void ColumnValueRange<T>::to_olap_filter(std::vector<ConditionType>& filters) {
    // If we have fixed range value, we generate in/not-in predicates.
    if (is_fixed_value_range()) {
        DCHECK(_fixed_op == FILTER_IN || _fixed_op == FILTER_NOT_IN);
        bool filter_in = (_fixed_op == FILTER_IN) ? true : false;
        if constexpr (Negative) {
            filter_in = !filter_in;
        }
        const std::string op = (filter_in) ? "*=" : "!=";

        ConditionType condition;
        condition.set_is_index_filter_only(_is_index_filter_only);
        condition.set_column_name(_column_name);
        condition.set_condition_op(op);
        for (const auto& value : _fixed_values) {
            condition.add_condition_value(value, type(), precision(), scale());
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
            filters.push_back(std::move(condition));
        }
    } else {
        ConditionType low;
        low.set_is_index_filter_only(_is_index_filter_only);
        if (_type_min != _low_value || FILTER_LARGER_OR_EQUAL != _low_op) {
            low.set_column_name(_column_name);
            if constexpr (Negative) {
                low.set_condition_op((_low_op == FILTER_LARGER_OR_EQUAL ? "<<" : "<="));
            } else {
                low.set_condition_op((_low_op == FILTER_LARGER_OR_EQUAL ? ">=" : ">>"));
            }
            low.add_condition_value(_low_value, type(), precision(), scale());
        }

        if (!low.condition_values.empty()) {
            filters.push_back(std::move(low));
        }

        ConditionType high;
        high.set_is_index_filter_only(_is_index_filter_only);
        if (_type_max != _high_value || FILTER_LESS_OR_EQUAL != _high_op) {
            high.set_column_name(_column_name);
            if constexpr (Negative) {
                high.set_condition_op((_high_op == FILTER_LESS_OR_EQUAL ? ">>" : ">="));
            } else {
                high.set_condition_op((_high_op == FILTER_LESS_OR_EQUAL ? "<=" : "<<"));
            }
            high.add_condition_value(_high_value, type(), precision(), scale());
        }

        if (!high.condition_values.empty()) {
            filters.push_back(std::move(high));
        }
    }
}

template <class T>
Status ColumnValueRange<T>::add_fixed_values(SQLFilterOp op, const ValuesContainer& values) {
    if (TYPE_UNKNOWN == _column_type) {
        return Status::InternalError("AddFixedValue failed, Invalid type");
    }
    if (op == FILTER_IN) {
        if (is_empty_value_range()) {
            // nothing to do
            _fixed_op = op;
        } else if (is_fixed_value_range() && _fixed_op == FILTER_NOT_IN) {
            ValuesContainer not_in_operands = STLSetDifference(_fixed_values, values);
            ValuesContainer in_operands = STLSetDifference(values, _fixed_values);
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
            ValuesContainer not_in_operands = STLSetDifference(values, _fixed_values);
            ValuesContainer in_operands = STLSetDifference(_fixed_values, values);
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
void ColumnValueRange<T>::convert_to_fixed_value() {
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
Status ColumnValueRange<T>::add_range(SQLFilterOp op, T value) {
    if (TYPE_UNKNOWN == _column_type) {
        return Status::InternalError("AddRange failed, Invalid type");
    }

    // If we already have IN value range, we can put `value` into it.
    if (is_fixed_value_range()) {
        if (_fixed_op != FILTER_IN) {
            return Status::InternalError(strings::Substitute("Add Range Fail! Unsupported SQLFilterOp $0", op));
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
            return Status::InternalError(strings::Substitute("Add Range Fail! Unsupported SQLFilterOp $0", op));
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
                } else if (value < _type_min) {
                    return Status::NotSupported("reject value smaller than type min");
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
                } else if (value > _type_max) {
                    return Status::NotSupported("reject value larger than type max");
                } else {
                    // accept this value, but keep range unchanged.
                }
                break;
            }
            default: {
                return Status::InternalError(strings::Substitute("Add Range Fail! Unsupported SQLFilterOp $0", op));
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
ColumnValueRange<T>::ColumnValueRange() = default;

template <class T>
ColumnValueRange<T>::ColumnValueRange(std::string col_name, LogicalType type, T min, T max)
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
ColumnValueRange<T>::ColumnValueRange(std::string col_name, LogicalType type, T type_min, T type_max, T min, T max)
        : _column_name(std::move(col_name)),
          _column_type(type),
          _type_min(type_min),
          _type_max(type_max),
          _low_value(min),
          _high_value(max),
          _low_op(FILTER_LARGER_OR_EQUAL),
          _high_op(FILTER_LESS_OR_EQUAL),
          _fixed_op(FILTER_IN) {}

template <class T>
bool ColumnValueRange<T>::is_fixed_value_range() const {
    return _fixed_values.size() != 0 || _empty_range;
}

template <class T>
bool ColumnValueRange<T>::is_empty_value_range() const {
    if (TYPE_UNKNOWN == _column_type) {
        return true;
    }
    // TODO(yan): sometimes we don't have Fixed Value Range, but have
    // following value range like > 10 && < 5, which is also empty value range.
    // Maybe we can add that check later. Without that check, there is no correctness problem
    // but only performance performance.
    return _fixed_values.empty() && _empty_range;
}

template <class T>
bool ColumnValueRange<T>::is_full_value_range() const {
    if (_is_init_state || is_fixed_value_range()) {
        return false;
    }

    bool full_low = (_low_value == _type_min && _low_op == FILTER_LARGER_OR_EQUAL) ||
                    (_low_value < _type_min && _low_op == FILTER_LARGER);
    bool full_high = (_high_value == _type_max && _high_op == FILTER_LESS_OR_EQUAL) ||
                     (_high_value > _type_max && _high_op == FILTER_LESS);
    return full_high && full_low;
}

template <class T>
bool ColumnValueRange<T>::is_fixed_value_convertible() const {
    if (is_fixed_value_range()) {
        return false;
    }
    return is_enumeration_type(_column_type);
}

template <class T>
bool ColumnValueRange<T>::is_range_value_convertible() const {
    if (!is_fixed_value_range() || _fixed_op != FILTER_IN) {
        return false;
    }
    return !(TYPE_NULL == _column_type || TYPE_BOOLEAN == _column_type);
}

template <class T>
size_t ColumnValueRange<T>::get_convertible_fixed_value_size() const {
    return is_fixed_value_convertible() ? helper::difference(_low_value, _high_value) : 0;
}

template <class T>
void ColumnValueRange<T>::convert_to_range_value() {
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
void ColumnValueRange<T>::set_precision(int precision) {
    this->_precision = precision;
}

template <class T>
void ColumnValueRange<T>::set_scale(int scale) {
    this->_scale = scale;
}

template <class T>
int ColumnValueRange<T>::precision() const {
    return this->_precision;
}

template <class T>
int ColumnValueRange<T>::scale() const {
    return this->_scale;
}

#define InsitializeColumnValueRange(T)                                                                          \
    template class ColumnValueRange<T>;                                                                         \
                                                                                                                \
    template GeneralCondition ColumnValueRange<T>::to_olap_not_null_filter<GeneralCondition>() const;           \
    template OlapCondition ColumnValueRange<T>::to_olap_not_null_filter<OlapCondition>() const;                 \
                                                                                                                \
    template void ColumnValueRange<T>::to_olap_filter<GeneralCondition, false>(std::vector<GeneralCondition>&); \
    template void ColumnValueRange<T>::to_olap_filter<GeneralCondition, true>(std::vector<GeneralCondition>&);  \
    template void ColumnValueRange<T>::to_olap_filter<OlapCondition, false>(std::vector<OlapCondition>&);       \
    template void ColumnValueRange<T>::to_olap_filter<OlapCondition, true>(std::vector<OlapCondition>&);

InsitializeColumnValueRange(int8_t);
InsitializeColumnValueRange(uint8_t);
InsitializeColumnValueRange(int16_t);
InsitializeColumnValueRange(int32_t);
InsitializeColumnValueRange(int64_t);
InsitializeColumnValueRange(__int128);
InsitializeColumnValueRange(int256_t);
InsitializeColumnValueRange(Slice);
InsitializeColumnValueRange(DecimalV2Value);
InsitializeColumnValueRange(bool);
InsitializeColumnValueRange(DateValue);
InsitializeColumnValueRange(TimestampValue);

#undef InsitializeColumnValueRange

} // namespace starrocks
