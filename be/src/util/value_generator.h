// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once
#include "runtime/date_value.h"
#include "runtime/timestamp_value.h"

namespace starrocks::vectorized {
template <class T>
struct DefaultValueGenerator {
    static T next_value() { return T(); }
};

template <>
struct DefaultValueGenerator<DateValue> {
    static DateValue next_value() { return DateValue::MIN_DATE_VALUE; }
};

template <>
struct DefaultValueGenerator<TimestampValue> {
    static TimestampValue next_value() { return TimestampValue::MIN_TIMESTAMP_VALUE; }
};
} // namespace starrocks::vectorized
