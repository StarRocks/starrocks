// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "runtime/date_value.hpp"
#include "runtime/timestamp_value.h"

namespace starrocks::vectorized {
inline DateValue::operator TimestampValue() const {
    return TimestampValue{date::to_timestamp(_julian)};
}

inline TimestampValue::operator DateValue() const {
    return DateValue{timestamp::to_julian(_timestamp)};
}

inline void DateValue::to_date(int* year, int* month, int* day) const {
    date::to_date_with_cache(_julian, year, month, day);
}
} // namespace starrocks::vectorized