// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/date_value.h

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

#include <cstdint>
#include <string>

#include "runtime/time_types.h"
#include "storage/uint24.h"
#include "util/hash_util.hpp"
#include "util/slice.h"

namespace starrocks {
namespace vectorized {
class TimestampValue;

/**
 * POD class
 *
 * Support date type value without time, like "YYYY-dd-MM".
 * Implemented by Julian date (Julian date as the number of days
 * since the beginning of the Julian Period (January 1, 4713 BCE).)
 *
 */
class DateValue {
public:
    inline static DateValue create(int year, int month, int day);

public:
    void from_date(int year, int month, int day);

    int32_t to_date_literal() const;

    void from_date_literal(int64_t date_literal);

    bool from_date_literal_with_check(int64_t date_literal);

    void from_mysql_date(uint64_t date);

    uint24_t to_mysql_date() const;

    bool from_string(const char* date_str, size_t len);

    inline void to_date(int* year, int* month, int* day) const;

    bool get_weeks_of_year_with_cache(int* weeks) const;

    int get_week_of_year() const;
    /**
     * Get day of week.
     * @return
     *  - 0: Sunday
     *  - 1: Monday
     *  - 2: Tuesday
     *  - 3: Wednesday
     *  - 4: Thursday
     *  - 5: Friday
     *  - 6: Saturday
     */
    int weekday() const;

    void trunc_to_day();
    void trunc_to_month();
    void trunc_to_year();
    void trunc_to_week();
    void trunc_to_quarter();

    bool is_valid() const;

    std::string month_name() const;

    std::string day_name() const;

    std::string to_string() const;

    JulianDate julian() const { return _julian; }

    template <TimeUnit UNIT>
    inline DateValue add(int count) const;

    inline operator TimestampValue() const;

public:
    static const DateValue MAX_DATE_VALUE;
    static const DateValue MIN_DATE_VALUE;

public:
    JulianDate _julian;
};

DateValue DateValue::create(int year, int month, int day) {
    DateValue dv;
    dv.from_date(year, month, day);
    return dv;
}

template <TimeUnit UNIT>
DateValue DateValue::add(int count) const {
    return DateValue{date::add<UNIT>(_julian, count)};
}

inline bool operator==(const DateValue& lhs, const DateValue& rhs) {
    return lhs._julian == rhs._julian;
}

inline bool operator!=(const DateValue& lhs, const DateValue& rhs) {
    return lhs._julian != rhs._julian;
}

inline bool operator<=(const DateValue& lhs, const DateValue& rhs) {
    return lhs._julian <= rhs._julian;
}

inline bool operator<(const DateValue& lhs, const DateValue& rhs) {
    return lhs._julian < rhs._julian;
}

inline bool operator>=(const DateValue& lhs, const DateValue& rhs) {
    return lhs._julian >= rhs._julian;
}

inline bool operator>(const DateValue& lhs, const DateValue& rhs) {
    return lhs._julian > rhs._julian;
}

inline std::ostream& operator<<(std::ostream& os, const DateValue& value) {
    os << value.to_string();
    return os;
}
} // namespace vectorized
} // namespace starrocks

namespace std {
template <>
struct hash<starrocks::vectorized::DateValue> {
    size_t operator()(const starrocks::vectorized::DateValue& v) const { return std::hash<int32_t>()(v._julian); }
};
} // namespace std