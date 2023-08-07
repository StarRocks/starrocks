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

#include <cctz/civil_time.h>
#include <cctz/time_zone.h>

#include <string>

#include "runtime/datetime_value.h"
#include "runtime/time_types.h"
#include "types/date_value.h"
#include "util/hash_util.hpp"

namespace starrocks {

enum DateTimeType { TIMESTAMP_TIME = 1, TIMESTAMP_DATE = 2, TIMESTAMP_DATETIME = 3 };

class TimestampValue {
public:
    using type = Timestamp;

    inline static TimestampValue create(int year, int month, int day, int hour, int minute, int second,
                                        int microsecond);

    inline Timestamp timestamp() const { return _timestamp; }

    void set_timestamp(Timestamp timestamp) { _timestamp = timestamp; }

    bool from_timestamp_literal(uint64_t timestamp);

    bool from_timestamp_literal_with_check(uint64_t timestamp);

    uint64_t to_timestamp_literal() const;

    void to_time(int* hour, int* minute, int* second, int* usec) const {
        return timestamp::to_time(_timestamp, hour, minute, second, usec);
    }

    bool from_date_format_str(const char* value, int value_len, const char*);
    bool from_datetime_format_str(const char* value, int value_len, const char*);

    struct DatetimeContent {
        uint16_t _neg : 1;  // Used for time value.
        uint16_t _type : 3; // Which type of this value.
        uint16_t _hour : 12;
        uint8_t _minute;
        uint8_t _second;
        uint16_t _year;
        uint8_t _month;
        uint8_t _day;
        // TODO(zc): used for nothing
        uint64_t _microsecond;
    };

    bool from_uncommon_format_str(const char* format, int format_len, const char* value, int value_len);
    bool from_uncommon_format_str(const char* format, int format_len, const char* value, int value_len,
                                  DatetimeContent*, const char** sub_val_end);
    // Calculate how many days since 0000-01-01
    // 0000-01-01 is 1st B.C.
    static uint64_t calc_daynr(uint32_t year, uint32_t month, uint32_t day);

    static uint8_t calc_weekday(uint64_t daynr, bool);

    bool check_range(const DatetimeContent*) const;
    bool check_date(const DatetimeContent*) const;

    // This is private function which modify date but modify `_type`
    bool get_date_from_daynr(uint64_t, DatetimeContent*);

    void from_timestamp(int year, int month, int day, int hour, int minute, int second, int usec);

    void to_timestamp(int* year, int* month, int* day, int* hour, int* minute, int* second, int* usec) const;

    void trunc_to_second();
    void trunc_to_minute();
    void trunc_to_hour();
    void trunc_to_day();
    void trunc_to_month();
    void trunc_to_year();
    void trunc_to_week(int days);
    void trunc_to_quarter();

    template <bool end>
    void floor_to_second_period(long period);
    template <bool end>
    void floor_to_minute_period(long period);
    template <bool end>
    void floor_to_hour_period(long period);
    template <bool end>
    void floor_to_day_period(long period);
    template <bool end>
    void floor_to_month_period(long period);
    template <bool end>
    void floor_to_year_period(long period);
    template <bool end>
    void floor_to_week_period(long period);
    template <bool end>
    void floor_to_quarter_period(long period);

    bool from_string(const char* date_str, size_t len);

    int64_t to_unix_second() const;

    bool from_unixtime(int64_t second, const std::string& timezone);
    void from_unixtime(int64_t second, const cctz::time_zone& ctz);
    void from_unixtime(int64_t second, int64_t microsecond, const cctz::time_zone& ctz);

    void from_unix_second(int64_t second);

    template <TimeUnit UNIT>
    TimestampValue add(int count) const {
        return TimestampValue{timestamp::add<UNIT>(_timestamp, count)};
    }

    bool is_valid() const;

    // date_valid function needs this method, in TimestampValue is_valid_non_strict is equivalent to is_valid.
    bool is_valid_non_strict() const;

    // direct return microsecond will over int64
    int64_t diff_microsecond(TimestampValue other) const;

    std::string to_string() const;

    // Returns the formatted string length or -1 on error.
    int to_string(char* s, size_t n) const;

    static constexpr int max_string_length() { return 26; }

    inline operator DateValue() const;

    static TimestampValue MAX_TIMESTAMP_VALUE;
    static TimestampValue MIN_TIMESTAMP_VALUE;

    /**
     * Milliseconds since January 1, 2000, 00:00:00. A negative number indicates the number of
     * milliseconds before January 1, 2000, 00:00:00.
     */
    Timestamp _timestamp;
};

TimestampValue TimestampValue::create(int year, int month, int day, int hour, int minute, int second,
                                      int microsecond = 0) {
    TimestampValue ts;
    ts.from_timestamp(year, month, day, hour, minute, second, microsecond);
    return ts;
}

template <bool end>
void TimestampValue::floor_to_second_period(long period) {
    int64_t seconds = timestamp::to_julian(_timestamp);
    seconds -= date::AD_EPOCH_JULIAN;
    seconds *= SECS_PER_DAY;
    seconds += timestamp::to_time(_timestamp) / USECS_PER_SEC;
    seconds -= seconds % period;
    if constexpr (end) {
        seconds += period;
    }

    JulianDate day = seconds / SECS_PER_DAY + date::AD_EPOCH_JULIAN;
    Timestamp s = seconds % SECS_PER_DAY;
    _timestamp = timestamp::from_julian_and_time(day, s * USECS_PER_SEC);
}

template <bool end>
void TimestampValue::floor_to_minute_period(long period) {
    TimestampValue::floor_to_second_period<end>(period * 60);
}

template <bool end>
void TimestampValue::floor_to_hour_period(long period) {
    TimestampValue::floor_to_second_period<end>(period * 60 * 60);
}

template <bool end>
void TimestampValue::floor_to_day_period(long period) {
    int64_t days = timestamp::to_julian(_timestamp);
    days -= date::AD_EPOCH_JULIAN;
    days -= days % period;
    days += date::AD_EPOCH_JULIAN;
    if constexpr (end) {
        days += period;
    }
    _timestamp = timestamp::from_julian_and_time(days, 0);
}

template <bool end>
void TimestampValue::floor_to_month_period(long period) {
    int year, month, day;
    date::to_date_with_cache(timestamp::to_julian(_timestamp), &year, &month, &day);

    int months = (year - 1) * 12 + month;
    months -= (months - 1) % period;
    if constexpr (end) {
        months += period;
    }
    year = months / 12 + 1;
    month = months - months / 12 * 12;
    _timestamp = timestamp::from_datetime(year, month, 1, 0, 0, 0, 0);
}

template <bool end>
void TimestampValue::floor_to_year_period(long period) {
    int year, month, day;
    date::to_date_with_cache(timestamp::to_julian(_timestamp), &year, &month, &day);

    year -= (year - 1) % period;
    if constexpr (end) {
        year += period;
    }
    _timestamp = timestamp::from_datetime(year, 1, 1, 0, 0, 0, 0);
}

template <bool end>
void TimestampValue::floor_to_week_period(long period) {
    TimestampValue::floor_to_day_period<end>(period * 7);
}

template <bool end>
void TimestampValue::floor_to_quarter_period(long period) {
    TimestampValue::floor_to_month_period<end>(period * 3);
}

inline bool operator==(const TimestampValue& lhs, const TimestampValue& rhs) {
    return lhs._timestamp == rhs._timestamp;
}

inline bool operator!=(const TimestampValue& lhs, const TimestampValue& rhs) {
    return lhs._timestamp != rhs._timestamp;
}

inline bool operator<=(const TimestampValue& lhs, const TimestampValue& rhs) {
    return lhs._timestamp <= rhs._timestamp;
}

inline bool operator<(const TimestampValue& lhs, const TimestampValue& rhs) {
    return lhs._timestamp < rhs._timestamp;
}

inline bool operator>=(const TimestampValue& lhs, const TimestampValue& rhs) {
    return lhs._timestamp >= rhs._timestamp;
}

inline bool operator>(const TimestampValue& lhs, const TimestampValue& rhs) {
    return lhs._timestamp > rhs._timestamp;
}

inline std::ostream& operator<<(std::ostream& os, const TimestampValue& value) {
    os << value.to_string();
    return os;
}

} // namespace starrocks

namespace std {
template <>
struct hash<starrocks::TimestampValue> {
    size_t operator()(const starrocks::TimestampValue& v) const { return std::hash<int64_t>()(v._timestamp); }
};
} // namespace std
