// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <cctz/civil_time.h>
#include <cctz/time_zone.h>

#include <string>

#include "runtime/time_types.h"
#include "types/date_value.h"
#include "util/hash_util.hpp"

namespace starrocks::vectorized {

enum DateTimeType { TIMESTAMP_TIME = 1, TIMESTAMP_DATE = 2, TIMESTAMP_DATETIME = 3 };

const int DATE_MAX_DAYNR = 3652424;

// Limits of time value
const int TIME_MAX_HOUR = 838;

class TimestampValue {
public:
    using type = Timestamp;

    inline static TimestampValue create(int year, int month, int day, int hour, int minute, int second);

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

    void floor_to_second_period(int period);
    void floor_to_minute_period(int period);
    void floor_to_hour_period(int period);
    void floor_to_day_period(int period);
    void floor_to_month_period(int period);
    void floor_to_year_period(int period);
    void floor_to_week_period(int period);
    void floor_to_quarter_period(int period);

    bool from_string(const char* date_str, size_t len);

    int64_t to_unix_second() const;

    bool from_unixtime(int64_t second, const std::string& timezone);
    bool from_unixtime(int64_t second, const cctz::time_zone& ctz);

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

TimestampValue TimestampValue::create(int year, int month, int day, int hour, int minute, int second) {
    TimestampValue ts;
    ts.from_timestamp(year, month, day, hour, minute, second, 0);
    return ts;
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

} // namespace starrocks::vectorized

namespace std {
template <>
struct hash<starrocks::vectorized::TimestampValue> {
    size_t operator()(const starrocks::vectorized::TimestampValue& v) const {
        return std::hash<int64_t>()(v._timestamp);
    }
};
} // namespace std
