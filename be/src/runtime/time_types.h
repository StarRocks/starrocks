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

#include <cstdint>
#include <string>

#include "common/compiler_util.h"
#include "util/raw_container.h"

namespace starrocks {

// Date: Julian Date -2000-01-01 ~ 9999-01-01
// MAX USE 22 bits
typedef int32_t JulianDate;

// Timestamp: {Julian Date}{microsecond in one day, 0 ~ 86400000000}
// JulianDate use high 22 bits, microsecond use low 40 bits
typedef int64_t Timestamp;

static const uint8_t TIMESTAMP_BITS = 40;

static const Timestamp TIMESTAMP_BITS_TIME{UINT64_MAX >> 24};

// TimeUnit
enum TimeUnit {
    MICROSECOND,
    MILLISECOND,
    SECOND,
    MINUTE,
    HOUR,
    DAY,
    WEEK,
    MONTH,
    QUARTER,
    YEAR,
    SECOND_MICROSECOND,
    MINUTE_MICROSECOND,
    MINUTE_SECOND,
    HOUR_MICROSECOND,
    HOUR_SECOND,
    HOUR_MINUTE,
    DAY_MICROSECOND,
    DAY_SECOND,
    DAY_MINUTE,
    DAY_HOUR,
    YEAR_MONTH
};

// const value
static const int32_t MONTHS_PER_YEAR = 12;
static const int32_t SECS_PER_DAY = 86400;
static const int32_t SECS_PER_HOUR = 3600;
static const int32_t SECS_PER_MINUTE = 60;
static const int32_t MINS_PER_HOUR = 60;
static const int32_t HOURS_PER_DAY = 24;

static const int64_t USECS_PER_WEEK = 604800000000;
static const int64_t USECS_PER_DAY = 86400000000;
static const int64_t USECS_PER_HOUR = 3600000000;
static const int64_t USECS_PER_MINUTE = 60000000;
static const int64_t USECS_PER_SEC = 1000000;
static const int64_t USECS_PER_MILLIS = 1000;

static const int64_t NANOSECS_PER_USEC = 1000;
static const int64_t NANOSECS_PER_SEC = 1000000000;

// Corresponding to TimeUnit
static constexpr int64_t USECS_PER_UNIT[] = {
        1,                // Microsecond
        USECS_PER_MILLIS, // Millisecond
        USECS_PER_SEC,    // Second
        USECS_PER_MINUTE, // Minute
        USECS_PER_HOUR,   // Hour
        // not support calculate
        0, // Day
        0, // Week
        0, // Month
        0, // Quarter
        0, // Year
};

static constexpr uint8_t DAYS_IN_MONTH[2][13] = {{0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
                                                 {0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}};

// -------------------------------------- Date Function --------------------------------
class date {
public:
    static void init_date_cache();

    static int get_week_of_year(JulianDate julian);

    static bool get_weeks_of_year_with_cache(JulianDate julian, int* weeks);

    static int get_days_after_monday(JulianDate julian);

    static bool is_leap(int year);

    static int64_t standardize_date(int64_t value);

    inline static void to_date(JulianDate julian, int* year, int* month, int* day);

    inline static void to_date_with_cache(JulianDate julian, int* year, int* month, int* day);

    static bool check(int year, int month, int day);

    static JulianDate from_date(int year, int month, int day);

    // From Date Literal: 20100101
    static JulianDate from_date_literal(uint64_t date_literal);

    static JulianDate from_mysql_date(uint32_t mysql_date);

    template <TimeUnit UNIT>
    static JulianDate add(JulianDate date, int count);

    static void to_string(int year, int month, int day, char* to);

    inline static std::string to_string(int year, int month, int day);

    inline static std::string to_string(JulianDate date);

    inline static Timestamp to_timestamp(JulianDate date);

    // This method determine whether a character of value is a number
    // if it is then set the digit to v, and return true;
    // else return false;
    inline static bool char_to_digit(const char* value, int i, uint8_t* v);

    // Get date base on format "%Y-%m-%d", '-' means any char.
    // compare every char.
    static bool from_string_to_date_internal(const char* ptr, int* year, int* month, int* day);

    // process string based on format like "%Y-%m-%d %H:%i:%s",
    // if successful return true;
    // else return false;
    static bool from_string_to_datetime_internal(const char* ptr_date, const char* ptr_time, int* year, int* month,
                                                 int* day, int* hour, int* minute, int* second, int* microsecond);

    // obtain datetime based on any string format.
    static bool from_string(const char* date_str, size_t len, int* year, int* month, int* day, int* hour, int* minute,
                            int* second, int* microsecond);
    static bool from_string_to_date(const char* date_str, size_t len, int* year, int* month, int* day);
    static bool is_standard_datetime_format(const char* ptr, int length, const char** ptr_time);
    static bool from_string_to_datetime(const char* date_str, size_t len, int* year, int* month, int* day, int* hour,
                                        int* minute, int* second, int* microsecond);

public:
    // from_date(1970, 1, 1)
    static constexpr JulianDate UNIX_EPOCH_JULIAN = 2440588;
    // from_date(0000, 0, 0)
    static constexpr JulianDate ZERO_EPOCH_JULIAN = 1721028;
    // from_date(0000, 1, 1)
    static constexpr JulianDate BC_EPOCH_JULIAN = 1721060;
    // from_date(0001, 1, 1)
    static constexpr JulianDate AD_EPOCH_JULIAN = 1721426;

    // from_date(0000, 1, 1)
    static constexpr JulianDate MIN_DATE = 1721060;

    // from_date(9999, 12, 31)
    static constexpr JulianDate MAX_DATE = 5373484;

    static constexpr JulianDate INVALID_DATE = MAX_DATE + 1;
};

// ------------------------------------ Timestamp Function ------------------------------
class timestamp {
public:
    static void to_time(Timestamp timestamp, int* hour, int* minute, int* second, int* microsecond);

    // get time without date
    inline static Timestamp to_time(Timestamp timestamp);

    inline static JulianDate to_julian(Timestamp timestamp);

    inline static void to_date(Timestamp timestamp, int* year, int* month, int* day);

    inline static Timestamp from_datetime(int year, int month, int day, int hour, int minute, int second,
                                          int microsecond);

    inline static void to_datetime(Timestamp timestamp, int* year, int* month, int* day, int* hour, int* minute,
                                   int* second, int* microsecond);

    inline static Timestamp from_time(int hour, int minute, int second, int microsecond);

    inline static Timestamp from_julian_and_time(JulianDate julian, Timestamp microsecond);

    static bool check_time(int hour, int minute, int second, int microsecond);

    inline static bool check(int year, int month, int day, int hour, int minute, int second, int microsecond);

    template <TimeUnit UNIT>
    static Timestamp add(Timestamp timestamp, int count);

    template <bool use_iso8601_format = false>
    static std::string to_string(Timestamp timestamp);

    // Returns the length of formatted string or -1 if the size of buffer too
    // small to fill the formatted string.
    template <bool use_iso8601_format = false>
    static int to_string(Timestamp timestamp, char* s, size_t n);

    inline static double time_to_literal(double time);

    inline static Timestamp of_epoch_second(int64_t seconds, int64_t microseconds);

public:
    // MAX_DATE | USECS_PER_DAY
    static const Timestamp MAX_TIMESTAMP = (5908208226068291583LL);

    // MIN_DATE | 0
    static const Timestamp MIN_TIMESTAMP = (1892325482100162560LL);

    // seconds from 1970.01.01
    static const Timestamp UNIX_EPOCH_SECONDS = (210866803200LL);
};

// ========================== date inline function ===============================

template <TimeUnit UNIT>
JulianDate date::add(JulianDate date, int count) {
    if constexpr (UNIT == TimeUnit::DAY) {
        return date + count;
    } else if constexpr (UNIT == TimeUnit::WEEK) {
        return date + 7 * count;
    } else if constexpr (UNIT == TimeUnit::MONTH || UNIT == TimeUnit::QUARTER) {
        int year, month, day;
        to_date_with_cache(date, &year, &month, &day);

        if (UNIT == TimeUnit::QUARTER) {
            count = 3 * count;
        }
        int months = year * 12 + month - 1 + count;
        if (months < 0) {
            // @INFO: NOT SUPPORT BCE
            return date::INVALID_DATE;
        } else {
            year = months / 12;
            month = (months % 12) + 1;
            int max_day = DAYS_IN_MONTH[is_leap(year)][month];
            day = day > max_day ? max_day : day;
            return from_date(year, month, day);
        }
    } else {
        int year, month, day;
        to_date_with_cache(date, &year, &month, &day);

        year += count;
        // year must satisfy condition: 0 <= year <= 9999
        // because original year must satify this condition,
        // so count must satify: -year <= count <= (9999 - year)
        // when year and count all signed int, we can replace
        // it with "static_cast<unsigned int>(count + year) <= 9999"
        // to reduce one compare, so we use updated year here.
        if (static_cast<unsigned int>(year) > 9999) {
            return date::INVALID_DATE;
        }

        if (month == 2 && day == 29 && !is_leap(year)) {
            day = 28;
        }
        return from_date(year, month, day);
    }
}

std::string date::to_string(int year, int month, int day) {
    char to[10];
    date::to_string(year, month, day, to);
    return std::string(to, 10);
}

std::string date::to_string(JulianDate date) {
    int year, month, day;
    to_date_with_cache(date, &year, &month, &day);
    return to_string(year, month, day);
}

Timestamp date::to_timestamp(JulianDate date) {
    return ((Timestamp)date) << TIMESTAMP_BITS;
}

// This method determine whether a character of value is a number
// if it is then set the digit to v, and return true;
// else return false;
bool date::char_to_digit(const char* value, int i, uint8_t* v) {
    *v = *(value + i) - '0';
    if (*v > 9) {
        return true;
    } else {
        return false;
    }
}

// ============================== Timestamp inline function ==================================

Timestamp timestamp::to_time(Timestamp timestamp) {
    return timestamp & TIMESTAMP_BITS_TIME;
}

JulianDate timestamp::to_julian(Timestamp timestamp) {
    return static_cast<uint64_t>(timestamp) >> TIMESTAMP_BITS;
}

void timestamp::to_date(Timestamp timestamp, int* year, int* month, int* day) {
    date::to_date_with_cache(to_julian(timestamp), year, month, day);
}

Timestamp timestamp::from_datetime(int year, int month, int day, int hour, int minute, int second, int microsecond) {
    return timestamp::from_julian_and_time(date::from_date(year, month, day),
                                           timestamp::from_time(hour, minute, second, microsecond));
}

void timestamp::to_datetime(Timestamp timestamp, int* year, int* month, int* day, int* hour, int* minute, int* second,
                            int* microsecond) {
    date::to_date_with_cache(to_julian(timestamp), year, month, day);
    timestamp::to_time(timestamp, hour, minute, second, microsecond);
}

Timestamp timestamp::from_julian_and_time(JulianDate julian, Timestamp microsecond) {
    return date::to_timestamp(julian) | microsecond;
}

Timestamp timestamp::from_time(int hour, int minute, int second, int microsecond) {
    return (((((hour * MINS_PER_HOUR) + minute) * SECS_PER_MINUTE) + second) * USECS_PER_SEC) + microsecond;
}

bool timestamp::check(int year, int month, int day, int hour, int minute, int second, int microsecond) {
    return date::check(year, month, day) && check_time(hour, minute, second, microsecond);
}

inline void timestamp::to_time(Timestamp timestamp, int* hour, int* minute, int* second, int* microsecond) {
    Timestamp time = to_time(timestamp);

    *hour = time / USECS_PER_HOUR;
    time -= (*hour) * USECS_PER_HOUR;
    *minute = time / USECS_PER_MINUTE;
    time -= (*minute) * USECS_PER_MINUTE;
    *second = time / USECS_PER_SEC;
    *microsecond = time - (*second * USECS_PER_SEC);
}

template <TimeUnit UNIT>
Timestamp timestamp::add(Timestamp timestamp, int count) {
    if constexpr (static_cast<int>(UNIT) > static_cast<int>(TimeUnit::HOUR)) {
        JulianDate julian = date::add<UNIT>(to_julian(timestamp), count);

        if (julian > date::MAX_DATE || julian < date::MIN_DATE) {
            julian = date::INVALID_DATE;
        }

        return date::to_timestamp(julian) | timestamp::to_time(timestamp);
    } else {
        JulianDate days = timestamp::to_julian(timestamp);
        Timestamp microseconds = timestamp::to_time(timestamp) + USECS_PER_UNIT[static_cast<int>(UNIT)] * count;

        days += microseconds / USECS_PER_DAY;

        microseconds %= USECS_PER_DAY;
        if (microseconds < 0) {
            microseconds += USECS_PER_DAY;
            days--;
        }

        return (date::to_timestamp(days) | microseconds);
    }
}

double timestamp::time_to_literal(double time) {
    uint64_t t = time;
    uint64_t hour = t / 3600;
    uint64_t minute = t / 60 % 60;
    uint64_t second = t % 60;
    return hour * 10000 + minute * 100 + second;
}

Timestamp timestamp::of_epoch_second(int64_t seconds, int64_t nanoseconds) {
    int64_t second = seconds + timestamp::UNIX_EPOCH_SECONDS;
    JulianDate day = second / SECS_PER_DAY;
    return timestamp::from_julian_and_time(day, second * USECS_PER_SEC + nanoseconds / NANOSECS_PER_USEC);
}

struct JulianToDateEntry {
    // 14 bits
    uint16_t year;

    // 4 bits
    uint8_t month;

    // 5 bits
    uint8_t day;

    // 1-53, Base on 6 bits
    uint8_t week_th_of_year;
};

const constexpr uint32_t CACHE_JULIAN_DAYS = 200 * 366;
extern JulianToDateEntry g_julian_to_date_cache[];

inline void date::to_date(JulianDate julian, int* year, int* month, int* day) {
    if (UNLIKELY(julian == ZERO_EPOCH_JULIAN)) {
        *year = 0;
        *month = 0;
        *day = 0;
        return;
    }

    int quad;
    int extra;
    int y;

    julian += 32044;
    quad = julian / 146097;
    extra = (julian - quad * 146097) * 4 + 3;
    julian += 60 + quad * 3 + extra / 146097;
    quad = julian / 1461;
    julian -= quad * 1461;
    y = julian * 4 / 1461;
    julian = ((y != 0) ? ((julian + 305) % 365) : ((julian + 306) % 366)) + 123;
    y += quad * 4;
    quad = julian * 2141 / 65536;

    *year = y - 4800;
    *day = julian - 7834 * quad / 256;
    *month = (quad + 10) % MONTHS_PER_YEAR + 1;
}

inline void date::to_date_with_cache(JulianDate julian, int* year, int* month, int* day) {
    if (julian >= date::UNIX_EPOCH_JULIAN && julian < date::UNIX_EPOCH_JULIAN + CACHE_JULIAN_DAYS) {
        int p = julian - date::UNIX_EPOCH_JULIAN;
        *year = g_julian_to_date_cache[p].year;
        *month = g_julian_to_date_cache[p].month;
        *day = g_julian_to_date_cache[p].day;

        return;
    }

    return to_date(julian, year, month, day);
}

template <bool use_iso8601_format>
std::string timestamp::to_string(Timestamp timestamp) {
    std::string s;
    raw::make_room(&s, 26);
    int len = to_string<use_iso8601_format>(timestamp, s.data(), s.size());
    s.resize(len);
    return s;
}

template <bool use_iso8601_format>
int timestamp::to_string(Timestamp timestamp, char* to, size_t n) {
    int year, month, day;
    int hour, minute, second, microsecond;
    date::to_date_with_cache(timestamp::to_julian(timestamp), &year, &month, &day);
    to_time(timestamp, &hour, &minute, &second, &microsecond);

    if (n < 19) {
        return -1;
    }

    date::to_string(year, month, day, to);

    if constexpr (use_iso8601_format) {
        to[10] = (char)('T');
    } else {
        to[10] = (char)(' ');
    }
    to[11] = (char)('0' + (hour / 10));
    to[12] = (char)('0' + (hour % 10));
    to[13] = ':';
    // Minute
    to[14] = (char)('0' + (minute / 10));
    to[15] = (char)('0' + (minute % 10));
    to[16] = ':';
    /* Second */
    to[17] = (char)('0' + (second / 10));
    to[18] = (char)('0' + (second % 10));
    if (use_iso8601_format || microsecond > 0) {
        if (n < 26) {
            return -1;
        }
        to[19] = '.';
        uint32_t first = microsecond / 10000;
        uint32_t second = (microsecond % 10000) / 100;
        uint32_t third = microsecond % 100;
        to[20] = (char)('0' + first / 10);
        to[21] = (char)('0' + first % 10);
        to[22] = (char)('0' + second / 10);
        to[23] = (char)('0' + second % 10);
        to[24] = (char)('0' + third / 10);
        to[25] = (char)('0' + third % 10);
        return 26;
    }
    return 19;
}

} // namespace starrocks
