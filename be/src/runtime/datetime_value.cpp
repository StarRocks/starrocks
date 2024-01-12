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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/datetime_value.cpp

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

#include "runtime/datetime_value.h"

#include <cctz/civil_time.h>
#include <cctz/time_zone.h>
#include <fmt/format.h>

#include <cctype>
#include <cstring>
#include <ctime>
#include <limits>
#include <sstream>
#include <string_view>

#include "util/timezone_utils.h"

namespace starrocks {

const uint64_t log_10_int[] = {1,         10,         100,         1000,         10000UL,       100000UL,
                               1000000UL, 10000000UL, 100000000UL, 1000000000UL, 10000000000UL, 100000000000UL};

static int s_days_in_month[13] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
static const char* s_month_name[] = {"",     "January", "February",  "March",   "April",    "May",      "June",
                                     "July", "August",  "September", "October", "November", "December", nullptr};
static const char* s_ab_month_name[] = {"",    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", nullptr};
static const char* s_day_name[] = {"Monday", "Tuesday",  "Wednesday", "Thursday",
                                   "Friday", "Saturday", "Sunday",    nullptr};
static const char* s_ab_day_name[] = {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun", nullptr};

static bool str_to_int64(const char* ptr, const char** endptr, int64_t* ret);
static int check_word(const char* lib[], const char* str, const char* end, const char** endptr);

namespace joda {

//  Symbol  Meaning                      Presentation  Examples
//  ------  -------                      ------------  -------
//  G       era                          text          AD
//  C       century of era (>=0)         number        20
//  Y       year of era (>=0)            year          1996

//  x       weekyear                     year          1996
//  w       week of weekyear             number        27
//  e       day of week                  number        2
//  E       day of week                  text          Tuesday; Tue

//  y       year                         year          1996
//  D       day of year                  number        189
//  M       month of year                month         July; Jul; 07
//  d       day of month                 number        10

//  a       halfday of day               text          PM

//  K       hour of halfday (0~11)       number        0
//  h       clockhour of halfday (1~12)  number        12
//  H       hour of day (0~23)           number        0
//  k       clockhour of day (1~24)      number        24

//  m       minute of hour               number        30
//  s       second of minute             number        55
//  S       fraction of second           millis        978

//  z       time zone                    text          Pacific Standard Time; PST
//  Z       time zone offset/id          zone          -0800; -08:00; America/Los_Angeles

//  '       escape for text              delimiter
//  ''      single quote                 literal       '
enum JodaFormatChar : char {
    ERA = 'G',
    CENURY = 'C',
    YEAR_OF_ERA = 'Y',

    WEEK_YEAR = 'x',
    WEEK_OF_WEEKYEAR = 'w',
    DAY_OF_WEEK_NUM = 'e',
    DAY_OF_WEEK = 'E',

    YEAR = 'y',
    DAY_OF_YEAR = 'D',
    MONTH_OF_YEAR = 'M',
    DAY_OF_MONTH = 'd',

    HALFDAY_OF_DAY = 'a',
    HOUR_OF_HALFDAY = 'K',
    CLOCKHOUR_OF_HALFDAY = 'h',

    HOUR_OF_DAY = 'H',
    CLOCKHOUR_OF_DAY = 'k',
    MINUTE_OF_HOUR = 'm',
    SECOND_OF_MINUTE = 's',
    FRACTION_OF_SECOND = 'S',

    TIME_ZONE = 'z',
    TIME_ZONE_OFFSET = 'Z',
};

bool JodaFormat::prepare(std::string_view format) {
    const char* ptr = format.data();
    const char* end = format.data() + format.length();

    while (ptr < end) {
        const char* next_ch_ptr = ptr;
        uint32_t repeat_count = 0;
        for (char ch = *ptr; ch == *next_ch_ptr && next_ch_ptr < end; ++next_ch_ptr) {
            ++repeat_count;
        }

        char ch = *ptr;
        switch (ch) {
        case joda::JodaFormatChar::ERA:
        case joda::JodaFormatChar::CENURY:
            // NOT SUPPORTED
            return false;
        case joda::JodaFormatChar::WEEK_OF_WEEKYEAR: {
            _token_parsers.emplace_back([&]() {
                const char* tmp = val + std::min<int>(2, val_end - val);
                int64_t int_value = 0;
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                week_num = int_value;
                if (week_num > 53 || (strict_week_number && week_num == 0)) {
                    return false;
                }
                val = tmp;
                date_part_used = true;
                return true;
            });
            break;
        }
        case joda::JodaFormatChar::DAY_OF_WEEK_NUM: {
            _token_parsers.emplace_back([&]() {
                int64_t int_value = 0;
                const char* tmp = val + std::min<int>(1, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                if (int_value >= 7) {
                    return false;
                }
                if (int_value == 0) {
                    int_value = 7;
                }
                weekday = int_value;
                val = tmp;
                date_part_used = true;
                return true;
            });
            break;
        }
        case joda::JodaFormatChar::DAY_OF_WEEK: {
            if (repeat_count <= 3) {
                _token_parsers.emplace_back([&]() {
                    int64_t int_value = 0;
                    int_value = check_word(s_ab_day_name, val, val_end, &val);
                    if (int_value < 0) {
                        return false;
                    }
                    int_value++;
                    weekday = int_value;
                    date_part_used = true;
                    return true;
                });
            } else {
                _token_parsers.emplace_back([&]() {
                    int64_t int_value = check_word(s_day_name, val, val_end, &val);
                    if (int_value < 0) {
                        return false;
                    }
                    int_value++;
                    weekday = int_value;
                    date_part_used = true;
                    return true;
                });
            }
            break;
        }
        case joda::JodaFormatChar::WEEK_YEAR:
        case joda::JodaFormatChar::YEAR_OF_ERA:
        case joda::JodaFormatChar::YEAR: {
            _token_parsers.emplace_back([&]() {
                // year
                int64_t int_value = 0;
                const char* tmp = val + std::min<int>(4, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                if (tmp - val <= 2) {
                    int_value += int_value >= 70 ? 1900 : 2000;
                }
                _year = int_value;
                val = tmp;
                date_part_used = true;
                return true;
            });
            break;
        }
        case joda::JodaFormatChar::DAY_OF_YEAR: {
            _token_parsers.emplace_back([&, repeat_count]() {
                int64_t int_value = 0;
                const char* tmp = val + std::min<int>(repeat_count, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                yearday = int_value;
                val = tmp;
                date_part_used = true;
                return true;
            });
            break;
        }
        case joda::JodaFormatChar::MONTH_OF_YEAR:
            // month of year
            if (repeat_count == 2) {
                _token_parsers.emplace_back([&]() {
                    int64_t int_value = 0;
                    const char* tmp = val + std::min<int>(2, val_end - val);
                    if (!str_to_int64(val, &tmp, &int_value)) {
                        return false;
                    }
                    _month = int_value;
                    val = tmp;
                    date_part_used = true;
                    return true;
                });
            } else if (repeat_count == 3) {
                _token_parsers.emplace_back([&]() {
                    int64_t int_value = 0;
                    int_value = check_word(s_ab_month_name, val, val_end, &val);
                    if (int_value < 0) {
                        return false;
                    }
                    _month = int_value;
                    return true;
                });

            } else if (repeat_count == 4) {
                _token_parsers.emplace_back([&]() {
                    int64_t int_value = 0;
                    int_value = check_word(s_month_name, val, val_end, &val);
                    if (int_value < 0) {
                        return false;
                    }
                    _month = int_value;
                    return true;
                });
            } else {
                return false;
            }
            break;
        case joda::JodaFormatChar::DAY_OF_MONTH: {
            _token_parsers.emplace_back([&]() {
                int64_t int_value = 0;
                const char* tmp = val + std::min<int>(2, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                _day = int_value;
                val = tmp;
                date_part_used = true;
                return true;
            });
            break;
        }
        case joda::JodaFormatChar::HALFDAY_OF_DAY: {
            _token_parsers.emplace_back([&]() {
                if ((val_end - val) < 2 || toupper(*(val + 1)) != 'M') {
                    return false;
                }
                if (toupper(*val) == 'P') {
                    // PM
                    halfday = 12;
                }
                time_part_used = true;
                val += 2;
                return true;
            });
            break;
        }
        case joda::JodaFormatChar::CLOCKHOUR_OF_HALFDAY:
        case joda::JodaFormatChar::CLOCKHOUR_OF_DAY:
            _token_parsers.emplace_back([&, ch, repeat_count]() {
                int64_t int_value = 0;
                const char* tmp = val + std::min<int>(repeat_count, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                if (UNLIKELY(ch == joda::JodaFormatChar::CLOCKHOUR_OF_DAY && int_value > 24)) {
                    return false;
                }
                if (UNLIKELY(ch == joda::JodaFormatChar::CLOCKHOUR_OF_HALFDAY && int_value > 12)) {
                    return false;
                }
                _hour = int_value;
                val = tmp;
                time_part_used = true;
                return true;
            });
            break;
        case joda::JodaFormatChar::HOUR_OF_HALFDAY:
        case joda::JodaFormatChar::HOUR_OF_DAY: {
            _token_parsers.emplace_back([&, ch, repeat_count]() {
                int64_t int_value = 0;
                const char* tmp = val + std::min<int>(repeat_count, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                if (UNLIKELY(ch == joda::JodaFormatChar::HOUR_OF_DAY && int_value > 23)) {
                    return false;
                }
                if (UNLIKELY(ch == joda::JodaFormatChar::HOUR_OF_HALFDAY && int_value > 11)) {
                    return false;
                }

                _hour = int_value;
                val = tmp;
                time_part_used = true;
                return true;
            });
            break;
        }
        case joda::JodaFormatChar::MINUTE_OF_HOUR:
            _token_parsers.emplace_back([&, repeat_count]() {
                int64_t int_value = 0;
                const char* tmp = val + std::min<int>(repeat_count, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                _minute = int_value;
                val = tmp;
                time_part_used = true;
                return true;
            });
            break;
        case joda::JodaFormatChar::SECOND_OF_MINUTE:
            _token_parsers.emplace_back([&]() {
                int64_t int_value = 0;
                const char* tmp = val + std::min<int>(2, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                _second = int_value;
                val = tmp;
                time_part_used = true;
                return true;
            });
            break;
        case joda::JodaFormatChar::FRACTION_OF_SECOND:
            _token_parsers.emplace_back([&, repeat_count]() {
                int64_t int_value = 0;
                const char* tmp = val + std::min<int>(repeat_count, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                // The exact number of fractional digits. If more millisecond digits are available then specified the number
                // will be truncated, if there are fewer than specified then the number will be zero-padded to the right.
                // When parsing, only the exact number of digits are accepted.
                for (int actual_count = tmp - val; actual_count < 6; actual_count++) {
                    int_value *= 10;
                }
                _microsecond = int_value;
                val = tmp;
                time_part_used = true;
                frac_part_used = true;
                return true;
            });
            break;
        case joda::JodaFormatChar::TIME_ZONE:
        case joda::JodaFormatChar::TIME_ZONE_OFFSET: {
            _token_parsers.emplace_back([&]() {
                std::string_view tz(val, val_end);
                if (!TimezoneUtils::find_cctz_time_zone(tz, ctz)) {
                    return false;
                }
                has_timezone = true;
                return true;
            });
            break;
        }
        default: {
            _token_parsers.emplace_back([&, ch]() {
                if (ch != *val) {
                    return false;
                }
                val++;
                return true;
            });
            break;
        }
        }

        ptr += repeat_count;
    }

    _token_parsers.emplace_back([&]() {
        if (halfday > 0) {
            _hour = (_hour % 12) + halfday;
        }

        // Year day
        if (yearday > 0) {
            uint64_t days = calc_daynr(_year, 1, 1) + yearday - 1;
            if (!get_date_from_daynr(days)) {
                return false;
            }
        }
        // weekday
        if (week_num >= 0 && weekday > 0) {
            // Check
            if ((strict_week_number && (strict_week_number_year < 0 || strict_week_number_year_type != sunday_first)) ||
                (!strict_week_number && strict_week_number_year >= 0)) {
                return false;
            }
            uint64_t days = calc_daynr(strict_week_number ? strict_week_number_year : _year, 1, 1);

            uint8_t weekday_b = calc_weekday(days, sunday_first);

            if (sunday_first) {
                days += ((weekday_b == 0) ? 0 : 7) - weekday_b + (week_num - 1) * 7 + weekday % 7;
            } else {
                days += ((weekday_b <= 3) ? 0 : 7) - weekday_b + (week_num - 1) * 7 + weekday - 1;
            }
            if (!get_date_from_daynr(days)) {
                return false;
            }
        }

        // Compute timestamp type
        if (frac_part_used) {
            if (date_part_used) {
                _type = TIME_DATETIME;
            } else {
                _type = TIME_TIME;
            }
        } else {
            if (date_part_used) {
                if (time_part_used) {
                    _type = TIME_DATETIME;
                } else {
                    _type = TIME_DATE;
                }
            } else {
                _type = TIME_TIME;
            }
        }

        // Timezone
        if (has_timezone) {
            const auto tp = cctz::convert(cctz::civil_second(_year, _month, _day, _hour, _minute, _second), ctz);
            int64_t timestamp = tp.time_since_epoch().count();
            if (!from_unixtime(timestamp, TimezoneUtils::local_time_zone())) {
                return false;
            }
        }

        if (check_range() || check_date()) {
            return false;
        }
        _neg = false;

        return true;
    });
    return true;
}

bool JodaFormat::parse(std::string_view str, DateTimeValue* output) {
    val = str.data();
    val_end = str.data() + str.length();
    date_part_used = false;
    time_part_used = false;
    frac_part_used = false;
    _year = 2000;
    _month = 1;
    _day = 1;
    halfday = 0;
    yearday = -1;
    weekday = -1;
    week_num = -1;
    has_timezone = false;

    for (auto& p : _token_parsers) {
        if (!p()) {
            return false;
        }
    }
    *output = DateTimeValue(type(), year(), month(), day(), hour(), minute(), second(), microsecond());
    return true;
}

} // namespace joda

uint8_t mysql_week_mode(uint32_t mode) {
    mode &= 7;
    if (!(mode & WEEK_MONDAY_FIRST)) {
        mode ^= WEEK_FIRST_WEEKDAY;
    }
    return mode;
}

static bool is_leap(uint32_t year) {
    return ((year % 4) == 0) && ((year % 100 != 0) || ((year % 400) == 0 && year));
}

static uint32_t calc_days_in_year(uint32_t year) {
    return is_leap(year) ? 366 : 365;
}

DateTimeValue DateTimeValue::_s_min_datetime_value(0, TIME_DATETIME, 0, 0, 0, 0, 0, 1, 1);
DateTimeValue DateTimeValue::_s_max_datetime_value(0, TIME_DATETIME, 23, 59, 59, 0, 9999, 12, 31);
RE2 DateTimeValue::time_zone_offset_format_reg(R"(^[+-]{1}\d{2}\:\d{2}$)", re2::RE2::Quiet);

bool DateTimeValue::check_range() const {
    return _year > 9999 || _month > 12 || _day > 31 || _hour > (_type == TIME_TIME ? TIME_MAX_HOUR : 23) ||
           _minute > 59 || _second > 59 || _microsecond > 999999;
}

bool DateTimeValue::check_date() const {
    if (_month == 0 || _day == 0) {
        return true;
    }
    if (_day > s_days_in_month[_month]) {
        // Feb 29 in leap year is valid.
        if (_month == 2 && _day == 29 && is_leap(_year)) {
            return false;
        }
        return true;
    }
    return false;
}

// The interval format is that with no delimiters
// YYYY-MM-DD HH-MM-DD.FFFFFF AM in default format
// 0    1  2  3  4  5  6      7
bool DateTimeValue::from_date_str(const char* date_str, int len) {
    const char* ptr = date_str;
    const char* end = date_str + len;
    // ONLY 2, 6 can follow by a sapce
    const static int allow_space_mask = 4 | 64;
    const static int MAX_DATE_PARTS = 8;
    uint32_t date_val[MAX_DATE_PARTS];
    int32_t date_len[MAX_DATE_PARTS];

    _neg = false;
    // Skip space character
    while (ptr < end && isspace(*ptr)) {
        ptr++;
    }
    if (ptr == end || !isdigit(*ptr)) {
        return false;
    }
    // Fix year length
    const char* pos = ptr;
    while (pos < end && (isdigit(*pos) || *pos == 'T')) {
        pos++;
    }
    int year_len = 4;
    int digits = pos - ptr;
    bool is_interval_format = false;

    // Compatible with MySQL. Shit!!!
    // For YYYYMMDD/YYYYMMDDHHMMSS is 4 digits years
    if (pos == end || *pos == '.') {
        if (digits == 4 || digits == 8 || digits >= 14) {
            year_len = 4;
        } else {
            year_len = 2;
        }
        is_interval_format = true;
    }

    int field_idx = 0;
    int field_len = year_len;
    while (ptr < end && isdigit(*ptr) && field_idx < MAX_DATE_PARTS - 1) {
        const char* start = ptr;
        int temp_val = 0;
        bool scan_to_delim = (!is_interval_format) && (field_idx != 6);
        while (ptr < end && isdigit(*ptr) && (scan_to_delim || field_len--)) {
            temp_val = temp_val * 10 + (*ptr++ - '0');
        }
        // Imposible
        if (temp_val > 999999L) {
            return false;
        }
        date_val[field_idx] = temp_val;
        date_len[field_idx] = ptr - start;
        field_len = 2;

        if (ptr == end) {
            field_idx++;
            break;
        }
        if (field_idx == 2 && *ptr == 'T') {
            // YYYYMMDDTHHMMDD, skip 'T' and continue
            ptr++;
            field_idx++;
            continue;
        }

        // Second part
        if (field_idx == 5) {
            if (*ptr == '.') {
                ptr++;
                field_len = 6;
            } else if (isdigit(*ptr)) {
                field_idx++;
                break;
            }
            field_idx++;
            continue;
        }
        // escape separator
        while (ptr < end && (ispunct(*ptr) || isspace(*ptr))) {
            if (isspace(*ptr)) {
                if (((1 << field_idx) & allow_space_mask) == 0) {
                    return false;
                }
            }
            ptr++;
        }
        field_idx++;
    }
    int num_field = field_idx;
    if (num_field <= 3) {
        _type = TIME_DATE;
    } else {
        _type = TIME_DATETIME;
    }
    if (!is_interval_format) {
        year_len = date_len[0];
    }
    for (; field_idx < MAX_DATE_PARTS; ++field_idx) {
        date_len[field_idx] = 0;
        date_val[field_idx] = 0;
    }
    _year = date_val[0];
    _month = date_val[1];
    _day = date_val[2];
    _hour = date_val[3];
    _minute = date_val[4];
    _second = date_val[5];
    _microsecond = date_val[6];
    if (_microsecond && date_len[6] < 6) {
        _microsecond *= log_10_int[6 - date_len[6]];
    }
    if (year_len == 2) {
        if (_year < YY_PART_YEAR) {
            _year += 2000;
        } else {
            _year += 1900;
        }
    }
    if (num_field < 3 || check_range()) {
        return false;
    }
    if (check_date()) {
        return false;
    }
    return true;
}

// [0, 101) invalid
// [101, (YY_PART_YEAR - 1) * 10000 + 1231] for two digits year 2000 ~ 2069
// [(YY_PART_YEAR - 1) * 10000 + 1231, YY_PART_YEAR * 10000L + 101) invalid
// [YY_PART_YEAR * 10000L + 101, 991231] for two digits year 1970 ~1999
// (991231, 10000101) invalid, because support 1000-01-01
// [10000101, 99991231] for four digits year date value.
// (99991231, 101000000) invalid, NOTE below this is datetime vaule hh:mm:ss must exist.
// [101000000, (YY_PART_YEAR - 1)##1231235959] two digits year datetime value
// ((YY_PART_YEAR - 1)##1231235959, YY_PART_YEAR##0101000000) invalid
// ((YY_PART_YEAR)##1231235959, 99991231235959] two digits year datetime value 1970 ~ 1999
// (999991231235959, ~) valid
int64_t DateTimeValue::standardlize_timevalue(int64_t value) {
    _type = TIME_DATE;
    if (value <= 0) {
        return 0;
    }
    if (value >= 10000101000000L) {
        // 9999-99-99 99:99:99
        if (value > 99999999999999L) {
            return 0;
        }

        // between 1000-01-01 00:00:00L and 9999-99-99 99:99:99
        // all digits exist.
        _type = TIME_DATETIME;
        return value;
    }
    // 2000-01-01
    if (value < 101) {
        return 0;
    }
    // two digits  year. 2000 ~ 2069
    if (value <= (YY_PART_YEAR - 1) * 10000L + 1231L) {
        return (value + 20000000L) * 1000000L;
    }
    // two digits year, invalid date
    if (value < YY_PART_YEAR * 10000L + 101) {
        return 0;
    }
    // two digits year. 1970 ~ 1999
    if (value <= 991231L) {
        return (value + 19000000L) * 1000000L;
    }
    // TODO(zhaochun): Don't allow year betwen 1000-01-01
    if (value < 10000101) {
        return 0;
    }
    // four digits years without hour.
    if (value <= 99991231L) {
        return value * 1000000L;
    }
    // below 0000-01-01
    if (value < 101000000) {
        return 0;
    }

    // below is with datetime, must have hh:mm:ss
    _type = TIME_DATETIME;
    // 2000 ~ 2069
    if (value <= (YY_PART_YEAR - 1) * 10000000000L + 1231235959L) {
        return value + 20000000000000L;
    }
    if (value < YY_PART_YEAR * 10000000000L + 101000000L) {
        return 0;
    }
    // 1970 ~ 1999
    if (value <= 991231235959L) {
        return value + 19000000000000L;
    }
    return value;
}

bool DateTimeValue::from_date_int64(int64_t value) {
    _neg = false;
    value = standardlize_timevalue(value);
    if (value <= 0) {
        return false;
    }
    uint64_t date = value / 1000000;
    uint64_t time = value % 1000000;

    _year = date / 10000;
    date %= 10000;
    _month = date / 100;
    _day = date % 100;
    _hour = time / 10000;
    time %= 10000;
    _minute = time / 100;
    _second = time % 100;
    _microsecond = 0;

    if (check_range() || check_date()) {
        return false;
    }
    return true;
}

void DateTimeValue::set_zero(int type) {
    memset(this, 0, sizeof(*this));
    _type = type;
}

void DateTimeValue::set_type(int type) {
    _type = type;
}

void DateTimeValue::set_max_time(bool neg) {
    set_zero(TIME_TIME);
    _hour = TIME_MAX_HOUR;
    _minute = TIME_MAX_MINUTE;
    _second = TIME_MAX_SECOND;
    _neg = neg;
}

bool DateTimeValue::from_time_int64(int64_t value) {
    _type = TIME_TIME;
    if (value > TIME_MAX_VALUE) {
        // 0001-01-01 00:00:00 to convert to a datetime
        if (value > 10000000000L) {
            if (from_date_int64(value)) {
                return true;
            }
        }
        set_max_time(false);
        return false;
    } else if (value < -1 * TIME_MAX_VALUE) {
        set_max_time(true);
        return false;
    }
    if (value < 0) {
        _neg = 1;
        value = -value;
    }
    _hour = value / 10000;
    value %= 10000;
    _minute = value / 100;
    if (_minute > TIME_MAX_MINUTE) {
        return false;
    }
    _second = value % 100;
    if (_second > TIME_MAX_SECOND) {
        return false;
    }
    return true;
}

char* DateTimeValue::append_date_string(char* to) const {
    uint32_t temp;
    // Year
    temp = _year / 100;
    *to++ = (char)('0' + (temp / 10));
    *to++ = (char)('0' + (temp % 10));
    temp = _year % 100;
    *to++ = (char)('0' + (temp / 10));
    *to++ = (char)('0' + (temp % 10));
    *to++ = '-';
    // Month
    *to++ = (char)('0' + (_month / 10));
    *to++ = (char)('0' + (_month % 10));
    *to++ = '-';
    // Day
    *to++ = (char)('0' + (_day / 10));
    *to++ = (char)('0' + (_day % 10));
    return to;
}

char* DateTimeValue::append_time_string(char* to) const {
    if (_neg) {
        *to++ = '-';
    }
    // Hour
    uint32_t temp = _hour;
    if (temp >= 100) {
        *to++ = (char)('0' + (temp / 100));
        temp %= 100;
    }
    *to++ = (char)('0' + (temp / 10));
    *to++ = (char)('0' + (temp % 10));
    *to++ = ':';
    // Minute
    *to++ = (char)('0' + (_minute / 10));
    *to++ = (char)('0' + (_minute % 10));
    *to++ = ':';
    /* Second */
    *to++ = (char)('0' + (_second / 10));
    *to++ = (char)('0' + (_second % 10));
    if (_microsecond > 0) {
        *to++ = '.';
        uint32_t first = _microsecond / 10000;
        uint32_t second = (_microsecond % 10000) / 100;
        uint32_t third = _microsecond % 100;
        *to++ = (char)('0' + first / 10);
        *to++ = (char)('0' + first % 10);
        *to++ = (char)('0' + second / 10);
        *to++ = (char)('0' + second % 10);
        *to++ = (char)('0' + third / 10);
        *to++ = (char)('0' + third % 10);
    }
    return to;
}

char* DateTimeValue::to_datetime_string(char* to) const {
    to = append_date_string(to);
    *to++ = ' ';
    to = append_time_string(to);
    *to++ = '\0';
    return to;
}

char* DateTimeValue::to_date_string(char* to) const {
    to = append_date_string(to);
    *to++ = '\0';
    return to;
}

char* DateTimeValue::to_time_string(char* to) const {
    to = append_time_string(to);
    *to++ = '\0';
    return to;
}

char* DateTimeValue::to_string(char* to) const {
    switch (_type) {
    case TIME_TIME:
        to = to_time_string(to);
        break;
    case TIME_DATE:
        to = to_date_string(to);
        break;
    case TIME_DATETIME:
        to = to_datetime_string(to);
        break;
    default:
        *to++ = '\0';
        break;
    }
    return to;
}

int64_t DateTimeValue::to_datetime_int64() const {
    return (_year * 10000L + _month * 100 + _day) * 1000000L + _hour * 10000 + _minute * 100 + _second;
}

int64_t DateTimeValue::to_date_int64() const {
    return _year * 10000L + _month * 100 + _day;
}

int64_t DateTimeValue::to_time_int64() const {
    int sign = _neg == 0 ? 1 : -1;
    return sign * (_hour * 10000L + _minute * 100 + _second);
}

int64_t DateTimeValue::to_int64() const {
    switch (_type) {
    case TIME_TIME:
        return to_time_int64();
    case TIME_DATE:
        return to_date_int64();
    case TIME_DATETIME:
        return to_datetime_int64();
    default:
        return 0;
    }
}

bool DateTimeValue::get_date_from_daynr(uint64_t daynr) {
    if (daynr <= 0 || daynr > DATE_MAX_DAYNR) {
        return false;
    }
    _year = daynr / 365;
    uint32_t days_befor_year = 0;
    while (daynr < (days_befor_year = calc_daynr(_year, 1, 1))) {
        _year--;
    }
    uint32_t days_of_year = daynr - days_befor_year + 1;
    int leap_day = 0;
    if (is_leap(_year)) {
        if (days_of_year > 31 + 28) {
            days_of_year--;
            if (days_of_year == 31 + 28) {
                leap_day = 1;
            }
        }
    }
    _month = 1;
    while (days_of_year > s_days_in_month[_month]) {
        days_of_year -= s_days_in_month[_month];
        _month++;
    }
    _day = days_of_year + leap_day;
    return true;
}

bool DateTimeValue::from_date_daynr(uint64_t daynr) {
    _neg = false;
    if (!get_date_from_daynr(daynr)) {
        return false;
    }
    _hour = 0;
    _minute = 0;
    _second = 0;
    _microsecond = 0;
    _type = TIME_DATE;
    return true;
}

// Following code is stolen from MySQL.
uint64_t DateTimeValue::calc_daynr(uint32_t year, uint32_t month, uint32_t day) {
    uint64_t delsum = 0;
    int y = year;

    if (year == 0 && month == 0) {
        return 0;
    }

    /* Cast to int to be able to handle month == 0 */
    delsum = 365 * y + 31 * (month - 1) + day;
    if (month <= 2) {
        // No leap year
        y--;
    } else {
        // This is great!!!
        // 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
        // 0, 0, 3, 3, 4, 4, 5, 5, 5,  6,  7,  8
        delsum -= (month * 4 + 23) / 10;
    }
    // Every 400 year has 97 leap year, 100, 200, 300 are not leap year.
    return delsum + y / 4 - y / 100 + y / 400;
}

static char* int_to_str(uint64_t val, char* to) {
    char buf[64];
    char* ptr = buf;
    // Use do/while for 0 value
    do {
        *ptr++ = '0' + (val % 10);
        val /= 10;
    } while (val);

    while (ptr > buf) {
        *to++ = *--ptr;
    }

    return to;
}

static char* append_string(const char* from, char* to) {
    while (*from) {
        *to++ = *from++;
    }
    return to;
}

static char* append_with_prefix(const char* str, int str_len, char prefix, int full_len, char* to) {
    int len = (str_len > full_len) ? str_len : full_len;
    len -= str_len;
    while (len-- > 0) {
        // push prefix;
        *to++ = prefix;
    }
    while (str_len-- > 0) {
        *to++ = *str++;
    }

    return to;
}

static char* append_with_suffix(const char* str, int str_len, char suffix, int full_len, char* to) {
    int len = (str_len > full_len) ? str_len : full_len;
    len -= str_len;
    while (str_len-- > 0) {
        *to++ = *str++;
    }
    while (len-- > 0) {
        *to++ = suffix;
    }

    return to;
}

int DateTimeValue::compute_format_len(const char* format, int len) {
    int size = 0;
    const char* ptr = format;
    const char* end = format + len;

    while (ptr < end) {
        if (*ptr != '%' || (ptr + 1) < end) {
            size++;
            ptr++;
            continue;
        }
        switch (*++ptr) {
        case 'M':
        case 'W':
            size += 10;
            break;
        case 'D':
        case 'Y':
        case 'x':
        case 'X':
            size += 4;
            break;
        case 'a':
        case 'b':
            size += 10;
            break;
        case 'j':
            size += 3;
            break;
        case 'u':
        case 'U':
        case 'v':
        case 'V':
        case 'y':
        case 'm':
        case 'd':
        case 'h':
        case 'i':
        case 'I':
        case 'l':
        case 'p':
        case 'S':
        case 's':
        case 'c':
        case 'e':
            size += 2;
            break;
        case 'k':
        case 'H':
            size += 7;
            break;
        case 'r':
            size += 11;
            break;
        case 'T':
            size += 8;
            break;
        case 'f':
            size += 6;
            break;
        case 'w':
        case '%':
        default:
            size++;
            break;
        }
    }
    return size;
}

bool DateTimeValue::to_joda_format_string(const char* format, int len, char* to) const {
    // max buffer size is 128
    // we need write a terminal zero
    constexpr int buffer_size = 127;
    const char* buffer_start = to;
    char buf[64];
    char* pos = nullptr;
    const char* ptr = format;
    const char* end = format + len;
    char ch = '\0';

    while (ptr < end) {
        int write_size = to - buffer_start;
        if (write_size == buffer_size) return false;
        // '\'' is the escape for text，the text in '\'' do not need to convert
        ch = *ptr;
        if (ch == '\'') {
            ++ptr;
            while (ptr < end && *ptr != '\'') {
                *to++ = *ptr++;
            }
            if (ptr < end && *ptr == '\'') {
                // skip the '\''
                ++ptr;
                continue;
            }
        }

        // compute the size of same char, this will determine if additional prefixes are required,
        // eg. format 'yyyyyy' need to display '002023'
        const char* next_ch_ptr = ptr;
        uint32_t same_ch_size = 0;
        uint32_t buf_size = 0;
        uint32_t actual_size = 0;
        ch = *ptr;
        for (; ch == *next_ch_ptr && next_ch_ptr < end; ++next_ch_ptr) {
            ++same_ch_size;
        }

        switch (ch) {
        case 'G':
            // era
            if (write_size + 2 >= buffer_size) return false;
            if (_year <= 0) {
                to = append_string("BC", to);
            } else {
                to = append_string("AD", to);
            }
            break;
        case 'C':
            // century of era
            pos = int_to_str(_year / 100, buf);
            buf_size = pos - buf;
            actual_size = std::max(buf_size, same_ch_size);
            if (write_size + actual_size >= buffer_size) return false;
            to = append_with_prefix(buf, pos - buf, '0', actual_size, to);
            break;
        case 'Y': {
            // year of era
            int output_year = same_ch_size == 2 ? (_year % 100) : _year;
            if (_year <= 0) {
                pos = int_to_str(1 - output_year, buf);
            } else {
                pos = int_to_str(output_year, buf);
            }
            buf_size = pos - buf;
            actual_size = std::max(buf_size, same_ch_size);
            if (write_size + actual_size >= buffer_size) return false;
            to = append_with_prefix(buf, pos - buf, '0', actual_size, to);
            break;
        }
        case 'x': {
            // weekyear
            if (_type == TIME_TIME) {
                return false;
            }
            uint32_t year = 0;
            calc_week(*this, mysql_week_mode(3), &year);
            int output_year = same_ch_size == 2 ? (year % 100) : year;
            pos = int_to_str(output_year, buf);
            buf_size = pos - buf;
            actual_size = std::max(buf_size, same_ch_size);
            if (write_size + actual_size >= buffer_size) return false;
            to = append_with_prefix(buf, pos - buf, '0', actual_size, to);
            break;
        }
        case 'w':
            // week of weekyear
            if (_type == TIME_TIME) {
                return false;
            }
            pos = int_to_str(week(mysql_week_mode(3)), buf);
            buf_size = pos - buf;
            actual_size = std::max(buf_size, same_ch_size);
            if (write_size + actual_size >= buffer_size) return false;
            to = append_with_prefix(buf, pos - buf, '0', actual_size, to);
            break;
        case 'e': {
            // day of week
            if (_type == TIME_TIME || (_month == 0 && _year == 0)) {
                return false;
            }
            uint8_t weekday = calc_weekday(daynr(), true);
            if (weekday == 0) weekday = 7;
            pos = int_to_str(weekday, buf);
            buf_size = pos - buf;
            actual_size = std::max(buf_size, same_ch_size);
            if (write_size + actual_size >= buffer_size) return false;
            to = append_with_prefix(buf, pos - buf, '0', actual_size, to);
            break;
        }
        case 'E':
            // weekday name (Sunday..Saturday)
            if (same_ch_size > 3) {
                if (write_size + 8 >= buffer_size) return false;
                to = append_string(s_day_name[weekday()], to);
            } else {
                if (write_size + 3 >= buffer_size) return false;
                if (_type == TIME_TIME || (_year == 0 && _month == 0)) {
                    return false;
                }
                to = append_string(s_ab_day_name[weekday()], to);
            }
            break;
        case 'y': {
            // year
            int output_year = same_ch_size == 2 ? _year % 100 : _year;
            pos = int_to_str(output_year, buf);
            buf_size = pos - buf;
            actual_size = std::max(buf_size, same_ch_size);
            if (write_size + actual_size >= buffer_size) return false;
            to = append_with_prefix(buf, pos - buf, '0', actual_size, to);
            break;
        }
        case 'D':
            // day of year (001..366)
            pos = int_to_str(daynr() - calc_daynr(_year, 1, 1) + 1, buf);
            buf_size = pos - buf;
            actual_size = std::max(buf_size, same_ch_size);
            if (write_size + actual_size >= buffer_size) return false;
            to = append_with_prefix(buf, pos - buf, '0', actual_size, to);
            break;
        case 'M':
            if (same_ch_size > 3) {
                // month name (January..December)
                if (write_size + 9 >= buffer_size) return false;
                if (_month == 0) {
                    return false;
                }
                to = append_string(s_month_name[_month], to);
                break;
            } else if (same_ch_size == 3) {
                // Abbreviated month name
                if (write_size + 3 >= buffer_size) return false;
                if (_month == 0) {
                    return false;
                }
                to = append_string(s_ab_month_name[_month], to);
                break;
            } else {
                // month, numeric (00..12)
                pos = int_to_str(_month, buf);
                buf_size = pos - buf;
                actual_size = std::max(buf_size, same_ch_size);
                if (write_size + actual_size >= buffer_size) return false;
                to = append_with_prefix(buf, pos - buf, '0', actual_size, to);
                break;
            }
        case 'd':
            // day of month (00...31)
            pos = int_to_str(_day, buf);
            buf_size = pos - buf;
            actual_size = std::max(buf_size, same_ch_size);
            if (write_size + actual_size >= buffer_size) return false;
            to = append_with_prefix(buf, pos - buf, '0', actual_size, to);
            break;
        case 'a':
            // AM or PM
            if (write_size + 2 >= buffer_size) return false;
            if ((_hour % 24) >= 12) {
                to = append_string("PM", to);
            } else {
                to = append_string("AM", to);
            }
            break;
        case 'K':
            // hour (0..11)
            pos = int_to_str((_hour % 24 + 12) % 12, buf);
            buf_size = pos - buf;
            actual_size = std::max(buf_size, same_ch_size);
            if (write_size + actual_size >= buffer_size) return false;
            to = append_with_prefix(buf, pos - buf, '0', actual_size, to);
            break;
        case 'h':
            // hour (1..12)
            pos = int_to_str((_hour % 24 + 11) % 12 + 1, buf);
            buf_size = pos - buf;
            actual_size = std::max(buf_size, same_ch_size);
            if (write_size + actual_size >= buffer_size) return false;
            to = append_with_prefix(buf, pos - buf, '0', actual_size, to);
            break;
        case 'H':
            // hour (0..23)
            pos = int_to_str(_hour, buf);
            buf_size = pos - buf;
            actual_size = std::max(buf_size, same_ch_size);
            if (write_size + actual_size >= buffer_size) return false;
            to = append_with_prefix(buf, pos - buf, '0', actual_size, to);
            break;
        case 'k':
            // hour (1..24)
            pos = int_to_str((_hour + 23) % 24 + 1, buf);
            buf_size = pos - buf;
            actual_size = std::max(buf_size, same_ch_size);
            if (write_size + actual_size >= buffer_size) return false;
            to = append_with_prefix(buf, pos - buf, '0', actual_size, to);
            break;
        case 'm':
            // minutes, numeric (00..59)
            pos = int_to_str(_minute, buf);
            buf_size = pos - buf;
            actual_size = std::max(buf_size, same_ch_size);
            if (write_size + 2 >= buffer_size) return false;
            to = append_with_prefix(buf, pos - buf, '0', actual_size, to);
            break;
        case 's':
            // seconds (00..59)
            pos = int_to_str(_second, buf);
            buf_size = pos - buf;
            actual_size = std::max(buf_size, same_ch_size);
            if (write_size + actual_size >= buffer_size) return false;
            to = append_with_prefix(buf, pos - buf, '0', actual_size, to);
            break;
        case 'S': {
            // fraction of second
            RETURN_IF(same_ch_size > 6, false);
            uint64_t val = _microsecond;
            for (int i = 0; i < 6 - same_ch_size; i++) {
                val /= 10;
            }
            pos = int_to_str(val, buf);
            buf_size = pos - buf;
            actual_size = std::max(buf_size, same_ch_size);
            if (write_size + actual_size >= buffer_size) return false;
            to = append_with_suffix(buf, pos - buf, '0', actual_size, to);
            break;
        }
        case 'z':
        case 'Z':
            // sr do not support datetime with timezone type， just ignore
            break;
        default:
            if (write_size + 1 >= buffer_size) return false;
            *to++ = ch;
            break;
        }

        ptr = next_ch_ptr;
    }
    *to++ = '\0';
    return true;
}

bool DateTimeValue::to_format_string(const char* format, int len, char* to) const {
    // max buffer size is 128
    // we need write a terminal zero
    constexpr int buffer_size = 127;
    const char* buffer_start = to;
    char buf[64];
    char* pos = nullptr;
    const char* ptr = format;
    const char* end = format + len;
    char ch = '\0';

    while (ptr < end) {
        int write_size = to - buffer_start;
        if (write_size == buffer_size) return false;

        if (*ptr != '%' || (ptr + 1) == end) {
            *to++ = *ptr++;
            continue;
        }
        // Skip '%'
        ptr++;
        switch (ch = *ptr++) {
        case 'a':
            // Abbreviated weekday name
            if (write_size + 3 >= buffer_size) return false;
            if (_type == TIME_TIME || (_year == 0 && _month == 0)) {
                return false;
            }
            to = append_string(s_ab_day_name[weekday()], to);
            break;
        case 'b':
            // Abbreviated month name
            if (write_size + 3 >= buffer_size) return false;
            if (_month == 0) {
                return false;
            }
            to = append_string(s_ab_month_name[_month], to);
            break;
        case 'c':
            // Month, numeric (0...12)
            if (write_size + 2 >= buffer_size) return false;
            pos = int_to_str(_month, buf);
            to = append_with_prefix(buf, pos - buf, '0', 1, to);
            break;
        case 'd':
            // Day of month (00...31)
            if (write_size + 2 >= buffer_size) return false;
            pos = int_to_str(_day, buf);
            to = append_with_prefix(buf, pos - buf, '0', 2, to);
            break;
        case 'D':
            // Day of the month with English suffix (0th, 1st, ...)
            if (write_size + 4 >= buffer_size) return false;
            pos = int_to_str(_day, buf);
            to = append_with_prefix(buf, pos - buf, '0', 1, to);
            if (_day >= 10 && _day <= 19) {
                to = append_string("th", to);
            } else {
                switch (_day % 10) {
                case 1:
                    to = append_string("st", to);
                    break;
                case 2:
                    to = append_string("nd", to);
                    break;
                case 3:
                    to = append_string("rd", to);
                    break;
                default:
                    to = append_string("th", to);
                    break;
                }
            }
            break;
        case 'e':
            // Day of the month, numeric (0..31)
            if (write_size + 2 >= buffer_size) return false;
            pos = int_to_str(_day, buf);
            to = append_with_prefix(buf, pos - buf, '0', 1, to);
            break;
        case 'f':
            if (write_size + 6 >= buffer_size) return false;
            // Microseconds (000000..999999)
            pos = int_to_str(_microsecond, buf);
            to = append_with_prefix(buf, pos - buf, '0', 6, to);
            break;
        case 'h':
        case 'I':
            // Hour (01..12)
            if (write_size + 2 >= buffer_size) return false;
            pos = int_to_str((_hour % 24 + 11) % 12 + 1, buf);
            to = append_with_prefix(buf, pos - buf, '0', 2, to);
            break;
        case 'H':
            // Hour (00..23)
            if (write_size + 2 >= buffer_size) return false;
            pos = int_to_str(_hour, buf);
            to = append_with_prefix(buf, pos - buf, '0', 2, to);
            break;
        case 'i':
            // Minutes, numeric (00..59)
            if (write_size + 2 >= buffer_size) return false;
            pos = int_to_str(_minute, buf);
            to = append_with_prefix(buf, pos - buf, '0', 2, to);
            break;
        case 'j':
            // Day of year (001..366)
            if (write_size + 3 >= buffer_size) return false;
            pos = int_to_str(daynr() - calc_daynr(_year, 1, 1) + 1, buf);
            to = append_with_prefix(buf, pos - buf, '0', 3, to);
            break;
        case 'k':
            // Hour (0..23)
            if (write_size + 2 >= buffer_size) return false;
            pos = int_to_str(_hour, buf);
            to = append_with_prefix(buf, pos - buf, '0', 1, to);
            break;
        case 'l':
            // Hour (1..12)
            if (write_size + 2 >= buffer_size) return false;
            pos = int_to_str((_hour % 24 + 11) % 12 + 1, buf);
            to = append_with_prefix(buf, pos - buf, '0', 1, to);
            break;
        case 'm':
            // Month, numeric (00..12)
            if (write_size + 2 >= buffer_size) return false;
            pos = int_to_str(_month, buf);
            to = append_with_prefix(buf, pos - buf, '0', 2, to);
            break;
        case 'M':
            // Month name (January..December)
            if (write_size + 9 >= buffer_size) return false;
            if (_month == 0) {
                return false;
            }
            to = append_string(s_month_name[_month], to);
            break;
        case 'p':
            // AM or PM
            if (write_size + 2 >= buffer_size) return false;
            if ((_hour % 24) >= 12) {
                to = append_string("PM", to);
            } else {
                to = append_string("AM", to);
            }
            break;
        case 'r':
            // Time, 12-hour (hh:mm:ss followed by AM or PM)
            if (write_size + 11 >= buffer_size) return false;
            *to++ = (char)('0' + (((_hour + 11) % 12 + 1) / 10));
            *to++ = (char)('0' + (((_hour + 11) % 12 + 1) % 10));
            *to++ = ':';
            // Minute
            *to++ = (char)('0' + (_minute / 10));
            *to++ = (char)('0' + (_minute % 10));
            *to++ = ':';
            /* Second */
            *to++ = (char)('0' + (_second / 10));
            *to++ = (char)('0' + (_second % 10));
            if ((_hour % 24) >= 12) {
                to = append_string(" PM", to);
            } else {
                to = append_string(" AM", to);
            }
            break;
        case 's':
        case 'S':
            // Seconds (00..59)
            if (write_size + 2 >= buffer_size) return false;
            pos = int_to_str(_second, buf);
            to = append_with_prefix(buf, pos - buf, '0', 2, to);
            break;
        case 'T':
            // Time, 24-hour (hh:mm:ss)
            if (write_size + 8 >= buffer_size) return false;
            *to++ = (char)('0' + ((_hour % 24) / 10));
            *to++ = (char)('0' + ((_hour % 24) % 10));
            *to++ = ':';
            // Minute
            *to++ = (char)('0' + (_minute / 10));
            *to++ = (char)('0' + (_minute % 10));
            *to++ = ':';
            /* Second */
            *to++ = (char)('0' + (_second / 10));
            *to++ = (char)('0' + (_second % 10));
            break;
        case 'u':
            // Week (00..53), where Monday is the first day of the week;
            // WEEK() mode 1
            if (write_size + 2 >= buffer_size) return false;
            if (_type == TIME_TIME) {
                return false;
            }
            pos = int_to_str(week(mysql_week_mode(1)), buf);
            to = append_with_prefix(buf, pos - buf, '0', 2, to);
            break;
        case 'U':
            // Week (00..53), where Sunday is the first day of the week;
            // WEEK() mode 0
            if (write_size + 2 >= buffer_size) return false;
            if (_type == TIME_TIME) {
                return false;
            }
            pos = int_to_str(week(mysql_week_mode(0)), buf);
            to = append_with_prefix(buf, pos - buf, '0', 2, to);
            break;
        case 'v':
            // Week (01..53), where Monday is the first day of the week;
            // WEEK() mode 3; used with %x
            if (write_size + 2 >= buffer_size) return false;
            if (_type == TIME_TIME) {
                return false;
            }
            pos = int_to_str(week(mysql_week_mode(3)), buf);
            to = append_with_prefix(buf, pos - buf, '0', 2, to);
            break;
        case 'V':
            // Week (01..53), where Sunday is the first day of the week;
            // WEEK() mode 2; used with %X
            if (write_size + 2 >= buffer_size) return false;
            if (_type == TIME_TIME) {
                return false;
            }
            pos = int_to_str(week(mysql_week_mode(2)), buf);
            to = append_with_prefix(buf, pos - buf, '0', 2, to);
            break;
        case 'w':
            // Day of the week (0=Sunday..6=Saturday)
            if (write_size + 8 >= buffer_size) return false;
            if (_type == TIME_TIME || (_month == 0 && _year == 0)) {
                return false;
            }
            pos = int_to_str(calc_weekday(daynr(), true), buf);
            to = append_with_prefix(buf, pos - buf, '0', 1, to);
            break;
        case 'W':
            // Weekday name (Sunday..Saturday)
            if (write_size + 8 >= buffer_size) return false;
            to = append_string(s_day_name[weekday()], to);
            break;
        case 'x': {
            // Year for the week, where Monday is the first day of the week,
            // numeric, four digits; used with %v
            if (_type == TIME_TIME) {
                return false;
            }
            uint32_t year = 0;
            calc_week(*this, mysql_week_mode(3), &year);
            pos = int_to_str(year, buf);
            to = append_with_prefix(buf, pos - buf, '0', 4, to);
            break;
        }
        case 'X': {
            // Year for the week where Sunday is the first day of the week,
            // numeric, four digits; used with %V
            if (write_size + 4 >= buffer_size) return false;
            if (_type == TIME_TIME) {
                return false;
            }
            uint32_t year = 0;
            calc_week(*this, mysql_week_mode(2), &year);
            pos = int_to_str(year, buf);
            to = append_with_prefix(buf, pos - buf, '0', 4, to);
            break;
        }
        case 'y':
            // Year, numeric (two digits)
            if (write_size + 2 >= buffer_size) return false;
            pos = int_to_str(_year % 100, buf);
            to = append_with_prefix(buf, pos - buf, '0', 2, to);
            break;
        case 'Y':
            // Year, numeric, four digits
            if (write_size + 4 >= buffer_size) return false;
            pos = int_to_str(_year, buf);
            to = append_with_prefix(buf, pos - buf, '0', 4, to);
            break;
        default:
            if (write_size + 1 >= buffer_size) return false;
            *to++ = ch;
            break;
        }
    }
    *to++ = '\0';
    return true;
}

uint8_t DateTimeValue::calc_week(const DateTimeValue& value, uint8_t mode, uint32_t* year) {
    bool monday_first = mode & WEEK_MONDAY_FIRST;
    bool week_year = mode & WEEK_YEAR;
    bool first_weekday = mode & WEEK_FIRST_WEEKDAY;
    uint64_t day_nr = value.daynr();
    uint64_t daynr_first_day = calc_daynr(value._year, 1, 1);
    uint8_t weekday_first_day = calc_weekday(daynr_first_day, !monday_first);

    int days = 0;
    *year = value._year;

    // Check wether the first days of this year belongs to last year
    if (value._month == 1 && value._day <= (7 - weekday_first_day)) {
        if (!week_year && ((first_weekday && weekday_first_day != 0) || (!first_weekday && weekday_first_day > 3))) {
            return 0;
        }
        (*year)--;
        week_year = true;
        daynr_first_day -= (days = calc_days_in_year(*year));
        weekday_first_day = (weekday_first_day + 53 * 7 - days) % 7;
    }

    // How many days since first week
    if ((first_weekday && weekday_first_day != 0) || (!first_weekday && weekday_first_day > 3)) {
        // days in new year belongs to last year.
        days = day_nr - (daynr_first_day + (7 - weekday_first_day));
    } else {
        // days in new year belongs to this year.
        days = day_nr - (daynr_first_day - weekday_first_day);
    }

    if (week_year && days >= 52 * 7) {
        weekday_first_day = (weekday_first_day + calc_days_in_year(*year)) % 7;
        if ((first_weekday && weekday_first_day == 0) || (!first_weekday && weekday_first_day <= 3)) {
            // Belong to next year.
            (*year)++;
            return 1;
        }
    }

    return days / 7 + 1;
}

uint8_t DateTimeValue::week(uint8_t mode) const {
    uint32_t year = 0;
    return calc_week(*this, mode, &year);
}

uint8_t DateTimeValue::calc_weekday(uint64_t day_nr, bool is_sunday_first_day) {
    return (day_nr + 5L + (is_sunday_first_day ? 1L : 0L)) % 7;
}

// TODO(zhaochun): Think endptr is NULL
// Return true if convert to a integer success. Otherwise false.
static bool str_to_int64(const char* ptr, const char** endptr, int64_t* ret) {
    const static uint64_t MAX_NEGATIVE_NUMBER = 0x8000000000000000;
    const static uint64_t ULONGLONG_MAX = ~0;
    const static uint64_t LFACTOR2 = 100000000000ULL;
    const char* end = *endptr;
    uint64_t cutoff_1 = 0;
    uint64_t cutoff_2 = 0;
    uint64_t cutoff_3 = 0;
    // Skip space
    while (ptr < end && (*ptr == ' ' || *ptr == '\t')) {
        ptr++;
    }
    if (ptr >= end) {
        return false;
    }
    // Sign
    bool neg = false;
    if (*ptr == '-') {
        neg = true;
        ptr++;
        cutoff_1 = MAX_NEGATIVE_NUMBER / LFACTOR2;
        cutoff_2 = (MAX_NEGATIVE_NUMBER % LFACTOR2) / 100;
        cutoff_3 = (MAX_NEGATIVE_NUMBER % LFACTOR2) % 100;
    } else {
        if (*ptr == '+') {
            ptr++;
        }
        cutoff_1 = ULONGLONG_MAX / LFACTOR2;
        cutoff_2 = (ULONGLONG_MAX % LFACTOR2) / 100;
        cutoff_3 = (ULONGLONG_MAX % LFACTOR2) % 100;
    }
    if (ptr >= end) {
        return false;
    }
    // Skip '0'
    while (ptr < end && *ptr == '0') {
        ptr++;
    }
    const char* n_end = ptr + 9;
    if (n_end > end) {
        n_end = end;
    }
    uint64_t value_1 = 0;
    while (ptr < n_end && isdigit(*ptr)) {
        value_1 *= 10;
        value_1 += *ptr++ - '0';
    }
    if (ptr == end || !isdigit(*ptr)) {
        *endptr = ptr;
        *ret = neg ? (~value_1 + 1) : value_1;
        return true;
    }
    // TODO
    uint64_t value_2 = 0;
    uint64_t value_3 = 0;

    // Check overflow.
    if (value_1 > cutoff_1 ||
        (value_1 == cutoff_1 && (value_2 > cutoff_2 || (value_2 == cutoff_2 && value_3 > cutoff_3)))) {
        return false;
    }
    return true;
}

static int min(int a, int b) {
    return a < b ? a : b;
}

static int find_in_lib(const char* lib[], const char* str, const char* end) {
    int pos = 0;
    int find_count = 0;
    int find_pos = 0;
    for (; lib[pos] != nullptr; ++pos) {
        const char* i = str;
        const char* j = lib[pos];
        while (i < end && *j) {
            if (toupper(*i) != toupper(*j)) {
                break;
            }
            ++i;
            ++j;
        }
        if (i == end) {
            if (*j == '\0') {
                return pos;
            } else {
                find_count++;
                find_pos = pos;
            }
        }
    }
    return find_count == 1 ? find_pos : -1;
}

static int check_word(const char* lib[], const char* str, const char* end, const char** endptr) {
    const char* ptr = str;
    while (ptr < end && isalpha(*ptr)) {
        ptr++;
    }
    int pos = find_in_lib(lib, str, ptr);
    if (pos >= 0) {
        *endptr = ptr;
    }
    return pos;
}

bool DateTimeValue::from_date_format_str(const char* format, int format_len, const char* value, int value_len,
                                         const char** sub_val_end) {
    const char* ptr = format;
    const char* end = format + format_len;
    const char* val = value;
    const char* val_end = value + value_len;

    bool date_part_used = false;
    bool time_part_used = false;
    bool frac_part_used = false;

    int day_part = 0;
    int weekday = -1;
    int yearday = -1;
    int week_num = -1;

    bool strict_week_number = false;
    bool sunday_first = false;
    bool strict_week_number_year_type = false;
    int strict_week_number_year = -1;
    bool usa_time = false;

    while (ptr < end && val < val_end) {
        // Skip space character
        while (val < val_end && isspace(*val)) {
            val++;
        }
        if (val >= val_end) {
            break;
        }
        // Check switch
        if (*ptr == '%' && ptr + 1 < end) {
            const char* tmp = nullptr;
            int64_t int_value = 0;
            ptr++;
            switch (*ptr++) {
            // Year
            case 'y':
                // Year, numeric (two digits)
                tmp = val + min(2, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                int_value += int_value >= 70 ? 1900 : 2000;
                _year = int_value;
                val = tmp;
                date_part_used = true;
                break;
            case 'Y':
                // Year, numeric, four digits
                tmp = val + min(4, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                if (tmp - val <= 2) {
                    int_value += int_value >= 70 ? 1900 : 2000;
                }
                _year = int_value;
                val = tmp;
                date_part_used = true;
                break;
            // Month
            case 'm':
            case 'c':
                tmp = val + min(2, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                _month = int_value;
                val = tmp;
                date_part_used = true;
                break;
            case 'M':
                int_value = check_word(s_month_name, val, val_end, &val);
                if (int_value < 0) {
                    return false;
                }
                _month = int_value;
                break;
            case 'b':
                int_value = check_word(s_ab_month_name, val, val_end, &val);
                if (int_value < 0) {
                    return false;
                }
                _month = int_value;
                break;
            // Day
            case 'd':
            case 'e':
                tmp = val + min(2, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                _day = int_value;
                val = tmp;
                date_part_used = true;
                break;
            case 'D':
                tmp = val + min(2, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                _day = int_value;
                val = tmp + min(2, val_end - tmp);
                date_part_used = true;
                break;
            // Hour
            case 'h':
            case 'I':
            case 'l':
                usa_time = true;
            // Fall through
            case 'k':
            case 'H':
                tmp = val + min(2, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                _hour = int_value;
                val = tmp;
                time_part_used = true;
                break;
            // Minute
            case 'i':
                tmp = val + min(2, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                _minute = int_value;
                val = tmp;
                time_part_used = true;
                break;
            // Second
            case 's':
            case 'S':
                tmp = val + min(2, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                _second = int_value;
                val = tmp;
                time_part_used = true;
                break;
            // Micro second
            case 'f':
                tmp = val + min(6, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                int_value *= log_10_int[6 - (tmp - val)];
                _microsecond = int_value;
                val = tmp;
                frac_part_used = true;
                break;
            // AM/PM
            case 'p':
                if ((val_end - val) < 2 || toupper(*(val + 1)) != 'M' || !usa_time) {
                    return false;
                }
                if (toupper(*val) == 'P') {
                    // PM
                    day_part = 12;
                }
                time_part_used = true;
                val += 2;
                break;
            // Weekday
            case 'W':
                int_value = check_word(s_day_name, val, val_end, &val);
                if (int_value < 0) {
                    return false;
                }
                int_value++;
                weekday = int_value;
                date_part_used = true;
                break;
            case 'a':
                int_value = check_word(s_ab_day_name, val, val_end, &val);
                if (int_value < 0) {
                    return false;
                }
                int_value++;
                weekday = int_value;
                date_part_used = true;
                break;
            case 'w':
                tmp = val + min(1, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                if (int_value >= 7) {
                    return false;
                }
                if (int_value == 0) {
                    int_value = 7;
                }
                weekday = int_value;
                val = tmp;
                date_part_used = true;
                break;
            case 'j':
                tmp = val + min(3, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                yearday = int_value;
                val = tmp;
                date_part_used = true;
                break;
            case 'u':
            case 'v':
            case 'U':
            case 'V':
                sunday_first = (*(ptr - 1) == 'U' || *(ptr - 1) == 'V');
                // Used to check if there is %x or %X
                strict_week_number = (*(ptr - 1) == 'V' || *(ptr - 1) == 'v');
                tmp = val + min(2, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                week_num = int_value;
                if (week_num > 53 || (strict_week_number && week_num == 0)) {
                    return false;
                }
                val = tmp;
                date_part_used = true;
                break;
            // strict week number, must be used with %V or %v
            case 'x':
            case 'X':
                strict_week_number_year_type = (*(ptr - 1) == 'X');
                tmp = val + min(4, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                strict_week_number_year = int_value;
                val = tmp;
                date_part_used = true;
                break;
            case 'r':
                if (from_date_format_str("%I:%i:%S %p", 11, val, val_end - val, &tmp)) {
                    return false;
                }
                val = tmp;
                time_part_used = true;
                break;
            case 'T':
                if (from_date_format_str("%H:%i:%S", 8, val, val_end - val, &tmp)) {
                    return false;
                }
                time_part_used = true;
                val = tmp;
                break;
            case '.':
                while (val < val_end && ispunct(*val)) {
                    val++;
                }
                break;
            case '@':
                while (val < val_end && isalpha(*val)) {
                    val++;
                }
                break;
            case '#':
                while (val < val_end && isdigit(*val)) {
                    val++;
                }
                break;
            case '%': // %%, escape the %
                if ('%' != *val) {
                    return false;
                }
                val++;
                break;
            default:
                return false;
            }
        } else if (!isspace(*ptr)) {
            if (*ptr != *val) {
                return false;
            }
            ptr++;
            val++;
        } else {
            ptr++;
        }
    }

    if (usa_time) {
        if (_hour > 12 || _hour < 1) {
            return false;
        }
        _hour = (_hour % 12) + day_part;
    }
    if (sub_val_end) {
        *sub_val_end = val;
        return false;
    }
    // Year day
    if (yearday > 0) {
        uint64_t days = calc_daynr(_year, 1, 1) + yearday - 1;
        if (!get_date_from_daynr(days)) {
            return false;
        }
    }
    // weekday
    if (week_num >= 0 && weekday > 0) {
        // Check
        if ((strict_week_number && (strict_week_number_year < 0 || strict_week_number_year_type != sunday_first)) ||
            (!strict_week_number && strict_week_number_year >= 0)) {
            return false;
        }
        uint64_t days = calc_daynr(strict_week_number ? strict_week_number_year : _year, 1, 1);

        uint8_t weekday_b = calc_weekday(days, sunday_first);

        if (sunday_first) {
            days += ((weekday_b == 0) ? 0 : 7) - weekday_b + (week_num - 1) * 7 + weekday % 7;
        } else {
            days += ((weekday_b <= 3) ? 0 : 7) - weekday_b + (week_num - 1) * 7 + weekday - 1;
        }
        if (!get_date_from_daynr(days)) {
            return false;
        }
    }

    // Compute timestamp type
    if (frac_part_used) {
        if (date_part_used) {
            _type = TIME_DATETIME;
        } else {
            _type = TIME_TIME;
        }
    } else {
        if (date_part_used) {
            if (time_part_used) {
                _type = TIME_DATETIME;
            } else {
                _type = TIME_DATE;
            }
        } else {
            _type = TIME_TIME;
        }
    }

    if (check_range() || check_date()) {
        return false;
    }
    _neg = false;
    return true;
}

bool DateTimeValue::date_add_interval(const TimeInterval& interval, TimeUnit unit) {
    int sign = interval.is_neg ? -1 : 1;
    switch (unit) {
    case MICROSECOND:
    case MILLISECOND:
    case SECOND:
    case MINUTE:
    case HOUR:
    case SECOND_MICROSECOND:
    case MINUTE_MICROSECOND:
    case MINUTE_SECOND:
    case HOUR_MICROSECOND:
    case HOUR_SECOND:
    case HOUR_MINUTE:
    case DAY_MICROSECOND:
    case DAY_SECOND:
    case DAY_MINUTE:
    case DAY_HOUR: {
        // This may change the day information
        int64_t microseconds = _microsecond + sign * interval.microsecond;
        int64_t extra_second = microseconds / 1000000L;
        microseconds %= 1000000L;

        int64_t seconds =
                (_day - 1) * 86400L + _hour * 3600L + _minute * 60 + _second +
                sign * (interval.day * 86400 + interval.hour * 3600 + interval.minute * 60 + interval.second) +
                extra_second;
        if (microseconds < 0) {
            seconds--;
            microseconds += 1000000L;
        }
        int64_t days = seconds / 86400;
        seconds %= 86400L;
        if (seconds < 0) {
            seconds += 86400L;
            days--;
        }
        _microsecond = microseconds;
        _second = seconds % 60;
        _minute = (seconds / 60) % 60;
        _hour = seconds / 3600;
        int64_t day_nr = calc_daynr(_year, _month, 1) + days;
        if (!get_date_from_daynr(day_nr)) {
            return false;
        }
        _type = TIME_DATETIME;
        break;
    }
    case DAY:
    case WEEK: {
        // This only change day information, not change second information
        int64_t day_nr = daynr() + interval.day * sign;
        if (!get_date_from_daynr(day_nr)) {
            return false;
        }
        break;
    }
    case YEAR: {
        // This only change year information
        _year += sign * interval.year;
        if (_year > 9999) {
            return false;
        }
        if (_month == 2 && _day == 29 && !is_leap(_year)) {
            _day = 28;
        }
        break;
    }
    case MONTH:
    case QUARTER:
    case YEAR_MONTH: {
        // This will change month and year information, maybe date.
        int64_t months = _year * 12 + _month - 1 + sign * (12 * interval.year + interval.month);
        _year = months / 12;
        if (_year > 9999) {
            return false;
        }
        _month = (months % 12) + 1;
        if (_day > s_days_in_month[_month]) {
            _day = s_days_in_month[_month];
            if (_month == 2 && is_leap(_year)) {
                _day++;
            }
        }
        break;
    }
    }
    return true;
}

bool DateTimeValue::unix_timestamp(int64_t* timestamp, std::string_view timezone) const {
    cctz::time_zone ctz;
    if (!TimezoneUtils::find_cctz_time_zone(timezone, ctz)) {
        return false;
    }
    return unix_timestamp(timestamp, ctz);
}

bool DateTimeValue::unix_timestamp(int64_t* timestamp, const cctz::time_zone& ctz) const {
    const auto tp = cctz::convert(cctz::civil_second(_year, _month, _day, _hour, _minute, _second), ctz);
    *timestamp = tp.time_since_epoch().count();
    return true;
}

bool DateTimeValue::from_cctz_timezone(const TimezoneHsScan& timezone_hsscan, std::string_view timezone,
                                       cctz::time_zone& ctz) {
    return TimezoneUtils::find_cctz_time_zone(timezone_hsscan, timezone, ctz);
}

bool DateTimeValue::from_unixtime(int64_t timestamp, const std::string& timezone) {
    cctz::time_zone ctz;
    if (!TimezoneUtils::find_cctz_time_zone(timezone, ctz)) {
        return false;
    }
    return from_unixtime(timestamp, ctz);
}

bool DateTimeValue::from_unixtime(int64_t timestamp, const cctz::time_zone& ctz) {
    return from_unixtime(timestamp, 0, ctz);
}

bool DateTimeValue::from_unixtime(int64_t timestamp, int64_t microsecond, const cctz::time_zone& ctz) {
    static const cctz::time_point<cctz::sys_seconds> epoch =
            std::chrono::time_point_cast<cctz::sys_seconds>(std::chrono::system_clock::from_time_t(0));
    cctz::time_point<cctz::sys_seconds> t = epoch + cctz::seconds(timestamp);

    const auto tp = cctz::convert(t, ctz);

    _neg = 0;
    _type = TIME_DATETIME;
    _year = tp.year();
    _month = tp.month();
    _day = tp.day();
    _hour = tp.hour();
    _minute = tp.minute();
    _second = tp.second();
    _microsecond = microsecond;

    return true;
}

const char* DateTimeValue::month_name() const {
    if (_month < 1 || _month > 12) {
        return nullptr;
    }
    return s_month_name[_month];
}

const char* DateTimeValue::day_name() const {
    int day = weekday();
    if (day < 0 || day >= 7) {
        return nullptr;
    }
    return s_day_name[day];
}

DateTimeValue DateTimeValue::local_time() {
    DateTimeValue value;
    value.from_unixtime(time(nullptr), TimezoneUtils::local_time_zone());
    return value;
}

std::ostream& operator<<(std::ostream& os, const DateTimeValue& value) {
    char buf[64];
    value.to_string(buf);
    return os << buf;
}

// NOTE:
//  only support DATE - DATE (no support DATETIME - DATETIME)
std::size_t operator-(const DateTimeValue& v1, const DateTimeValue& v2) {
    return v1.daynr() - v2.daynr();
}

std::size_t hash_value(DateTimeValue const& value) {
    return HashUtil::hash(&value, sizeof(DateTimeValue), 0);
}

// NOTE: This is only a subset of teradata date format and is compatible with Presto.
// see: https://github.com/trinodb/docs.trino.io/blob/master/318/_sources/functions/teradata.rst.txt
// see: https://docs.teradata.com/r/SQL-Data-Types-and-Literals/July-2021/Data-Type-Conversion-Functions/TO_DATE/Argument-Types-and-Rules/format_arg-Format-Elements
// - / , . ; :	Punctuation characters are ignored
// dd	Day of month (1-31)
// hh	Hour of day (1-12)
// hh24	Hour of the day (0-23)
// mi	Minute (0-59)
// mm	Month (01-12)
// ss	Second (0-59)
// yyyy	4-digit year
// yy	2-digit year
enum TeradataFormatChar : char {
    T1 = '-',   // Punctuation characters are ignored
    T2 = '/',   // Punctuation characters are ignored
    T3 = ',',   // Punctuation characters are ignored
    T4 = '.',   // Punctuation characters are ignored
    T5 = ';',   // Punctuation characters are ignored
    T6 = ':',   // Punctuation characters are ignored
    T7 = ' ',   // Punctuation characters are ignored
    T8 = '\r',  // Punctuation characters are ignored
    T9 = '\n',  // Punctuation characters are ignored
    T10 = '\t', // Punctuation characters are ignored
    D = 'd',    // Day of month (1-31)
    H = 'h',    // Hour of day (1-12), , // Hour of the day (0-23)
    M = 'm',    // Minute (0-59)
    I = 'i',    // Month (01-12)
    S = 's',    // Second (0-59)
    Y = 'y',    // 2-digit year
    A = 'a',    // am
    P = 'p',    // pm
    UNRECOGNIZED
};

bool TeradataFormat::prepare(std::string_view format) {
    const char* ptr = format.data();
    const char* end = format.data() + format.length();
    while (ptr < end) {
        const char* next_ch_ptr = ptr;
        uint32_t repeat_count = 0;
        for (char ch = *ptr; ch == *next_ch_ptr && next_ch_ptr < end; ++next_ch_ptr) {
            ++repeat_count;
        }

        switch (*ptr) {
        case TeradataFormatChar::T1:
        case TeradataFormatChar::T2:
        case TeradataFormatChar::T3:
        case TeradataFormatChar::T4:
        case TeradataFormatChar::T5:
        case TeradataFormatChar::T6:
        case TeradataFormatChar::T7:
        case TeradataFormatChar::T8:
        case TeradataFormatChar::T9:
        case TeradataFormatChar::T10: {
            //  - / , . ; :	Punctuation characters are ignored
            auto ignored_char = *ptr;
            _token_parsers.emplace_back([&, ignored_char]() {
                if (*val != ignored_char) {
                    return false;
                }
                val++;
                return true;
            });
            break;
        }
        case TeradataFormatChar::D: {
            // dd
            if (repeat_count != 2) {
                return false;
            }
            _token_parsers.emplace_back([&]() {
                int64_t int_value = 0;
                const char* tmp = val + std::min<int>(2, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                _day = int_value;
                val = tmp;
                return true;
            });
            break;
        }
        case TeradataFormatChar::H: {
            if (repeat_count != 2) {
                return false;
            }

            if (next_ch_ptr != end && *next_ch_ptr == '2') {
                // hh24
                ptr += 2;
                ++next_ch_ptr;
                if (next_ch_ptr == end || *next_ch_ptr != '4') {
                    return false;
                }
                ++next_ch_ptr;
            }
            _token_parsers.emplace_back([&]() {
                int64_t int_value = 0;
                const char* tmp = val + std::min<int>(2, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                _hour = int_value;
                val = tmp;
                return true;
            });
            break;
        }
        case TeradataFormatChar::A: {
            // am
            if (repeat_count != 1) {
                return false;
            }
            if (next_ch_ptr == end || *next_ch_ptr != 'm') {
                return false;
            }
            next_ch_ptr++;
            ptr++;
            _token_parsers.emplace_back([&]() {
                if (*val != 'a') {
                    return false;
                }
                val++;
                if (*val != 'm') {
                    return false;
                }
                val++;
                return true;
            });
            break;
        }
        case TeradataFormatChar::P: {
            // pm
            if (repeat_count != 1) {
                return false;
            }
            if (next_ch_ptr == end || *next_ch_ptr != 'm') {
                return false;
            }
            next_ch_ptr++;
            ptr++;
            _token_parsers.emplace_back([&]() {
                if (*val != 'p') {
                    return false;
                }
                val++;
                if (*val != 'm') {
                    return false;
                }
                val++;
                _hour += 12;
                return true;
            });
            break;
        }
        case TeradataFormatChar::M: {
            if (repeat_count == 2) {
                // mm
                _token_parsers.emplace_back([&]() {
                    int64_t int_value = 0;
                    const char* tmp = val + std::min<int>(2, val_end - val);
                    if (!str_to_int64(val, &tmp, &int_value)) {
                        return false;
                    }
                    _month = int_value;
                    val = tmp;
                    return true;
                });
            } else if (repeat_count == 1) {
                if (next_ch_ptr == end || *next_ch_ptr != TeradataFormatChar::I) {
                    return false;
                }
                next_ch_ptr++;
                ptr++;

                // mi
                _token_parsers.emplace_back([&]() {
                    int64_t int_value = 0;
                    const char* tmp = val + std::min<int>(2, val_end - val);
                    ;
                    if (!str_to_int64(val, &tmp, &int_value)) {
                        return false;
                    }
                    _minute = int_value;
                    val = tmp;
                    return true;
                });
            } else {
                return false;
            }
            break;
        }
        case TeradataFormatChar::S: {
            if (repeat_count != 2) {
                return false;
            }
            // ss
            _token_parsers.emplace_back([&]() {
                int64_t int_value = 0;
                const char* tmp = val + std::min<int>(2, val_end - val);
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                _second = int_value;
                val = tmp;
                return true;
            });
            break;
        }
        case TeradataFormatChar::Y: {
            // yy
            if (repeat_count == 2) {
                _token_parsers.emplace_back([&]() {
                    int64_t int_value = 0;
                    const char* tmp = val + std::min<int>(2, val_end - val);
                    ;
                    if (!str_to_int64(val, &tmp, &int_value)) {
                        return false;
                    }
                    int_value += int_value >= 70 ? 1900 : 2000;
                    _year = int_value;
                    val = tmp;
                    return true;
                });
            } else if (repeat_count == 4) {
                // yyyy
                _token_parsers.emplace_back([&]() {
                    int64_t int_value = 0;
                    const char* tmp = val + std::min<int>(4, val_end - val);
                    ;
                    if (!str_to_int64(val, &tmp, &int_value)) {
                        return false;
                    }
                    _year = int_value;
                    val = tmp;
                    return true;
                });
            } else {
                return false;
            }
            break;
        }
        default: {
            return false;
        }
        }
        ptr += repeat_count;
    }

    return true;
}

bool TeradataFormat::parse(std::string_view str, DateTimeValue* output) {
    val = str.data();
    val_end = str.data() + str.length();

    for (auto& p : _token_parsers) {
        if (!p()) {
            return false;
        }
    }
    *output = DateTimeValue(type(), year(), month(), day(), hour(), minute(), second(), microsecond());
    return true;
}

} // namespace starrocks
