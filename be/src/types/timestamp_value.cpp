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

#include "types/timestamp_value.h"

#include "runtime/time_types.h"
#include "util/timezone_utils.h"

namespace starrocks {
TimestampValue TimestampValue::MAX_TIMESTAMP_VALUE{timestamp::MAX_TIMESTAMP};
TimestampValue TimestampValue::MIN_TIMESTAMP_VALUE{timestamp::MIN_TIMESTAMP};

static constexpr int s_days_in_month[13] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
static int month_to_quarter[13] = {0, 1, 1, 1, 4, 4, 4, 7, 7, 7, 10, 10, 10};

static const char* s_month_name[] = {"",     "January", "February",  "March",   "April",    "May",      "June",
                                     "July", "August",  "September", "October", "November", "December", nullptr};
static const char* s_ab_month_name[] = {"",    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", nullptr};
static const char* s_day_name[] = {"Monday", "Tuesday",  "Wednesday", "Thursday",
                                   "Friday", "Saturday", "Sunday",    nullptr};
static const char* s_ab_day_name[] = {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun", nullptr};

TimestampValue TimestampValue::create_from_unixtime(int64_t ts, const cctz::time_zone& ctz) {
    TimestampValue value;
    value.from_unixtime(ts, ctz);
    return value;
}

bool TimestampValue::from_timestamp_literal(uint64_t timestamp) {
    uint64_t date = timestamp / 1000000;
    uint64_t time = timestamp % 1000000;

    int hour = time / 10000;
    time %= 10000;

    int minute = time / 100;
    int second = time % 100;
    int usec = 0;

    JulianDate julian = date::from_date_literal(date);
    Timestamp ts = timestamp::from_time(hour, minute, second, usec);
    _timestamp = timestamp::from_julian_and_time(julian, ts);
    return true;
}

bool TimestampValue::from_timestamp_literal_with_check(uint64_t timestamp) {
    timestamp = date::standardize_date(timestamp);
    if (timestamp <= 0) {
        return false;
    }

    uint64_t date = timestamp / 1000000;
    uint64_t time = timestamp % 1000000;

    int year = date / 10000;
    int month = (date / 100) % 100;
    int day = date % 100;

    int hour = time / 10000;
    time %= 10000;

    int minute = time / 100;
    int second = time % 100;
    int usec = 0;

    if (!timestamp::check(year, month, day, hour, minute, second, usec)) {
        return false;
    }

    JulianDate julian = date::from_date_literal(date);
    Timestamp ts = timestamp::from_time(hour, minute, second, usec);
    _timestamp = timestamp::from_julian_and_time(julian, ts);
    return true;
}

uint64_t TimestampValue::to_timestamp_literal() const {
    int y, m, d, h, min, s, u;
    to_timestamp(&y, &m, &d, &h, &min, &s, &u);
    uint64_t date_val = y * 10000lu + m * 100 + d;
    uint64_t time_val = h * 10000lu + min * 100 + s;
    return date_val * 1000000 + time_val;
}

int64_t TimestampValue::diff_microsecond(TimestampValue other) const {
    return (timestamp::to_julian(_timestamp) - timestamp::to_julian(other._timestamp)) * USECS_PER_DAY +
           (timestamp::to_time(_timestamp) - timestamp::to_time(other._timestamp));
}

bool TimestampValue::from_string(const char* date_str, size_t len) {
    int year, month, day, hour, minute, second, microsecond;
    if (!date::from_string_to_datetime(date_str, len, &year, &month, &day, &hour, &minute, &second, &microsecond)) {
        return false;
    }

    if (!timestamp::check(year, month, day, hour, minute, second, microsecond)) {
        return false;
    }

    from_timestamp(year, month, day, hour, minute, second, microsecond);
    return true;
}

// process string content based on format like "%Y-%m-%d". '-' means any char.
// string content must match digit parts and chats parts.
bool TimestampValue::from_date_format_str(const char* value, int value_len, const char* str_format) {
    if (value_len != 10 || value[4] != str_format[2] || value[7] != str_format[5]) {
        return false;
    }

    uint8_t year1;
    uint8_t year2;
    uint8_t year3;
    uint8_t year4;
    uint8_t month1;
    uint8_t month2;
    uint8_t day1;
    uint8_t day2;
    if (date::char_to_digit(value, 0, &year1) || date::char_to_digit(value, 1, &year2) ||
        date::char_to_digit(value, 2, &year3) || date::char_to_digit(value, 3, &year4) ||
        date::char_to_digit(value, 5, &month1) || date::char_to_digit(value, 6, &month2) ||
        date::char_to_digit(value, 8, &day1) || date::char_to_digit(value, 9, &day2)) {
        return false;
    }

    uint16_t year = year1 * 1000 + year2 * 100 + year3 * 10 + year4;
    uint8_t month = month1 * 10 + month2;
    uint8_t day = day1 * 10 + day2;

    if (month > 12 || (day > s_days_in_month[month] && (month != 2 || day != 29 || !date::is_leap(year)))) {
        return false;
    }

    _timestamp = timestamp::from_datetime(year, month, day, 0, 0, 0, 0);
    return true;
}

// process string content based on format like "%Y-%m-%d %H:%i:%s". '-'/':' means any char.
// string content must match digit parts and chats parts.
bool TimestampValue::from_datetime_format_str(const char* value, int value_len, const char* str_format) {
    if (value_len != 19 || value[4] != str_format[2] || value[7] != str_format[5] || value[10] != str_format[8] ||
        value[13] != str_format[11] || value[16] != str_format[14]) {
        return false;
    }

    uint8_t year1;
    uint8_t year2;
    uint8_t year3;
    uint8_t year4;
    uint8_t month1;
    uint8_t month2;
    uint8_t day1;
    uint8_t day2;

    uint8_t hour1;
    uint8_t hour2;
    uint8_t minute1;
    uint8_t minute2;
    uint8_t second1;
    uint8_t second2;
    if (date::char_to_digit(value, 0, &year1) || date::char_to_digit(value, 1, &year2) ||
        date::char_to_digit(value, 2, &year3) || date::char_to_digit(value, 3, &year4) ||
        date::char_to_digit(value, 5, &month1) || date::char_to_digit(value, 6, &month2) ||
        date::char_to_digit(value, 8, &day1) || date::char_to_digit(value, 9, &day2) ||
        date::char_to_digit(value, 11, &hour1) || date::char_to_digit(value, 12, &hour2) ||
        date::char_to_digit(value, 14, &minute1) || date::char_to_digit(value, 15, &minute2) ||
        date::char_to_digit(value, 17, &second1) || date::char_to_digit(value, 18, &second2)) {
        return false;
    }

    uint16_t year = year1 * 1000 + year2 * 100 + year3 * 10 + year4;
    uint8_t month = month1 * 10 + month2;
    uint8_t day = day1 * 10 + day2;
    uint16_t hour = hour1 * 10 + hour2;
    uint8_t minute = minute1 * 10 + minute2;
    uint8_t second = second1 * 10 + second2;

    if (month > 12 || (day > s_days_in_month[month] && (month != 2 || day != 29 || !date::is_leap(year))) ||
        hour > 23 || minute > 59 || second > 59) {
        return false;
    }

    _timestamp = timestamp::from_datetime(year, month, day, hour, minute, second, 0);
    return true;
}

// This codes is taken from DateTimeValue
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

// This codes is taken from DateTimeValue
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

// This codes is taken from DateTimeValue
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

// This codes is taken from DateTimeValue
const uint64_t log_10_int[] = {1,         10,         100,         1000,         10000UL,       100000UL,
                               1000000UL, 10000000UL, 100000000UL, 1000000000UL, 10000000000UL, 100000000000UL};

// This codes is taken from DateTimeValue
// Following code is stolen from MySQL.
uint64_t TimestampValue::calc_daynr(uint32_t year, uint32_t month, uint32_t day) {
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

// This codes is taken from DateTimeValue
uint8_t TimestampValue::calc_weekday(uint64_t day_nr, bool is_sunday_first_day) {
    return (day_nr + 5L + (is_sunday_first_day ? 1L : 0L)) % 7;
}

// This codes is taken from DateTimeValue
bool TimestampValue::get_date_from_daynr(uint64_t daynr, DatetimeContent* content) {
    if (daynr <= 0 || daynr > DATE_MAX_DAYNR) {
        return false;
    }
    content->_year = daynr / 365;
    uint32_t days_befor_year = 0;
    while (daynr < (days_befor_year = calc_daynr(content->_year, 1, 1))) {
        (content->_year)--;
    }
    uint32_t days_of_year = daynr - days_befor_year + 1;
    int leap_day = 0;
    if (date::is_leap(content->_year)) {
        if (days_of_year > 31 + 28) {
            days_of_year--;
            if (days_of_year == 31 + 28) {
                leap_day = 1;
            }
        }
    }
    content->_month = 1;
    while (days_of_year > s_days_in_month[content->_month]) {
        days_of_year -= s_days_in_month[content->_month];
        (content->_month)++;
    }
    content->_day = days_of_year + leap_day;
    return true;
}

// This codes is taken from DateTimeValue
bool TimestampValue::check_range(const DatetimeContent* content) const {
    return content->_year > 9999 || content->_month > 12 || content->_day > 31 ||
           content->_hour > (content->_type == TIMESTAMP_TIME ? TIME_MAX_HOUR : 23) || content->_minute > 59 ||
           content->_second > 59 || content->_microsecond > 999999;
}

// This codes is taken from DateTimeValue
bool TimestampValue::check_date(const DatetimeContent* content) const {
    if (content->_month == 0 || content->_day == 0) {
        return true;
    }
    if (content->_day > s_days_in_month[content->_month]) {
        // Feb 29 in leap year is valid.
        if (content->_month == 2 && content->_day == 29 && date::is_leap(content->_year)) {
            return false;
        }
        return true;
    }
    return false;
}

// ============================
// This codes and associative methods is from DateTimeValue.
// Uncommon approach to process string content based on format string
bool TimestampValue::from_uncommon_format_str(const char* format, int format_len, const char* value, int value_len) {
    DatetimeContent content;
    memset(&content, 0, sizeof(DatetimeContent));
    bool result = from_uncommon_format_str(format, format_len, value, value_len, &content, nullptr);
    if (result) {
        _timestamp = timestamp::from_datetime(content._year, content._month, content._day, content._hour,
                                              content._minute, content._second, 0);
    }
    return result;
}

// This codes is taken from DateTimeValue.
// It compute datetime Based on any possible format string.
bool TimestampValue::from_uncommon_format_str(const char* format, int format_len, const char* value, int value_len,
                                              DatetimeContent* content, const char** sub_val_end) {
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
                tmp = val + std::min(2, (int)(val_end - val));
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                int_value += int_value >= 70 ? 1900 : 2000;
                content->_year = int_value;
                val = tmp;
                date_part_used = true;
                break;
            case 'Y':
                // Year, numeric, four digits
                tmp = val + std::min(4, (int)(val_end - val));
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                if (tmp - val <= 2) {
                    int_value += int_value >= 70 ? 1900 : 2000;
                }
                content->_year = int_value;
                val = tmp;
                date_part_used = true;
                break;
                // Month
            case 'm':
            case 'c':
                tmp = val + std::min(2, (int)(val_end - val));
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                content->_month = int_value;
                val = tmp;
                date_part_used = true;
                break;
            case 'M':
                int_value = check_word(s_month_name, val, val_end, &val);
                if (int_value < 0) {
                    return false;
                }
                content->_month = int_value;
                break;
            case 'b':
                int_value = check_word(s_ab_month_name, val, val_end, &val);
                if (int_value < 0) {
                    return false;
                }
                content->_month = int_value;
                break;
                // Day
            case 'd':
            case 'e':
                tmp = val + std::min(2, (int)(val_end - val));
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                content->_day = int_value;
                val = tmp;
                date_part_used = true;
                break;
            case 'D':
                tmp = val + std::min(2, (int)(val_end - val));
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                content->_day = int_value;
                val = tmp + std::min(2, (int)(val_end - tmp));
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
                tmp = val + std::min(2, (int)(val_end - val));
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                content->_hour = int_value;
                val = tmp;
                time_part_used = true;
                break;
                // Minute
            case 'i':
                tmp = val + std::min(2, (int)(val_end - val));
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                content->_minute = int_value;
                val = tmp;
                time_part_used = true;
                break;
                // Second
            case 's':
            case 'S':
                tmp = val + std::min(2, (int)(val_end - val));
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                content->_second = int_value;
                val = tmp;
                time_part_used = true;
                break;
                // Micro second
            case 'f':
                tmp = val + std::min(6, (int)(val_end - val));
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                int_value *= log_10_int[6 - (tmp - val)];
                content->_microsecond = int_value;
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
                tmp = val + std::min(1, (int)(val_end - val));
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
                tmp = val + std::min(3, (int)(val_end - val));
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
                tmp = val + std::min(2, (int)(val_end - val));
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
                tmp = val + std::min(4, (int)(val_end - val));
                if (!str_to_int64(val, &tmp, &int_value)) {
                    return false;
                }
                strict_week_number_year = int_value;
                val = tmp;
                date_part_used = true;
                break;
            case 'r':
                if (from_uncommon_format_str("%I:%i:%S %p", 11, val, val_end - val, content, &tmp)) {
                    return false;
                }
                val = tmp;
                time_part_used = true;
                break;
            case 'T':
                if (from_uncommon_format_str("%H:%i:%S", 8, val, val_end - val, content, &tmp)) {
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
        if (content->_hour > 12 || content->_hour < 1) {
            return false;
        }
        content->_hour = (content->_hour % 12) + day_part;
    }
    if (sub_val_end) {
        *sub_val_end = val;
        return false;
    }
    // Year day
    if (yearday > 0) {
        uint64_t days = calc_daynr(content->_year, 1, 1) + yearday - 1;
        if (!get_date_from_daynr(days, content)) {
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
        uint64_t days = calc_daynr(strict_week_number ? strict_week_number_year : content->_year, 1, 1);

        uint8_t weekday_b = calc_weekday(days, sunday_first);

        if (sunday_first) {
            days += ((weekday_b == 0) ? 0 : 7) - weekday_b + (week_num - 1) * 7 + weekday % 7;
        } else {
            days += ((weekday_b <= 3) ? 0 : 7) - weekday_b + (week_num - 1) * 7 + weekday - 1;
        }
        if (!get_date_from_daynr(days, content)) {
            return false;
        }
    }

    // Compute timestamp type
    if (frac_part_used) {
        if (date_part_used) {
            content->_type = TIMESTAMP_DATETIME;
        } else {
            content->_type = TIMESTAMP_TIME;
        }
    } else {
        if (date_part_used) {
            if (time_part_used) {
                content->_type = TIMESTAMP_DATETIME;
            } else {
                content->_type = TIMESTAMP_DATE;
            }
        } else {
            content->_type = TIMESTAMP_TIME;
        }
    }

    if (check_range(content) || check_date(content)) {
        return false;
    }
    content->_neg = false;
    return true;
}

void TimestampValue::from_timestamp(int year, int month, int day, int hour, int minute, int second, int usec) {
    _timestamp = timestamp::from_datetime(year, month, day, hour, minute, second, usec);
}

void TimestampValue::to_timestamp(int* year, int* month, int* day, int* hour, int* minute, int* second,
                                  int* usec) const {
    timestamp::to_datetime(_timestamp, year, month, day, hour, minute, second, usec);
}

void TimestampValue::trunc_to_millisecond() {
    Timestamp time = _timestamp & TIMESTAMP_BITS_TIME;
    uint64_t microseconds = time % USECS_PER_MILLIS;
    _timestamp -= microseconds;
}

void TimestampValue::trunc_to_second() {
    Timestamp time = _timestamp & TIMESTAMP_BITS_TIME;
    uint64_t microseconds = time % USECS_PER_SEC;
    _timestamp -= microseconds;
}

void TimestampValue::trunc_to_minute() {
    Timestamp time = _timestamp & TIMESTAMP_BITS_TIME;
    uint64_t microseconds = time % USECS_PER_MINUTE;
    _timestamp -= microseconds;
}

void TimestampValue::trunc_to_hour() {
    Timestamp time = _timestamp & TIMESTAMP_BITS_TIME;
    uint64_t microseconds = time % USECS_PER_HOUR;
    _timestamp -= microseconds;
}

void TimestampValue::trunc_to_day() {
    _timestamp &= (~TIMESTAMP_BITS_TIME);
}

void TimestampValue::trunc_to_month() {
    int year, month, day;
    date::to_date_with_cache(timestamp::to_julian(_timestamp), &year, &month, &day);
    _timestamp = timestamp::from_datetime(year, month, 1, 0, 0, 0, 0);
}

void TimestampValue::trunc_to_year() {
    int year, month, day;
    date::to_date_with_cache(timestamp::to_julian(_timestamp), &year, &month, &day);
    _timestamp = timestamp::from_datetime(year, 1, 1, 0, 0, 0, 0);
}

void TimestampValue::trunc_to_week(int days) {
    int year, month, day;
    date::to_date_with_cache(timestamp::to_julian(_timestamp) + days, &year, &month, &day);
    _timestamp = timestamp::from_datetime(year, month, day, 0, 0, 0, 0);
}

void TimestampValue::trunc_to_quarter() {
    int year, month, day;
    date::to_date_with_cache(timestamp::to_julian(_timestamp), &year, &month, &day);
    _timestamp = timestamp::from_datetime(year, month_to_quarter[month], 1, 0, 0, 0, 0);
}

int64_t TimestampValue::to_unix_second() const {
    int64_t result = timestamp::to_julian(_timestamp);
    result *= SECS_PER_DAY;
    result += timestamp::to_time(_timestamp) / USECS_PER_SEC;
    result -= timestamp::UNIX_EPOCH_SECONDS;
    return result;
}

bool TimestampValue::from_unixtime(int64_t second, const std::string& timezone) {
    cctz::time_zone ctz;
    if (!TimezoneUtils::find_cctz_time_zone(timezone, ctz)) {
        return false;
    }
    from_unixtime(second, ctz);
    return true;
}

void TimestampValue::from_unixtime(int64_t second, const cctz::time_zone& ctz) {
    static const cctz::time_point<cctz::sys_seconds> epoch =
            std::chrono::time_point_cast<cctz::sys_seconds>(std::chrono::system_clock::from_time_t(0));
    cctz::time_point<cctz::sys_seconds> t = epoch + cctz::seconds(second);

    const auto tp = cctz::convert(t, ctz);
    from_timestamp(tp.year(), tp.month(), tp.day(), tp.hour(), tp.minute(), tp.second(), 0);
}

void TimestampValue::from_unixtime(int64_t second, int64_t microsecond, const cctz::time_zone& ctz) {
    from_unixtime(second, ctz);
    _timestamp += microsecond;
    return;
}

void TimestampValue::from_unix_second(int64_t second, int64_t microsecond) {
    second += timestamp::UNIX_EPOCH_SECONDS;
    JulianDate day = second / SECS_PER_DAY;
    Timestamp s = second % SECS_PER_DAY;
    _timestamp = timestamp::from_julian_and_time(day, s * USECS_PER_SEC + microsecond);
}

bool TimestampValue::is_valid() const {
    return (_timestamp >= timestamp::MIN_TIMESTAMP) & (_timestamp <= timestamp::MAX_TIMESTAMP);
}

bool TimestampValue::is_valid_non_strict() const {
    return is_valid();
}

std::string TimestampValue::to_string() const {
    return timestamp::to_string(_timestamp);
}

int TimestampValue::to_string(char* s, size_t n) const {
    return timestamp::to_string(_timestamp, s, n);
}

} // namespace starrocks
