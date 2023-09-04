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

#include "runtime/time_types.h"

#include <string>

#include "gutil/strings/substitute.h"
#include "util/raw_container.h"

namespace starrocks {

// two-digit years < this are 20..; >= this are 19..
const int YY_PART_YEAR = 70;

const uint64_t LOG_10_INT[] = {1,         10,         100,         1000,         10000UL,       100000UL,
                               1000000UL, 10000000UL, 100000000UL, 1000000000UL, 10000000000UL, 100000000000UL};

// Date Cache
JulianToDateEntry g_julian_to_date_cache[CACHE_JULIAN_DAYS];

static const uint32_t CACHE_DATE_LITERAL_START = 19900101;
static const uint32_t CACHE_DATE_LITERAL_END = 20250101;
static JulianDate g_date_literal_to_julian_cache[CACHE_DATE_LITERAL_END - CACHE_DATE_LITERAL_START];

// MySQL DATE CACHE
static const int CACHE_DATE_LOGIC_START = (1990 << 9) | (1 << 5) | (1);
static const int CACHE_DATE_LOGIC_END = (2025 << 9) | (1 << 5) | (1);
static JulianDate g_mysql_date_to_julian_cache[CACHE_DATE_LOGIC_END - CACHE_DATE_LOGIC_START];

void date::init_date_cache() {
    // julian date to date cache
    for (int i = 0; i < CACHE_JULIAN_DAYS; ++i) {
        int year, month, day;
        to_date(date::UNIX_EPOCH_JULIAN + i, &year, &month, &day);

        g_julian_to_date_cache[i].year = year;
        g_julian_to_date_cache[i].month = month;
        g_julian_to_date_cache[i].day = day;
        g_julian_to_date_cache[i].week_th_of_year = get_week_of_year(date::UNIX_EPOCH_JULIAN + i);
    }

    // date to julian date cache
    for (int j = CACHE_DATE_LITERAL_START; j < CACHE_DATE_LITERAL_END; ++j) {
        int year = j / 10000;
        int month = (j / 100) % 100;
        int day = j % 100;

        g_date_literal_to_julian_cache[j - CACHE_DATE_LITERAL_START] = date::from_date(year, month, day);
    }

    // date mysql to julian date cache
    for (int j = CACHE_DATE_LOGIC_START; j < CACHE_DATE_LOGIC_END; ++j) {
        int year = j >> 9;
        int month = (j >> 5) & 15;
        int day = j & 31;

        g_mysql_date_to_julian_cache[j - CACHE_DATE_LOGIC_START] = date::from_date(year, month, day);
    }
}

int date::get_week_of_year(JulianDate julian) {
    // day
    auto julian_day = julian;
    int year, month, day1;
    date::to_date(julian_day, &year, &month, &day1);

    /* current day */
    auto julian_day4 = date::from_date(year, 1, 4);
    auto day0 = date::get_days_after_monday(julian_day4);

    if (julian_day < julian_day4 - day0) {
        julian_day4 = date::from_date(year - 1, 1, 4);
        day0 = date::get_days_after_monday(julian_day4);
    }

    int result = (julian_day - (julian_day4 - day0)) / 7 + 1;

    if (result >= 52) {
        julian_day4 = date::from_date(year + 1, 1, 4);
        day0 = date::get_days_after_monday(julian_day4);
        if (julian_day >= julian_day4 - day0) {
            result = (julian_day - (julian_day4 - day0)) / 7 + 1;
        }
    }

    return result;
}

int date::get_days_after_monday(JulianDate julian) {
    int day = (julian + 1) % 7;
    // Monday: 1
    if (day > 0) {
        return day - 1;
    } else {
        // Because It's Sunday.
        return 6;
    }
}

bool date::get_weeks_of_year_with_cache(JulianDate julian, int* weeks) {
    if (julian >= date::UNIX_EPOCH_JULIAN && julian < date::UNIX_EPOCH_JULIAN + CACHE_JULIAN_DAYS) {
        *weeks = g_julian_to_date_cache[julian - date::UNIX_EPOCH_JULIAN].week_th_of_year;
        return true;
    }
    return false;
}

int64_t date::standardize_date(int64_t value) {
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

bool date::from_string(const char* date_str, size_t len, int* year, int* month, int* day, int* hour, int* minute,
                       int* second, int* microsecond) {
    const char* ptr = date_str;
    const char* end = date_str + len;
    // ONLY 2, 6 can follow by a sapce
    const static int allow_space_mask = 4 | 64;
    const static int MAX_DATE_PARTS = 8;
    uint32_t date_val[MAX_DATE_PARTS];
    int32_t date_len[MAX_DATE_PARTS];

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
    if (num_field < 3) {
        return false;
    }
    if (!is_interval_format) {
        year_len = date_len[0];
    }
    for (; field_idx < MAX_DATE_PARTS; ++field_idx) {
        date_len[field_idx] = 0;
        date_val[field_idx] = 0;
    }

    *year = date_val[0];
    *month = date_val[1];
    *day = date_val[2];
    *hour = date_val[3];
    *minute = date_val[4];
    *second = date_val[5];
    *microsecond = date_val[6];

    if (*microsecond && date_len[6] < 6) {
        (*microsecond) *= LOG_10_INT[6 - date_len[6]];
    }
    if (year_len == 2) {
        const int YY_PART_YEAR = 70;
        if ((*year) < YY_PART_YEAR) {
            (*year) += 2000;
        } else {
            (*year) += 1900;
        }
    }

    return true;
}

// Get date base on format "%Y-%m-%d", '-' means any char.
// compare every char.
bool date::from_string_to_date_internal(const char* ptr, int* year, int* month, int* day) {
    uint8_t year1;
    uint8_t year2;
    uint8_t year3;
    uint8_t year4;
    uint8_t month1;
    uint8_t month2;
    uint8_t day1;
    uint8_t day2;
    if (char_to_digit(ptr, 0, &year1) || char_to_digit(ptr, 1, &year2) || char_to_digit(ptr, 2, &year3) ||
        char_to_digit(ptr, 3, &year4) || isdigit(ptr[4]) || char_to_digit(ptr, 5, &month1) ||
        char_to_digit(ptr, 6, &month2) || isdigit(ptr[7]) || char_to_digit(ptr, 8, &day1) ||
        char_to_digit(ptr, 9, &day2)) {
        return false;
    }

    *year = year1 * 1000 + year2 * 100 + year3 * 10 + year4;
    *month = month1 * 10 + month2;
    *day = day1 * 10 + day2;

    if (*month > 12 || (*day > DAYS_IN_MONTH[is_leap(*year)][*month])) {
        return false;
    }
    return true;
}

// try to obtain date base on format "%Y-%m-%d", if failed use uncommon approach to process.
bool date::from_string_to_date(const char* date_str, size_t len, int* year, int* month, int* day) {
    const char* ptr = date_str;
    const char* end = date_str + len;
    // Skip space character
    while (ptr < end && isspace(*ptr)) {
        ++ptr;
    }
    // Skip space character
    while (ptr < end && isspace(*(end - 1))) {
        --end;
    }

    int length = end - ptr;
    // maybe It like "%Y-%m-%d".
    if (length == 10) {
        bool result = date::from_string_to_date_internal(ptr, year, month, day);
        if (!result) {
            int hour, minute, second, microsecond;
            return from_string(date_str, len, year, month, day, &hour, &minute, &second, &microsecond);
        }
        return result;
    } else {
        // se uncommon approach.
        int hour, minute, second, microsecond;
        return from_string(date_str, len, year, month, day, &hour, &minute, &second, &microsecond);
    }
}

// if format string has at least 19 chars we guess
// It like "%Y-%m-%d %H:%i:%s",
// and if 10th char is T and 11th is digit or
// if there is some space between first 10 chars and last 8 chars, return true.
// else return false;
bool date::is_standard_datetime_format(const char* ptr, int length, const char** ptr_time) {
    if (length >= 19) {
        if (isdigit(ptr[9])) {
            if (ptr[10] == 'T' && isdigit(ptr[11]) && length == 19) {
                *ptr_time = ptr + 11;
                return true;
            }
            const char* local_ptr = ptr + 10;
            length -= 18;
            for (int i = 0; i < length; ++i) {
                if (!isspace(local_ptr[i])) {
                    return false;
                }
            }
            *ptr_time = local_ptr + length;
            return true;
        }
    }
    return false;
}

// process string based on format like "%Y-%m-%d %H:%i:%s",
// if successful return true;
// else return false;
bool date::from_string_to_datetime_internal(const char* ptr_date, const char* ptr_time, int* year, int* month, int* day,
                                            int* hour, int* minute, int* second, int* microsecond) {
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
    if (char_to_digit(ptr_date, 0, &year1) || char_to_digit(ptr_date, 1, &year2) ||
        char_to_digit(ptr_date, 2, &year3) || char_to_digit(ptr_date, 3, &year4) || isdigit(ptr_date[4]) ||
        char_to_digit(ptr_date, 5, &month1) || char_to_digit(ptr_date, 6, &month2) || isdigit(ptr_date[7]) ||
        char_to_digit(ptr_date, 8, &day1) || char_to_digit(ptr_date, 9, &day2) || char_to_digit(ptr_time, 0, &hour1) ||
        char_to_digit(ptr_time, 1, &hour2) || isdigit(ptr_time[2]) || char_to_digit(ptr_time, 3, &minute1) ||
        char_to_digit(ptr_time, 4, &minute2) || isdigit(ptr_time[5]) || char_to_digit(ptr_time, 6, &second1) ||
        char_to_digit(ptr_time, 7, &second2)) {
        return false;
    }

    *year = year1 * 1000 + year2 * 100 + year3 * 10 + year4;
    *month = month1 * 10 + month2;
    *day = day1 * 10 + day2;
    *hour = hour1 * 10 + hour2;
    *minute = minute1 * 10 + minute2;
    *second = second1 * 10 + second2;
    *microsecond = 0;
    if (*month > 12 || (*day > DAYS_IN_MONTH[is_leap(*year)][*month]) || *hour > 23 || *minute > 59 || *second > 59) {
        return false;
    }

    return true;
}

// if string content is 10 chars try to process based on "%Y-%m-%d",
//    if successful return result;
//    else failed use uncommon approach.
// if string content is like "%Y-%m-%d %H:%i:%s" try to process based on %Y-%m-%d %H:%i:%s,
//    if successful return result;
//    else failed use uncommon approach.
// else use uncommon approach.
bool date::from_string_to_datetime(const char* date_str, size_t len, int* year, int* month, int* day, int* hour,
                                   int* minute, int* second, int* microsecond) {
    const char* ptr = date_str;
    const char* end = date_str + len;
    // Skip space character
    while (ptr < end && isspace(*ptr)) {
        ++ptr;
    }
    while (ptr < end && isspace(*(end - 1))) {
        --end;
    }

    int length = end - ptr;
    if (length == 10) {
        *hour = *minute = *second = *microsecond = 0;
        bool result = from_string_to_date_internal(ptr, year, month, day);
        if (!result) {
            return from_string(date_str, len, year, month, day, hour, minute, second, microsecond);
        }
        return result;
    }

    const char* ptr_date = ptr;
    const char* ptr_time = nullptr;
    if (is_standard_datetime_format(ptr, length, &ptr_time)) {
        bool result = from_string_to_datetime_internal(ptr_date, ptr_time, year, month, day, hour, minute, second,
                                                       microsecond);
        if (!result) {
            return from_string(date_str, len, year, month, day, hour, minute, second, microsecond);
        }
        return result;
    } else {
        return from_string(date_str, len, year, month, day, hour, minute, second, microsecond);
    }
}

JulianDate date::from_date(int year, int month, int day) {
    JulianDate century;
    JulianDate julian;

    if (month > 2) {
        month += 1;
        year += 4800;
    } else {
        month += 13;
        year += 4799;
    }

    century = year / 100;
    julian = year * 365 - 32167;
    julian += year / 4 - century + century / 4;
    julian += 7834 * month / 256 + day;
    return julian;
}

JulianDate date::from_date_literal(uint64_t date_literal) {
    if (date_literal >= CACHE_DATE_LITERAL_START && date_literal < CACHE_DATE_LITERAL_END) {
        return g_date_literal_to_julian_cache[date_literal - CACHE_DATE_LITERAL_START];
    }

    int year = date_literal / 10000;
    int month = (date_literal / 100) % 100;
    int day = date_literal % 100;

    return from_date(year, month, day);
}

JulianDate date::from_mysql_date(uint32_t mysql_date) {
    if (mysql_date >= CACHE_DATE_LOGIC_START && mysql_date < CACHE_DATE_LOGIC_END) {
        return g_mysql_date_to_julian_cache[mysql_date - CACHE_DATE_LOGIC_START];
    }

    int year = mysql_date >> 9;
    int month = (mysql_date >> 5) & 15;
    int day = mysql_date & 31;

    return from_date(year, month, day);
}

bool date::is_leap(int year) {
    return ((year % 4) == 0) && ((year % 100 != 0) || ((year % 400) == 0 && year));
}

bool date::check(int year, int month, int day) {
    if (year > 9999 || month > 12 || month < 1 || day > 31 || day < 1) {
        return false;
    }

    return day <= DAYS_IN_MONTH[is_leap(year)][month];
}

void date::to_string(int year, int month, int day, char* to) {
    uint32_t temp;
    temp = year / 100;
    to[0] = temp / 10 + '0';
    to[1] = temp % 10 + '0';

    temp = year % 100;
    to[2] = temp / 10 + '0';
    to[3] = temp % 10 + '0';

    to[4] = '-';

    to[5] = month / 10 + '0';
    to[6] = month % 10 + '0';

    to[7] = '-';

    to[8] = day / 10 + '0';
    to[9] = day % 10 + '0';
}

bool timestamp::check_time(int hour, int minute, int second, int microsecond) {
    return hour < HOURS_PER_DAY && minute < MINS_PER_HOUR && second < SECS_PER_MINUTE && microsecond < USECS_PER_SEC;
}

} // namespace starrocks
