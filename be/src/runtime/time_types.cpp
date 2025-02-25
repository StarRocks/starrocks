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

#ifdef __SSE4_1__
#include <smmintrin.h> // SSE4.1 intrinsics
#endif

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

static bool is_space(char ch) {
    // \t, \n, \v, \f, \r are 9~13, respectively.
    return UNLIKELY(ch == ' ' || (ch >= 9 && ch <= 13));
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
    while (ptr < end && is_space(*ptr)) {
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
        while (ptr < end && (ispunct(*ptr) || is_space(*ptr))) {
            if (is_space(*ptr)) {
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

    // Validate that the date values are within valid ranges
    if (*year > 9999 || *month < 1 || *month > 12 || *day < 1 || *day > DAYS_IN_MONTH[is_leap(*year)][*month]) {
        return false;
    }

    // Validate time values if present
    if (num_field > 3 && (*hour > 23 || *minute > 59 || *second > 59 || *microsecond >= 1000000)) {
        return false;
    }

    return true;
}

bool date::from_string_to_date(const char* date_str, size_t len, int* year, int* month, int* day) {
    // We can use from_string_to_datetime and just extract the date components
    ToDatetimeResult result;
    auto [is_valid, _] = from_string_to_datetime(date_str, len, &result);

    if (is_valid) {
        *year = result.year;
        *month = result.month;
        *day = result.day;
        return true;
    }

    return false;
}

// If format string has at least 19 chars we guess
// is it like "%Y-%m-%d %H:%i:%s" or "%Y-%m-%dT%H:%i:%s[.f+]Z",
// and if 10th char is T and 11th is digit followed by time format and optional microseconds and Z
// or if there is some space between first 10 chars and last 8 chars, return true.
// else return false;
// @return <is_valid, is_only_date>
//     is_valid is true if the date_str is valid datetime string.
//     is_only_date is true if the date_str only contains date part.
//         hour, minute, second, and microsecond of res will be undefined if is_only_date is true.
std::pair<bool, bool> date::from_string_to_datetime(const char* date_str, size_t len, ToDatetimeResult* res) {
    auto& [year, month, day, hour, minute, second, microsecond] = *res;

    // Reset result values
    hour = minute = second = microsecond = 0;

    // Skip leading and trailing spaces
    const char* ptr = date_str;
    const char* end = date_str + len;

    while (ptr < end && is_space(*ptr)) {
        ++ptr;
    }
    while (ptr < end && is_space(*(end - 1))) {
        --end;
    }

    int length = end - ptr;

    // Exit early if string is too short to contain a date
    if (length < 10) {
        // Fall back to generic parser as last resort
        return {from_string(date_str, len, &year, &month, &day, &hour, &minute, &second, &microsecond), false};
    }

    //
    // STEP 1: Parse the date part (YYYY-MM-DD)
    //

    bool separators_valid = false;
    bool digits_valid = false;

#ifdef __SSE4_1__
    __m128i date_chars = _mm_loadu_si128(reinterpret_cast<const __m128i*>(ptr));

    // Check positions 4 and 7 are '-' (or other separators)
    separators_valid = !isdigit(ptr[4]) && !isdigit(ptr[7]);

    if (separators_valid) {
        // Check digit positions (0-3, 5-6, 8-9)
        __m128i is_digit = _mm_and_si128(_mm_cmpgt_epi8(date_chars, _mm_set1_epi8('0' - 1)),
                                         _mm_cmplt_epi8(date_chars, _mm_set1_epi8('9' + 1)));

        // Create mask for digit positions
        const __m128i digit_mask = _mm_set_epi8(0, 0, 0, 0, 0, 0, 1, 0, 1, 1, 0, 1, 1, 0, 1, 1);

        // Check if all digit positions contain digits
        digits_valid = _mm_testc_si128(_mm_cmpeq_epi8(_mm_and_si128(is_digit, digit_mask), digit_mask), digit_mask);
    }
#else
    // Non-SIMD validation
    separators_valid = !isdigit(ptr[4]) && !isdigit(ptr[7]);
    digits_valid = isdigit(ptr[0]) && isdigit(ptr[1]) && isdigit(ptr[2]) && isdigit(ptr[3]) && isdigit(ptr[5]) &&
                   isdigit(ptr[6]) && isdigit(ptr[8]) && isdigit(ptr[9]);
#endif

    if (separators_valid && digits_valid) {
        // Extract date values directly
        year = (ptr[0] - '0') * 1000 + (ptr[1] - '0') * 100 + (ptr[2] - '0') * 10 + (ptr[3] - '0');
        month = (ptr[5] - '0') * 10 + (ptr[6] - '0');
        day = (ptr[8] - '0') * 10 + (ptr[9] - '0');

        bool date_valid = (month > 0 && month <= 12 && day > 0 && day <= DAYS_IN_MONTH[is_leap(year)][month]);

        // If date parsing failed, fall back to generic parser
        if (!date_valid) {
            return {from_string(date_str, len, &year, &month, &day, &hour, &minute, &second, &microsecond), false};
        }
    } else {
        // If validation failed, fall back to generic parser
        return {from_string(date_str, len, &year, &month, &day, &hour, &minute, &second, &microsecond), false};
    }

    // Date is valid - if we're at the end of the string, return date-only result
    if (length == 10) {
        return {true, true};
    }

    //
    // STEP 2: Check for time separator ('T' or space)
    //

    const char* time_ptr = nullptr;

    // Check for 'T' separator (ISO 8601)
    if (ptr[10] == 'T') {
        time_ptr = ptr + 11;
    }
    // Check for space separator(s)
    else if (is_space(ptr[10])) {
        // Skip all spaces
        time_ptr = ptr + 10;
        while (time_ptr < end && is_space(*time_ptr)) {
            time_ptr++;
        }
    }

    // If no valid separator or not enough chars for time part, return date-only
    if (time_ptr == nullptr || (end - time_ptr) < 8) {
        return {true, true};
    }

    //
    // STEP 3: Parse time part (HH:MM:SS)
    //

    bool time_separators_valid = false;
    bool time_digits_valid = false;

#ifdef __SSE4_1__
    // Use SIMD to validate time format
    if ((end - time_ptr) >= 8) {
        // Load 8 chars of time part
        __m128i time_chars = _mm_loadu_si128(reinterpret_cast<const __m128i*>(time_ptr));

        // Check separator positions (2 and 5 should be ':')
        time_separators_valid = (time_ptr[2] == ':' && time_ptr[5] == ':');

        if (time_separators_valid) {
            // Create mask for digit positions (0,1,3,4,6,7 should be digits)
            const __m128i time_digit_mask = _mm_set_epi8(0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 1, 1, 0, 1, 1);

            // Check which positions contain digits
            __m128i is_time_digit = _mm_and_si128(_mm_cmpgt_epi8(time_chars, _mm_set1_epi8('0' - 1)),
                                                  _mm_cmplt_epi8(time_chars, _mm_set1_epi8('9' + 1)));

            // Check if all digit positions contain digits
            time_digits_valid = _mm_testc_si128(
                    _mm_cmpeq_epi8(_mm_and_si128(is_time_digit, time_digit_mask), time_digit_mask), time_digit_mask);
        }
    }
#else
    // Non-SIMD time validation
    time_separators_valid = (time_ptr[2] == ':' && time_ptr[5] == ':');
    time_digits_valid = isdigit(time_ptr[0]) && isdigit(time_ptr[1]) && isdigit(time_ptr[3]) && isdigit(time_ptr[4]) &&
                         isdigit(time_ptr[6]) && isdigit(time_ptr[7]);
#endif

    if (time_separators_valid && time_digits_valid) {
        // Extract time components
        hour = (time_ptr[0] - '0') * 10 + (time_ptr[1] - '0');
        minute = (time_ptr[3] - '0') * 10 + (time_ptr[4] - '0');
        second = (time_ptr[6] - '0') * 10 + (time_ptr[7] - '0');

        // Validate time ranges
        bool time_valid = (hour <= 23 && minute <= 59 && second <= 59);

        // If time parsing failed, return date-only
        if (!time_valid) {
            return {true, true};
        }
    } else {
        // If validation failed, return date-only
        return {true, true};
    }

    //
    // STEP 4: Check for optional microseconds or 'Z' timezone indicator
    //

    const char* microsec_ptr = time_ptr + 8;

    // For ISO 8601 format with 'Z'
    if (microsec_ptr < end && microsec_ptr[0] == 'Z') {
        // Valid ISO format with Z, no microseconds
        return {true, false};
    }

    // Check for microseconds (.uuu or .uuuZ)
    if (microsec_ptr < end && microsec_ptr[0] == '.') {
        const char* digit_ptr = microsec_ptr + 1;
        int micro_val = 0;
        int digit_count = 0;

#ifdef __SSE4_1__
        // Optimize microsecond digit counting with SIMD if we have at least 6 digits
        if ((end - digit_ptr) >= 6) {
            __m128i micro_chars = _mm_loadu_si128(reinterpret_cast<const __m128i*>(digit_ptr));

            // Check which positions contain digits
            __m128i is_micro_digit = _mm_and_si128(_mm_cmpgt_epi8(micro_chars, _mm_set1_epi8('0' - 1)),
                                                   _mm_cmplt_epi8(micro_chars, _mm_set1_epi8('9' + 1)));

            // Create a mask for up to 6 positions
            const __m128i micro_mask = _mm_set_epi8(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1);

            // Find the first non-digit position
            __m128i not_digit = _mm_andnot_si128(is_micro_digit, micro_mask);
            int mask = _mm_movemask_epi8(not_digit & micro_mask);

            // Count leading digits (up to 6)
            digit_count = mask ? __builtin_ctz(mask) : 6;

            // Use the actual digits for parsing (handled outside the ifdef)
        }
#endif

        // Standard parsing for microseconds - used for both SIMD and non-SIMD paths
        digit_ptr = microsec_ptr + 1;
        micro_val = 0;

        if (digit_count <= 0) {
            digit_count = 0;
        }

        int i = 0;
        while (digit_ptr < end && isdigit(*digit_ptr) && i < (digit_count > 0 ? digit_count : 6)) {
            micro_val = micro_val * 10 + (*digit_ptr - '0');
            digit_ptr++;
            if (digit_count == 0) {
                digit_count++;
            }
            i++;
        }

        // Scale up for fewer than 6 digits
        if (digit_count > 0 && digit_count < 6) {
            micro_val *= LOG_10_INT[6 - digit_count];
        }

        microsecond = micro_val;

        // Check for 'Z' after microseconds (ISO 8601)
        if (digit_ptr < end && *digit_ptr == 'Z') {
            return {true, false};
        }

        if (digit_ptr == end) {
            return {true, false};
        }
    } else if (microsec_ptr == end) {
        // End of string, valid datetime without microseconds
        return {true, false};
    }

    // If we're here, the microsecond format is invalid - fall back to generic parser
    return {from_string(date_str, len, &year, &month, &day, &hour, &minute, &second, &microsecond), false};
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

} // namespace starrocks
