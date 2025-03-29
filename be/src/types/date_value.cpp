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

#include "types/date_value.hpp"

#include "date_value.h"
#include "gutil/strings/substitute.h"
#include "types/timestamp_value.h"

namespace starrocks {

static const std::string s_day_name[] = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};
static const char* s_month_name[] = {"",     "January", "February",  "March",   "April",    "May",      "June",
                                     "July", "August",  "September", "October", "November", "December", nullptr};

static int month_to_quarter[13] = {0, 1, 1, 1, 4, 4, 4, 7, 7, 7, 10, 10, 10};
static int day_to_first[8] = {0 /*never use*/, 6, 0, 1, 2, 3, 4, 5};
static constexpr int s_days_in_month[13] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
static int month_to_quarter_end[13] = {0, 3, 3, 3, 6, 6, 6, 9, 9, 9, 12, 12, 12};

const DateValue DateValue::MAX_DATE_VALUE{date::MAX_DATE};
const DateValue DateValue::MIN_DATE_VALUE{date::MIN_DATE};

void DateValue::from_date(int year, int month, int day) {
    _julian = date::from_date(year, month, day);
}

int32_t DateValue::to_date_literal() const {
    int year, month, day;
    to_date(&year, &month, &day);
    return year * 10000 + month * 100 + day;
}

// return milliseconds since UNIX epoch.
int64_t DateValue::to_unixtime() const {
    int64_t result = (int64_t)_julian * SECS_PER_DAY;
    result -= timestamp::UNIX_EPOCH_SECONDS;
    result *= 1000L;
    return result;
}

void DateValue::from_date_literal(int64_t date_literal) {
    _julian = date::from_date_literal(date_literal);
}

bool DateValue::from_date_literal_with_check(int64_t date_literal) {
    uint64_t timestamp = date::standardize_date(date_literal);
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

    _julian = date::from_date(year, month, day);
    return true;
}

bool DateValue::get_weeks_of_year_with_cache(int* weeks) const {
    return date::get_weeks_of_year_with_cache(_julian, weeks);
}

int DateValue::get_week_of_year() const {
    return date::get_week_of_year(_julian);
}

void DateValue::from_mysql_date(uint64_t date) {
    _julian = date::from_mysql_date(date);
}

uint24_t DateValue::to_mysql_date() const {
    int y, m, d;
    to_date(&y, &m, &d);
    return {(uint32_t)(y << 9) | (m << 5) | (d)};
}

bool DateValue::from_string(const char* date_str, size_t len) {
    int year, month, day;
    // try to obtain year, month, day.
    if (!date::from_string_to_date(date_str, len, &year, &month, &day)) {
        return false;
    }

    if (!date::check(year, month, day)) {
        return false;
    }

    from_date(year, month, day);
    return true;
}

int DateValue::weekday() const {
    //  @info: _julian < 0 is impossible
    //    int w = (_julian + 1) % 7;
    //
    //    if (w < 0) {
    //        w += 7;
    //    }
    return (_julian + 1) % 7;
}

int DateValue::weekday_iso() const {
    // ISO 8601: Monday = 1, Sunday = 7
    int w = (_julian + 1) % 7;
    return (w == 0) ? 7 : w; 
}

void DateValue::trunc_to_day() {}

void DateValue::trunc_to_month() {
    int year, month, day;
    date::to_date_with_cache(_julian, &year, &month, &day);
    _julian = date::from_date(year, month, 1);
}

void DateValue::trunc_to_year() {
    int year, month, day;
    date::to_date_with_cache(_julian, &year, &month, &day);
    _julian = date::from_date(year, 1, 1);
}

void DateValue::trunc_to_week() {
    int year, month, day;
    date::to_date_with_cache(_julian - day_to_first[weekday() + 1], &year, &month, &day);
    _julian = date::from_date(year, month, day);
}

void DateValue::trunc_to_quarter() {
    int year, month, day;
    date::to_date_with_cache(_julian, &year, &month, &day);
    _julian = date::from_date(year, month_to_quarter[month], 1);
}

void DateValue::set_end_of_month() {
    int year, month, day;
    date::to_date_with_cache(_julian, &year, &month, &day);
    if ((month == 2) && date::is_leap(year)) {
        _julian = date::from_date(year, 2, 29);
    } else {
        _julian = date::from_date(year, month, s_days_in_month[month]);
    }
}

void DateValue::set_end_of_quarter() {
    int year, month, day;
    date::to_date_with_cache(_julian, &year, &month, &day);
    _julian = date::from_date(year, month_to_quarter_end[month], s_days_in_month[month_to_quarter_end[month]]);
}

void DateValue::set_end_of_year() {
    int year, month, day;
    date::to_date_with_cache(_julian, &year, &month, &day);
    _julian = date::from_date(year, 12, 31);
}

bool has_53_weeks(int year) {
    DateValue jan1;
    jan1.from_date(year, 1, 1);
    int jan1_weekday = jan1.weekday_iso();

    DateValue dec31;
    dec31.from_date(year, 12, 31);
    int dec31_weekday = dec31.weekday_iso();

    bool is_leap = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0));

    bool has_53 = (jan1_weekday == 4) ||
                  (jan1_weekday == 3 && is_leap) ||
                  (dec31_weekday == 4 || dec31_weekday == 5 || dec31_weekday == 6);
    
    return has_53;
}

DateValue get_first_monday_of_year(int year) {
    DateValue jan4;
    jan4.from_date(year, 1, 4);
    int weekday = jan4.weekday_iso();
    int days_to_subtract = weekday - 1;
    return jan4.add<TimeUnit::DAY>(-days_to_subtract);
}

int get_iso_weekday(int weekday) {
    return (weekday == 0) ? 7 : weekday;
}

bool DateValue::from_iso8601_string(const char* str, size_t len) {
    auto parse_yyyy_mm_dd = [this](int year, int month, int day) -> bool {
        this->from_date(year, month, day);
        return this->is_valid();
    };

   auto parse_yyyy_www = [this](int year, int week, int day_of_week = 1) -> bool {
       if (week == 0 || day_of_week < 1 || day_of_week > 7) {
           return false;
       }

       bool has_week_53 = has_53_weeks(year);
       if (week > (has_week_53 ? 53 : 52)) {
           return false;
       }

       DateValue first_monday = get_first_monday_of_year(year);

       int days_to_add = (week - 1) * 7 + (day_of_week - 1);
       
       DateValue dec31;
       dec31.from_date(year, 12, 31);
       int max_days = dec31._julian - first_monday._julian;
       
       if (days_to_add > max_days) {
           days_to_add = max_days - (dec31.weekday_iso() - day_of_week);
       }
       
       DateValue target_date = first_monday;
       target_date._julian += days_to_add;

       if (!target_date.is_valid()) {
           return false;
       }

       *this = target_date;
       return true;
   };

    auto parse_yyyy_ddd = [this, &parse_yyyy_mm_dd](int year, int day_of_year) -> bool {
        bool is_leap = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0));
        int max_days = is_leap ? 366 : 365;

        if (day_of_year < 1 || day_of_year > max_days) {
            return false;
        }

        int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        if (is_leap) {
            days_in_month[2] = 29;
        }

        int month = 1;
        int day = day_of_year;

        while (day > days_in_month[month]) {
            day -= days_in_month[month];
            month++;
        }

        return parse_yyyy_mm_dd(year, month, day);
    };

    if (str == nullptr || len == 0) {
        return false;
    }

    if (len < 4 || !isdigit(str[0]) || !isdigit(str[1]) || !isdigit(str[2]) || !isdigit(str[3])) {
        return false;
    }

    int year = (str[0] - '0') * 1000 + (str[1] - '0') * 100 + (str[2] - '0') * 10 + (str[3] - '0');

    if (year < 1 || year > 9999) {
        return false;
    }

    if (len >= 10 && str[4] == '-' && isdigit(str[5]) && isdigit(str[6]) && str[7] == '-' && isdigit(str[8]) && isdigit(str[9])) {
        int month = (str[5] - '0') * 10 + (str[6] - '0');
        int day = (str[8] - '0') * 10 + (str[9] - '0');
        return parse_yyyy_mm_dd(year, month, day);
    }
    else if (len >= 8 && str[4] == '-' && (str[5] == 'W' || str[5] == 'w') && isdigit(str[6]) && isdigit(str[7])) {
        int week = (str[6] - '0') * 10 + (str[7] - '0');
        int day_of_week = 1;

        if (len >= 10 && str[8] == '-' && isdigit(str[9])) {
            day_of_week = str[9] - '0';
            if (day_of_week < 1 || day_of_week > 7) {
                return false;
            }
        }

        return parse_yyyy_www(year, week, day_of_week);
    }
    else if (len >= 8 && str[4] == '-' && isdigit(str[5]) && isdigit(str[6]) && isdigit(str[7])) {
        int day_of_year = (str[5] - '0') * 100 + (str[6] - '0') * 10 + (str[7] - '0');
        return parse_yyyy_ddd(year, day_of_year);
    }
    else if (len >= 8 && isdigit(str[4]) && isdigit(str[5]) && isdigit(str[6]) && isdigit(str[7])) {
        int month = (str[4] - '0') * 10 + (str[5] - '0');
        int day = (str[6] - '0') * 10 + (str[7] - '0');
        return parse_yyyy_mm_dd(year, month, day);
    }
    else if (len == 4) {
        return parse_yyyy_mm_dd(year, 1, 1);
    }

    return false;
}

bool DateValue::is_valid() const {
    return (_julian >= date::MIN_DATE) & (_julian <= date::MAX_DATE);
}

bool DateValue::is_valid_non_strict() const {
    static const JulianDate zero_day = date::from_date(0, 0, 0);
    return is_valid() || _julian == zero_day;
}

std::string DateValue::month_name() const {
    int year, month, day;
    date::to_date_with_cache(_julian, &year, &month, &day);
    return s_month_name[month];
}

std::string DateValue::day_name() const {
    int day = weekday();
    if (day < 0 || day >= 7) {
        return {};
    }
    return s_day_name[day];
}

std::string DateValue::to_string() const {
    return date::to_string(_julian);
}

} // namespace starrocks
