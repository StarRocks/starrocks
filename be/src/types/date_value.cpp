// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "types/date_value.hpp"

#include "gutil/strings/substitute.h"
#include "types/timestamp_value.h"

namespace starrocks::vectorized {

static const std::string s_day_name[] = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};
static const char* s_month_name[] = {"",     "January", "February",  "March",   "April",    "May",      "June",
                                     "July", "August",  "September", "October", "November", "December", nullptr};

static int month_to_quarter[13] = {0, 1, 1, 1, 4, 4, 4, 7, 7, 7, 10, 10, 10};
static int day_to_first[8] = {0 /*never use*/, 6, 0, 1, 2, 3, 4, 5};

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
    return uint24_t((uint32_t)(y << 9) | (m << 5) | (d));
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
        return std::string();
    }
    return s_day_name[day];
}

std::string DateValue::to_string() const {
    return date::to_string(_julian);
}

} // namespace starrocks::vectorized
