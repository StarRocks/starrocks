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

#include "column/column_builder.h"
#include "column/vectorized_fwd.h"
#include "exprs/builtin_functions.h"
#include "exprs/function_context.h"
#include "exprs/function_helper.h"
#include "types/logical_type.h"
#include "util/timezone_hsscan.h"
namespace starrocks {

// TODO:
class TimeFunctions {
public:
    /**
     * Timestamp of now.
     * @param: []
     * @paramType columns: []
     * @return ConstColumn A ConstColumn holding a TimestampValue object.
     */
    DEFINE_VECTORIZED_FN(now);

    /**
     * Get current time.
     * @param: []
     * @paramType columns: []
     * @return ConstColumn A ConstColumn holding a double value which is measured in seconds.
     */
    DEFINE_VECTORIZED_FN(curtime);

    /**
     * Get current timestamp.
     * @param: []
     * @paramType columns: []
     * @return ConstColumn A ConstColumn holding a TimestampValue object.
     */
    DEFINE_VECTORIZED_FN(curdate);

    /**
     * @paramType columns: [TimestampColumn]
     * @return IntColumn
     */
    DEFINE_VECTORIZED_FN(year);

    /**
     * @paramType columns: [TimestampColumn]
     * @return Int16Column
     */
    DEFINE_VECTORIZED_FN(yearV2);

    DEFINE_VECTORIZED_FN(yearV3);

    /**
     * @paramType columns: [TimestampColumn]
     * @return IntColumn
     */
    DEFINE_VECTORIZED_FN(quarter);

    /**
     * Get month of the timestamp.
     * @param context
     * @param columns [TimestampColumn] Columns that hold timestamps.
     * @return IntColumn    Code of a month in a year: [1, 12].
     */
    DEFINE_VECTORIZED_FN(month);

    /**
     * Get month of the timestamp.
     * @param context
     * @param columns [TimestampColumn] Columns that hold timestamps.
     * @return Int8Column Code of a month in a year: [1, 12].
     */
    DEFINE_VECTORIZED_FN(monthV2);

    DEFINE_VECTORIZED_FN(monthV3);

    /**
     * Get day of week of the timestamp.
     * @param context
     * @param columns [TimestampColumn] Columns that hold timestamps.
     * @return  IntColumn Day of the week:
     *  - 1: Sunday
     *  - 2: Monday
     *  - 3: Tuesday
     *  - 4: Wednesday
     *  - 5: Thursday
     *  - 6: Friday
     *  - 7: Saturday
     */
    DEFINE_VECTORIZED_FN(day_of_week);

    /**
     * Get day of week of the timestamp.
     * syntax like select dayofweek_iso("2023-01-03");
     * result is 2
     * @param context
     * @param columns [TimestampColumn] Columns that hold timestamps.
     * @return  IntColumn Day of the day_of_week_iso:
     *  - 1: Monday
     *  - 2: Tuesday
     *  - 3: Wednesday
     *  - 4: Thursday
     *  - 5: Friday
     *  - 6: Saturday
     *  - 7: Sunday
     */
    DEFINE_VECTORIZED_FN(day_of_week_iso);

    /**
     * Get day of the timestamp.
     * @param context
     * @param columns [TimestampColumn] Columns that hold timestamps.
     * @return  IntColumn Day of the week:
     */
    DEFINE_VECTORIZED_FN(day);

    /**
     * Get day of the timestamp.
     * @param context
     * @param columns [TimestampColumn] Columns that hold timestamps.
     * @return  Int8Column Day of the week:
     */
    DEFINE_VECTORIZED_FN(dayV2);

    DEFINE_VECTORIZED_FN(dayV3);

    /**
     * Get day of the year.
     * @param context
     * @param columns [TimestampColumn] Columns that hold timestamps.
     * @return  IntColumn Day of the year:
     */
    DEFINE_VECTORIZED_FN(day_of_year);

    /**
     * Get week of the year.
     * @param context
     * @param columns [TimestampColumn] Columns that hold timestamps.
     * @return  IntColumn week of the year:
     */
    DEFINE_VECTORIZED_FN(week_of_year);

    /**
     * Get week of the year.
     * @param context
     * @param column[0] [TimestampColumn] Columns that hold timestamps.
     * @param mode's value is default 0.
     * @return  IntColumn week of the year:
     */
    DEFINE_VECTORIZED_FN(week_of_year_with_default_mode);

    /**
     * Get week of the year with iso.
     * @param context
     * @param column[0] [TimestampColumn] Columns that hold timestamps.
     * @return  IntColumn week of the year:
     */
    DEFINE_VECTORIZED_FN(week_of_year_iso);

    /**
     * Get week of the year.
     * @param context
     * @param column[0] [TimestampColumn] Columns that hold timestamps.
     * @param column[1] [IntColumn] Columns that hold mode.
     * @return  IntColumn week of the year:
     */
    DEFINE_VECTORIZED_FN(week_of_year_with_mode);

    /**
     * Get hour of the day
     * @param context
     * @param columns [TimestampColumn] Columns that hold timestamps.
     * @return  IntColumn hour of the day:
     */
    DEFINE_VECTORIZED_FN(hour);

    /**
     * Get hour of the day
     * @param context
     * @param columns [TimestampColumn] Columns that hold timestamps.
     * @return  Int8Column hour of the day:
     */
    DEFINE_VECTORIZED_FN(hourV2);

    /**
     * Get minute of the hour
     * @param context
     * @param columns [TimestampColumn] Columns that hold timestamps.
     * @return  IntColumn minute of the hour:
     */
    DEFINE_VECTORIZED_FN(minute);

    /**
     * Get minute of the hour
     * @param context
     * @param columns [TimestampColumn] Columns that hold timestamps.
     * @return  Int8Column minute of the hour:
     */
    DEFINE_VECTORIZED_FN(minuteV2);

    /**
     * Get second of the minute
     * @param context
     * @param columns [TimestampColumn] Columns that hold timestamps.
     * @return  IntColumn second of the minute:
     */
    DEFINE_VECTORIZED_FN(second);

    /**
     * Get second of the minute
     * @param context
     * @param columns [TimestampColumn] Columns that hold timestamps.
     * @return  IntColumn second of the minute:
     */
    DEFINE_VECTORIZED_FN(secondV2);

    /*
     * Called by datetime_trunc
     * Truncate to the corresponding part
     */
    DEFINE_VECTORIZED_FN(datetime_trunc_second);
    DEFINE_VECTORIZED_FN(datetime_trunc_minute);
    DEFINE_VECTORIZED_FN(datetime_trunc_hour);
    DEFINE_VECTORIZED_FN(datetime_trunc_day);
    DEFINE_VECTORIZED_FN(datetime_trunc_month);
    DEFINE_VECTORIZED_FN(datetime_trunc_year);
    DEFINE_VECTORIZED_FN(datetime_trunc_week);
    DEFINE_VECTORIZED_FN(datetime_trunc_quarter);
    /**
     * Get truncated time
     * @param columns @paramType columns: [BinaryColumn, TimestampColumn].
     * @return  TimestampColumn
     */
    // datetime_trunc for sql.
    DEFINE_VECTORIZED_FN(datetime_trunc);

    static Status datetime_trunc_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    static Status datetime_trunc_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    /*
     * Called by time_slice
     * Floor to the corresponding period
     */
    DEFINE_VECTORIZED_FN(time_slice_datetime_start_second);
    DEFINE_VECTORIZED_FN(time_slice_datetime_start_minute);
    DEFINE_VECTORIZED_FN(time_slice_datetime_start_hour);
    DEFINE_VECTORIZED_FN(time_slice_datetime_start_day);
    DEFINE_VECTORIZED_FN(time_slice_datetime_start_month);
    DEFINE_VECTORIZED_FN(time_slice_datetime_start_year);
    DEFINE_VECTORIZED_FN(time_slice_datetime_start_week);
    DEFINE_VECTORIZED_FN(time_slice_datetime_start_quarter);

    DEFINE_VECTORIZED_FN(time_slice_datetime_end_second);
    DEFINE_VECTORIZED_FN(time_slice_datetime_end_minute);
    DEFINE_VECTORIZED_FN(time_slice_datetime_end_hour);
    DEFINE_VECTORIZED_FN(time_slice_datetime_end_day);
    DEFINE_VECTORIZED_FN(time_slice_datetime_end_month);
    DEFINE_VECTORIZED_FN(time_slice_datetime_end_year);
    DEFINE_VECTORIZED_FN(time_slice_datetime_end_week);
    DEFINE_VECTORIZED_FN(time_slice_datetime_end_quarter);

    DEFINE_VECTORIZED_FN(time_slice_date_start_second);
    DEFINE_VECTORIZED_FN(time_slice_date_start_minute);
    DEFINE_VECTORIZED_FN(time_slice_date_start_hour);
    DEFINE_VECTORIZED_FN(time_slice_date_start_day);
    DEFINE_VECTORIZED_FN(time_slice_date_start_month);
    DEFINE_VECTORIZED_FN(time_slice_date_start_year);
    DEFINE_VECTORIZED_FN(time_slice_date_start_week);
    DEFINE_VECTORIZED_FN(time_slice_date_start_quarter);

    DEFINE_VECTORIZED_FN(time_slice_date_end_second);
    DEFINE_VECTORIZED_FN(time_slice_date_end_minute);
    DEFINE_VECTORIZED_FN(time_slice_date_end_hour);
    DEFINE_VECTORIZED_FN(time_slice_date_end_day);
    DEFINE_VECTORIZED_FN(time_slice_date_end_month);
    DEFINE_VECTORIZED_FN(time_slice_date_end_year);
    DEFINE_VECTORIZED_FN(time_slice_date_end_week);
    DEFINE_VECTORIZED_FN(time_slice_date_end_quarter);

    // time_slice for sql.
    DEFINE_VECTORIZED_FN(time_slice);

    static Status time_slice_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    static Status time_slice_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    /*
     * Called by date_trunc
     * Truncate to the corresponding part
     */
    DEFINE_VECTORIZED_FN(date_trunc_day);
    DEFINE_VECTORIZED_FN(date_trunc_month);
    DEFINE_VECTORIZED_FN(date_trunc_year);
    DEFINE_VECTORIZED_FN(date_trunc_week);
    DEFINE_VECTORIZED_FN(date_trunc_quarter);
    /**
     * Get truncated time
     * @param columns @paramType columns: [BinaryColumn, DateColumn].
     * @return  DateColumn
     */
    // datetime_trunc for sql.
    DEFINE_VECTORIZED_FN(date_trunc);

    static Status date_trunc_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    static Status date_trunc_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    DEFINE_VECTORIZED_FN(month_name);
    DEFINE_VECTORIZED_FN(day_name);

    static Status convert_tz_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    static Status convert_tz_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    DEFINE_VECTORIZED_FN(convert_tz);

    DEFINE_VECTORIZED_FN(utc_timestamp);

    DEFINE_VECTORIZED_FN(utc_time);

    DEFINE_VECTORIZED_FN(timestamp);

    /**
     * Get DateValue from timestamp.
     * @param context
     * @param columns [TimestampColumn] Columns that hold timestamps.
     * @return  DateColumn  Date that corresponds to the timestamp.
     */
    DEFINE_VECTORIZED_FN(to_date);

    /**
     * Calculate days from the first timestamp to the second timestamp. Only the date part of the timestamps are used in calculation.
     * @param context
     * @param columns [TimestampColumn] Columns that holds two groups timestamps for calculation.
     * @return  BigIntColumn Difference in days between the two timestamps. It can be negative.
     */
    DEFINE_VECTORIZED_FN(date_diff);

    /**
     * Calculate time difference in seconds from the first timestamp to the second timestamp.
     * @param context
     * @param columns [TimestampColumn] Columns that holds two groups timestamps for calculation.
     * @return  DoubleColumn Time difference in seconds between the two timestamps. It can be negative.
     */
    DEFINE_VECTORIZED_FN(time_diff);

    /**
     * Calculate times from the first timestamp to the second timestamp. according to type return bigint in hour/minute/second/millisecond.
     * @param context
     * @param columns [TimestampColumn] Columns that holds two groups timestamps for calculation.
     * @return  BigIntColumn Difference in times between the two timestamps. It can be negative.
     */
    DEFINE_VECTORIZED_FN(datediff);
    static Status datediff_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    static Status datediff_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    /*
     * Called by date_diff to handle
     */
    static StatusOr<ColumnPtr> date_diff_time(FunctionContext* context, const Columns& columns, int64_t t);

    // function for datediff
    struct DateDiffCtx {
        int64_t type;
        StatusOr<ColumnPtr> (*function)(FunctionContext* context, const Columns& columns, int64_t t);
    };

    /**
     * @param: [timestmap, year]
     * @paramType columns: [TimestampColumn, IntColumn]
     * @return TimestampColumn
     */
    DEFINE_VECTORIZED_FN(years_add);
    DEFINE_VECTORIZED_FN(years_sub);

    /**
     * @param: [timestmap, quarter]
     * @paramType columns: [TimestampColumn, IntColumn]
     * @return TimestampColumn
     */
    DEFINE_VECTORIZED_FN(quarters_add);
    DEFINE_VECTORIZED_FN(quarters_sub);

    /**
     * @param: [timestmap, month]
     * @paramType columns: [TimestampColumn, IntColumn]
     * @return TimestampColumn
     */
    DEFINE_VECTORIZED_FN(months_add);
    DEFINE_VECTORIZED_FN(months_sub);

    /**
     * @param: [timestmap, month]
     * @paramType columns: [TimestampColumn, IntColumn]
     * @return TimestampColumn
     */
    DEFINE_VECTORIZED_FN(weeks_add);
    DEFINE_VECTORIZED_FN(weeks_sub);

    /**
     * @param: [timestmap, days]
     * @paramType columns: [TimestampColumn, IntColumn]
     * @return TimestampColumn
     */
    DEFINE_VECTORIZED_FN(days_add);
    DEFINE_VECTORIZED_FN(days_sub);

    /**
     * @param: [timestmap, hours]
     * @paramType columns: [TimestampColumn, IntColumn]
     * @return TimestampColumn
     */
    DEFINE_VECTORIZED_FN(hours_add);
    DEFINE_VECTORIZED_FN(hours_sub);

    /**
     * @param: [timestmap, minutes]
     * @paramType columns: [TimestampColumn, IntColumn]
     * @return TimestampColumn
     */
    DEFINE_VECTORIZED_FN(minutes_add);
    DEFINE_VECTORIZED_FN(minutes_sub);

    /**
     * @param: [timestmap, seconds]
     * @paramType columns: [TimestampColumn, IntColumn]
     * @return TimestampColumn
     */
    DEFINE_VECTORIZED_FN(seconds_add);
    DEFINE_VECTORIZED_FN(seconds_sub);

    /**
     * @param: [timestmap, micros]
     * @paramType columns: [TimestampColumn, IntColumn]
     * @return TimestampColumn
     */
    DEFINE_VECTORIZED_FN(micros_add);
    DEFINE_VECTORIZED_FN(micros_sub);

    /**
     * @param: [timestmap, millis]
     * @paramType columns: [TimestampColumn, IntColumn]
     * @return TimestampColumn
     */
    DEFINE_VECTORIZED_FN(millis_add);
    DEFINE_VECTORIZED_FN(millis_sub);

    /**
     * @param: [timestmap, timestamp]
     * @paramType columns: [TimestampColumn, TimestampColumn]
     * @return BigIntColumn
     */
    DEFINE_VECTORIZED_FN(years_diff);

    /**
     *
     * @param context
     * @param columns [TimestampColumn, TimestampColumn]
     * @return BigIntColumn
     */
    DEFINE_VECTORIZED_FN(quarters_diff);

    /**
     * @param: [timestmap, timestamp]
     * @paramType columns: [TimestampColumn, TimestampColumn]
     * @return BigIntColumn
     */
    DEFINE_VECTORIZED_FN(months_diff);

    /**
     * Time difference in weeks.
     * @param context
     * @param columns [Timestampcolumn, TimestampColumn]
     * @return BigIntColumn
     */
    DEFINE_VECTORIZED_FN(weeks_diff);

    /**
     * @param: [timestmap, timestamp]
     * @paramType columns: [TimestampColumn, TimestampColumn]
     * @return BigIntColumn
     */
    DEFINE_VECTORIZED_FN(days_diff);

    /**
     *
     * @param context
     * @param columns [TimestampColumn, TimestampColumn]
     * @return BigIntColumn
     */
    DEFINE_VECTORIZED_FN(hours_diff);

    /**
     *
     * @param context
     * @param columns [TimestampColumn, TimestampColumn]
     * @return BigIntColumn
     */
    DEFINE_VECTORIZED_FN(minutes_diff);

    /**
     *
     * @param context
     * @param columns [TimestampColumn, TimestampColumn]
     * @return BigIntColumn
     */
    DEFINE_VECTORIZED_FN(seconds_diff);

    /**
     * DateValue from number of days.
     * @param context
     * @param columns   [Int32Column] Number of the days since 0000-00-00 (according to MySQL function TO_DAYS).
     * @return  DateColumn
     */
    DEFINE_VECTORIZED_FN(from_days);

    /**
     * DateValue to number of days.
     * @param context
     * @param columns   [DateColumn]
     * @return  Int32Column   Number of the days since 0000-00-00 (according to MySQL function TO_DAYS).
     */
    DEFINE_VECTORIZED_FN(to_days);

    // try to transfer content to date format based on "%Y-%m-%d",
    // if successful, return result TimestampValue
    // else take a uncommon approach to process this content.
    static StatusOr<ColumnPtr> str_to_date_from_date_format(FunctionContext* context, const starrocks::Columns& columns,
                                                            const char* str_format);

    // try to transfer content to date format based on "%Y-%m-%d %H:%i:%s",
    // if successful, return result TimestampValue
    // else take a uncommon approach to process this content.
    static StatusOr<ColumnPtr> str_to_date_from_datetime_format(FunctionContext* context,
                                                                const starrocks::Columns& columns,
                                                                const char* str_format);

    // Try to process string content, based on uncommon string format
    static StatusOr<ColumnPtr> str_to_date_uncommon(FunctionContext* context, const starrocks::Columns& columns);
    /**
     *
     * cast string to datetime
     * @param context
     * @param columns [BinaryColumn of TYPE_VARCHAR, BinaryColumn of TYPE_VARCHAR]  The first column holds the datetime string, the second column holds the format.
     * @return  TimestampColumn
     */
    DEFINE_VECTORIZED_FN(str_to_date);

    /**
     *
     * cast string to date, the function will call by FE getStrToDateFunction, and is invisible to user
     *
     */
    DEFINE_VECTORIZED_FN(str2date);

    static bool is_date_format(const Slice& slice, char** start);
    static bool is_datetime_format(const Slice& slice, char** start);

    static Status str_to_date_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    static Status str_to_date_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    static Status format_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    static Status format_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    static Status jodatime_format_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    static Status jodatime_format_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    /**
     * Format TimestampValue.
     * @param context
     * @param columns [TimestampColumn, BinaryColumn of TYPE_VARCHAR] The first column holds the timestamp, the second column holds the format.
     * @return  BinaryColumn of TYPE_VARCHAR.
     */
    DEFINE_VECTORIZED_FN(datetime_format);

    /**
     * Format DateValue.
     * @param context
     * @param columns [DateColumn, BinaryColumn of TYPE_VARCHAR] The first column holds the date, the second column holds the format.
     * @return  BinaryColumn of TYPE_VARCHAR.
     */
    DEFINE_VECTORIZED_FN(date_format);
    //    DEFINE_VECTORIZED_FN(month_name);
    //    DEFINE_VECTORIZED_FN(day_name);

    /**
     * Format TimestampValue using JodaTime’s date time format
     * @param context
     * @param columns [TimestampColumn, BinaryColumn of TYPE_VARCHAR] The first column holds the timestamp, the second column holds the format.
     * @return  BinaryColumn of TYPE_VARCHAR.
     */
    DEFINE_VECTORIZED_FN(jodadatetime_format);

    /**
     * Format DateValue using JodaTime’s date time format
     * @param context
     * @param columns [DateColumn, BinaryColumn of TYPE_VARCHAR] The first column holds the date, the second column holds the format.
     * @return  BinaryColumn of TYPE_VARCHAR.
     */
    DEFINE_VECTORIZED_FN(jodadate_format);

    /**
     * @param: [timestampstr, formatstr]
     * @paramType columns: [BinaryColumn, BinaryColumn]
     * @return BigIntColumn
     */
    DEFINE_VECTORIZED_FN(to_unix_from_datetime_with_format_64);
    DEFINE_VECTORIZED_FN(to_unix_from_datetime_with_format_32);

    /**
     * @param: [timestamp]
     * @paramType columns: [TimestampColumn]
     * @return BigIntColumn
     */
    DEFINE_VECTORIZED_FN(to_unix_from_datetime_64);
    DEFINE_VECTORIZED_FN(to_unix_from_datetime_32);

    /**
     * @param: [date]
     * @paramType columns: [DateColumn]
     * @return BigIntColumn
     */
    DEFINE_VECTORIZED_FN(to_unix_from_date_64);
    DEFINE_VECTORIZED_FN(to_unix_from_date_32);

    /**
     * @param: []
     * @return ConstColumn
     */
    DEFINE_VECTORIZED_FN(to_unix_for_now_64);
    DEFINE_VECTORIZED_FN(to_unix_for_now_32);

    /**
     * @param: [timestmap]
     * @paramType columns: [IntColumn]
     * @return BinaryColumn
     */
    DEFINE_VECTORIZED_FN(from_unix_to_datetime_64);
    DEFINE_VECTORIZED_FN(from_unix_to_datetime_32);

    // from_unix_datetime with format's auxiliary method
    static Status from_unix_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    static Status from_unix_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    /**
     * @param: [timestamp, formatstr]
     * @paramType columns: [IntColumn, BinaryColumn]
     * @return BinaryColumn
     */
    DEFINE_VECTORIZED_FN(from_unix_to_datetime_with_format_64);
    DEFINE_VECTORIZED_FN(from_unix_to_datetime_with_format_32);

    /**
     * return number of seconds in this day.
     * @param: [varchar]
     * @paramType columns: [BinaryColumn]
     * @return Int64Column
     */
    DEFINE_VECTORIZED_FN(time_to_sec);

    /**
     * Returns the date of the first specified DOW (day of week) that occurs after the input date.
     * @param: [timestamp, dow]
     * @paramType columns: [TimestampColumn, BinaryColumn of TYPE_VARCHAR]
     * @return DateColumn of TYPE_DATE.
     */
    DEFINE_VECTORIZED_FN(next_day);

    static Status next_day_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    static Status next_day_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    // Process the case where dow is not constant in next_day
    static StatusOr<ColumnPtr> next_day_common(FunctionContext* context, const Columns& columns);

    // Process the case where dow is constant in next_day
    static StatusOr<ColumnPtr> next_day_wdc(FunctionContext* context, const Columns& columns);

    /**
     * Returns the date of the first specified DOW (day of week) that occurs before the input date.
     * @param: [timestamp, dow]
     * @paramType columns: [TimestampColumn, BinaryColumn of TYPE_VARCHAR]
     * @return DateColumn of TYPE_DATE.
     */
    DEFINE_VECTORIZED_FN(previous_day);

    static Status previous_day_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    static Status previous_day_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    // Process the case where dow is not constant in previous_day
    static StatusOr<ColumnPtr> previous_day_common(FunctionContext* context, const Columns& columns);

    // Process the case where dow is constant in previous_day
    static StatusOr<ColumnPtr> previous_day_wdc(FunctionContext* context, const Columns& columns);

    /**
     * Returns the last day of the specified date part for a date or datetime.
     * @param: [date_or_datetime_expr, date_part]
     * @paramType columns: [TimestampColumn, VARCHAR]
     * @return DateColumn of TYPE_DATE.
     */
    DEFINE_VECTORIZED_FN(last_day);
    DEFINE_VECTORIZED_FN(last_day_with_format);

    static Status last_day_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    static Status last_day_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    // Following const variables used to obtains number days of year
    constexpr static int NUMBER_OF_LEAP_YEAR = 366;
    constexpr static int NUMBER_OF_NON_LEAP_YEAR = 365;

    static long compute_daynr(uint year, uint month, uint day);
    static int compute_weekday(long daynr, bool sunday_first_day_of_week);
    static uint32_t compute_days_in_year(uint year);
    static uint week_mode(uint mode);
    static int32_t compute_week(uint year, uint month, uint day, uint week_behaviour);

    /**
     * calculate a date by year and day of year
     * @param: [year, dayOfYear]
     * @paramType columns:[IntColumn,IntColumn]
     * @return DateColumn
     */
    DEFINE_VECTORIZED_FN(make_date);

    /** Flags for calc_week() function.  */
    constexpr static const unsigned int WEEK_MONDAY_FIRST = 1;
    constexpr static const unsigned int WEEK_YEAR = 2;
    constexpr static const unsigned int WEEK_FIRST_WEEKDAY = 4;

    // It's really hard to define max unix timestamp because of timezone.
    // so this value is 253402329599(UTC 9999-12-31 23:59:59) - 24 * 3600(for all timezones)
    constexpr static const int64_t MAX_UNIX_TIMESTAMP = 253402243199L;

private:
    DEFINE_VECTORIZED_FN_TEMPLATE(_t_from_unix_to_datetime);

    DEFINE_VECTORIZED_FN_TEMPLATE(_t_to_unix_from_datetime);

    DEFINE_VECTORIZED_FN_TEMPLATE(_t_to_unix_from_date);

    DEFINE_VECTORIZED_FN_TEMPLATE(_t_to_unix_from_datetime_with_format);

    // internal approach to process string content, based on any string format.
    static void str_to_date_internal(TimestampValue* ts, const Slice& fmt, const Slice& str,
                                     ColumnBuilder<TYPE_DATETIME>* result);

    static std::string convert_format(const Slice& format);

    DEFINE_VECTORIZED_FN_TEMPLATE(_t_from_unix_with_format);
    DEFINE_VECTORIZED_FN_TEMPLATE(_t_from_unix_with_format_general);

    template <LogicalType TIMESTAMP_TYPE>
    static StatusOr<ColumnPtr> _t_from_unix_with_format_const(std::string& format_content, FunctionContext* context,
                                                              const starrocks::Columns& columns);

    static StatusOr<ColumnPtr> convert_tz_general(FunctionContext* context, const Columns& columns);

    static StatusOr<ColumnPtr> convert_tz_const(FunctionContext* context, const Columns& columns,
                                                const cctz::time_zone& from, const cctz::time_zone& to);

    static StatusOr<ColumnPtr> _last_day_with_format(FunctionContext* context, const Columns& columns);
    static StatusOr<ColumnPtr> _last_day_with_format_const(std::string& format_content, FunctionContext* context,
                                                           const Columns& columns);
    static Status _error_date_part();

public:
    static TimestampValue start_of_time_slice;
    static std::string info_reported_by_time_slice;

    enum FormatType {
        yyyyMMdd,
        yyyy_MM_dd,
        yyyy_MM_dd_HH_mm_ss,
        yyyy_MM,
        yyyyMM,
        yyyy,
        // for string format like "%Y-%m-%d"
        yyyycMMcdd,
        // for string format like "%Y-%m-%d %H:%i:%s"
        yyyycMMcddcHHcmmcss,
        None
    };

    struct FormatCtx {
        bool is_valid = false;
        std::string fmt;
        int len;
        FormatType fmt_type;
    };

private:
    struct FromUnixState {
        bool const_format{false};
        std::string format_content;
        FromUnixState() = default;
    };

    // The context used for convert tz
    struct ConvertTzCtx {
        // false means the format is invalid, and the function always return null
        bool is_valid = false;
        cctz::time_zone from_tz;
        cctz::time_zone to_tz;
    };

    // fmt for string format like "%Y-%m-%d" and "%Y-%m-%d %H:%i:%s"
    struct StrToDateCtx {
        FormatType fmt_type;
        char* fmt;
    };

    // method for datetime_trunc and time_slice
    struct DateTruncCtx {
        ScalarFunction function;
    };

    // weekday context
    struct WeekDayCtx {
        int dow_weekday;
    };

    // last_day ctx
    struct LastDayCtx {
        bool const_optional{false};
        std::string optional_content;
    };

    template <LogicalType Type>
    friend StatusOr<ColumnPtr> do_format(const FormatCtx* ctx, const Columns& cols);
};

} // namespace starrocks