// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exprs/timestamp_functions.h

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

#ifndef STARROCKS_BE_SRC_QUERY_EXPRS_TIMESTAMP_FUNCTIONS_H
#define STARROCKS_BE_SRC_QUERY_EXPRS_TIMESTAMP_FUNCTIONS_H

#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/local_time/local_time.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/time_zone_base.hpp>
#include <boost/thread/thread.hpp>

#include "runtime/datetime_value.h"
#include "runtime/string_value.h"

namespace starrocks {

class Expr;
class OpcodeRegistry;
class TupleRow;

// The context used for timestamp function prepare phase,
// to save the converted date formatter, so that it doesn't
// need to be converted for each rows.
struct FormatCtx {
    // false means the format is invalid, and the function always return null
    bool is_valid = false;
    StringVal fmt;
};

// The context used for convert tz
struct ConvertTzCtx {
    // false means the format is invalid, and the function always return null
    bool is_valid = false;
    cctz::time_zone from_tz;
    cctz::time_zone to_tz;
};

class TimestampFunctions {
public:
    static void init();

    // Functions to extract parts of the timestamp, return integers.
    static starrocks_udf::IntVal year(starrocks_udf::FunctionContext* context,
                                      const starrocks_udf::DateTimeVal& ts_val);
    static starrocks_udf::IntVal quarter(starrocks_udf::FunctionContext* context,
                                         const starrocks_udf::DateTimeVal& ts_val);
    static starrocks_udf::IntVal month(starrocks_udf::FunctionContext* context,
                                       const starrocks_udf::DateTimeVal& ts_val);
    static starrocks_udf::IntVal day_of_week(starrocks_udf::FunctionContext* context,
                                             const starrocks_udf::DateTimeVal& ts_val);
    static starrocks_udf::IntVal day_of_month(starrocks_udf::FunctionContext* context,
                                              const starrocks_udf::DateTimeVal& ts_val);
    static starrocks_udf::IntVal day_of_year(starrocks_udf::FunctionContext* context,
                                             const starrocks_udf::DateTimeVal& ts_val);
    static starrocks_udf::IntVal week_of_year(starrocks_udf::FunctionContext* context,
                                              const starrocks_udf::DateTimeVal& ts_val);
    static starrocks_udf::IntVal hour(starrocks_udf::FunctionContext* context,
                                      const starrocks_udf::DateTimeVal& ts_val);
    static starrocks_udf::IntVal minute(starrocks_udf::FunctionContext* context,
                                        const starrocks_udf::DateTimeVal& ts_val);
    static starrocks_udf::IntVal second(starrocks_udf::FunctionContext* context,
                                        const starrocks_udf::DateTimeVal& ts_val);

    // Date/time functions.
    static starrocks_udf::DateTimeVal to_date(starrocks_udf::FunctionContext* ctx,
                                              const starrocks_udf::DateTimeVal& ts_val);
    static starrocks_udf::IntVal date_diff(starrocks_udf::FunctionContext* ctx,
                                           const starrocks_udf::DateTimeVal& ts_val1,
                                           const starrocks_udf::DateTimeVal& ts_val2);
    static starrocks_udf::DoubleVal time_diff(starrocks_udf::FunctionContext* ctx,
                                              const starrocks_udf::DateTimeVal& ts_val1,
                                              const starrocks_udf::DateTimeVal& ts_val2);
    static starrocks_udf::DateTimeVal years_add(starrocks_udf::FunctionContext* ctx,
                                                const starrocks_udf::DateTimeVal& ts_val,
                                                const starrocks_udf::IntVal& count);
    static starrocks_udf::DateTimeVal years_sub(starrocks_udf::FunctionContext* ctx,
                                                const starrocks_udf::DateTimeVal& ts_val,
                                                const starrocks_udf::IntVal& count);
    static starrocks_udf::DateTimeVal months_add(starrocks_udf::FunctionContext* ctx,
                                                 const starrocks_udf::DateTimeVal& ts_val,
                                                 const starrocks_udf::IntVal& count);
    static starrocks_udf::DateTimeVal months_sub(starrocks_udf::FunctionContext* ctx,
                                                 const starrocks_udf::DateTimeVal& ts_val,
                                                 const starrocks_udf::IntVal& count);
    static starrocks_udf::DateTimeVal weeks_add(starrocks_udf::FunctionContext* ctx,
                                                const starrocks_udf::DateTimeVal& ts_val,
                                                const starrocks_udf::IntVal& count);
    static starrocks_udf::DateTimeVal weeks_sub(starrocks_udf::FunctionContext* ctx,
                                                const starrocks_udf::DateTimeVal& ts_val,
                                                const starrocks_udf::IntVal& count);
    static starrocks_udf::DateTimeVal days_add(starrocks_udf::FunctionContext* ctx,
                                               const starrocks_udf::DateTimeVal& ts_val,
                                               const starrocks_udf::IntVal& count);
    static starrocks_udf::DateTimeVal days_sub(starrocks_udf::FunctionContext* ctx,
                                               const starrocks_udf::DateTimeVal& ts_val,
                                               const starrocks_udf::IntVal& count);
    static starrocks_udf::DateTimeVal hours_add(starrocks_udf::FunctionContext* ctx,
                                                const starrocks_udf::DateTimeVal& ts_val,
                                                const starrocks_udf::IntVal& count);
    static starrocks_udf::DateTimeVal hours_sub(starrocks_udf::FunctionContext* ctx,
                                                const starrocks_udf::DateTimeVal& ts_val,
                                                const starrocks_udf::IntVal& count);
    static starrocks_udf::DateTimeVal minutes_add(starrocks_udf::FunctionContext* ctx,
                                                  const starrocks_udf::DateTimeVal& ts_val,
                                                  const starrocks_udf::IntVal& count);
    static starrocks_udf::DateTimeVal minutes_sub(starrocks_udf::FunctionContext* ctx,
                                                  const starrocks_udf::DateTimeVal& ts_val,
                                                  const starrocks_udf::IntVal& count);
    static starrocks_udf::DateTimeVal seconds_add(starrocks_udf::FunctionContext* ctx,
                                                  const starrocks_udf::DateTimeVal& ts_val,
                                                  const starrocks_udf::IntVal& count);
    static starrocks_udf::DateTimeVal seconds_sub(starrocks_udf::FunctionContext* ctx,
                                                  const starrocks_udf::DateTimeVal& ts_val,
                                                  const starrocks_udf::IntVal& count);
    static starrocks_udf::DateTimeVal micros_add(starrocks_udf::FunctionContext* ctx,
                                                 const starrocks_udf::DateTimeVal& ts_val,
                                                 const starrocks_udf::IntVal& count);
    static starrocks_udf::DateTimeVal micros_sub(starrocks_udf::FunctionContext* ctx,
                                                 const starrocks_udf::DateTimeVal& ts_val,
                                                 const starrocks_udf::IntVal& count);
    static starrocks_udf::StringVal date_format(starrocks_udf::FunctionContext* ctx,
                                                const starrocks_udf::DateTimeVal& ts_val,
                                                const starrocks_udf::StringVal& format);
    static starrocks_udf::DateTimeVal from_days(starrocks_udf::FunctionContext* ctx, const starrocks_udf::IntVal& days);
    static starrocks_udf::IntVal to_days(starrocks_udf::FunctionContext* ctx, const starrocks_udf::DateTimeVal& ts_val);
    static starrocks_udf::DateTimeVal str_to_date(starrocks_udf::FunctionContext* ctx,
                                                  const starrocks_udf::StringVal& str,
                                                  const starrocks_udf::StringVal& format);
    static starrocks_udf::StringVal month_name(starrocks_udf::FunctionContext* ctx,
                                               const starrocks_udf::DateTimeVal& ts_val);
    static starrocks_udf::StringVal day_name(starrocks_udf::FunctionContext* ctx,
                                             const starrocks_udf::DateTimeVal& ts_val);

    // timestamp function
    template <TimeUnit unit>
    static starrocks_udf::BigIntVal timestamp_diff(starrocks_udf::FunctionContext* ctx,
                                                   const starrocks_udf::DateTimeVal& ts_val1,
                                                   const starrocks_udf::DateTimeVal& ts_val2);
    static starrocks_udf::BigIntVal years_diff(starrocks_udf::FunctionContext* ctx,
                                               const starrocks_udf::DateTimeVal& ts_val1,
                                               const starrocks_udf::DateTimeVal& ts_val2);
    static starrocks_udf::BigIntVal months_diff(starrocks_udf::FunctionContext* ctx,
                                                const starrocks_udf::DateTimeVal& ts_val1,
                                                const starrocks_udf::DateTimeVal& ts_val2);
    static starrocks_udf::BigIntVal weeks_diff(starrocks_udf::FunctionContext* ctx,
                                               const starrocks_udf::DateTimeVal& ts_val1,
                                               const starrocks_udf::DateTimeVal& ts_val2);
    static starrocks_udf::BigIntVal days_diff(starrocks_udf::FunctionContext* ctx,
                                              const starrocks_udf::DateTimeVal& ts_val1,
                                              const starrocks_udf::DateTimeVal& ts_val2);
    static starrocks_udf::BigIntVal hours_diff(starrocks_udf::FunctionContext* ctx,
                                               const starrocks_udf::DateTimeVal& ts_val1,
                                               const starrocks_udf::DateTimeVal& ts_val2);
    static starrocks_udf::BigIntVal minutes_diff(starrocks_udf::FunctionContext* ctx,
                                                 const starrocks_udf::DateTimeVal& ts_val1,
                                                 const starrocks_udf::DateTimeVal& ts_val2);
    static starrocks_udf::BigIntVal seconds_diff(starrocks_udf::FunctionContext* ctx,
                                                 const starrocks_udf::DateTimeVal& ts_val1,
                                                 const starrocks_udf::DateTimeVal& ts_val2);

    // TimeZone correlation functions.
    static starrocks_udf::DateTimeVal timestamp(starrocks_udf::FunctionContext* ctx,
                                                const starrocks_udf::DateTimeVal& val);
    // Helper for add/sub functions on the time portion.
    template <TimeUnit unit>
    static starrocks_udf::DateTimeVal timestamp_time_op(starrocks_udf::FunctionContext* ctx,
                                                        const starrocks_udf::DateTimeVal& ts_val,
                                                        const starrocks_udf::IntVal& count, bool is_add);
    static starrocks_udf::DateTimeVal now(starrocks_udf::FunctionContext* context);
    static starrocks_udf::DoubleVal curtime(starrocks_udf::FunctionContext* context);
    static starrocks_udf::DateTimeVal curdate(starrocks_udf::FunctionContext* context);
    static starrocks_udf::DateTimeVal utc_timestamp(starrocks_udf::FunctionContext* context);
    /// Returns the current time.
    static starrocks_udf::IntVal to_unix(FunctionContext* context, const DateTimeValue& ts_value);
    static starrocks_udf::IntVal to_unix(starrocks_udf::FunctionContext* context);
    /// Converts 'tv_val' to a unix time_t
    static starrocks_udf::IntVal to_unix(starrocks_udf::FunctionContext* context,
                                         const starrocks_udf::DateTimeVal& tv_val);
    /// Parses 'string_val' based on the format 'fmt'.
    static starrocks_udf::IntVal to_unix(starrocks_udf::FunctionContext* context,
                                         const starrocks_udf::StringVal& string_val,
                                         const starrocks_udf::StringVal& fmt);
    /// Return a timestamp string from a unix time_t
    /// Optional second argument is the format of the string.
    /// TIME is the integer type of the unix time argument.
    static starrocks_udf::StringVal from_unix(starrocks_udf::FunctionContext* context,
                                              const starrocks_udf::IntVal& unix_time);
    static starrocks_udf::StringVal from_unix(starrocks_udf::FunctionContext* context,
                                              const starrocks_udf::IntVal& unix_time,
                                              const starrocks_udf::StringVal& fmt);
    static starrocks_udf::DateTimeVal convert_tz(starrocks_udf::FunctionContext* ctx,
                                                 const starrocks_udf::DateTimeVal& ts_val,
                                                 const starrocks_udf::StringVal& from_tz,
                                                 const starrocks_udf::StringVal& to_tz);

    // Helper function to check date/time format strings.
    // TODO: eventually return format converted from Java to Boost.
    static bool check_format(const StringVal& format, DateTimeValue& t);

    // In order to support 0.11 grayscale upgrade
    // Todo(kks): remove this method when 0.12 release
    static StringVal convert_format(starrocks_udf::FunctionContext* ctx, const StringVal& format);

    // Issue a warning for a bad format string.
    static void report_bad_format(const StringVal* format);

    static void format_prepare(starrocks_udf::FunctionContext* context,
                               starrocks_udf::FunctionContext::FunctionStateScope scope);

    static void format_close(starrocks_udf::FunctionContext* context,
                             starrocks_udf::FunctionContext::FunctionStateScope scope);

    static void convert_tz_prepare(starrocks_udf::FunctionContext* context,
                                   starrocks_udf::FunctionContext::FunctionStateScope scope);

    static void convert_tz_close(starrocks_udf::FunctionContext* context,
                                 starrocks_udf::FunctionContext::FunctionStateScope scope);

    // These non-vectorized's trunc-functions will not be called
    static starrocks_udf::DateTimeVal datetime_trunc(starrocks_udf::FunctionContext* ctx,
                                                     const starrocks_udf::StringVal& format,
                                                     const starrocks_udf::DateTimeVal& ts_val);

    static void datetime_trunc_prepare(starrocks_udf::FunctionContext* context,
                                       starrocks_udf::FunctionContext::FunctionStateScope scope);

    static void datetime_trunc_close(starrocks_udf::FunctionContext* context,
                                     starrocks_udf::FunctionContext::FunctionStateScope scope);

    static starrocks_udf::DateTimeVal date_trunc(starrocks_udf::FunctionContext* ctx,
                                                 const starrocks_udf::StringVal& format,
                                                 const starrocks_udf::DateTimeVal& ts_val);

    static void date_trunc_prepare(starrocks_udf::FunctionContext* context,
                                   starrocks_udf::FunctionContext::FunctionStateScope scope);

    static void date_trunc_close(starrocks_udf::FunctionContext* context,
                                 starrocks_udf::FunctionContext::FunctionStateScope scope);
};
} // namespace starrocks

#endif
