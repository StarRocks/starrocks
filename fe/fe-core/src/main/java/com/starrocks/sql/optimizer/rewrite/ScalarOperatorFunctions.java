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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/sql/optimizer/rewrite/ScalarOperatorFunctions.java

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

package com.starrocks.sql.optimizer.rewrite;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.re2j.Pattern;
import com.starrocks.analysis.DecimalLiteral;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.apache.commons.lang.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalAdjusters;
import java.time.temporal.TemporalUnit;
import java.util.Map;
import java.util.Set;

import static com.starrocks.catalog.PrimitiveType.BIGINT;
import static com.starrocks.catalog.PrimitiveType.DATE;
import static com.starrocks.catalog.PrimitiveType.DATETIME;
import static com.starrocks.catalog.PrimitiveType.DECIMAL128;
import static com.starrocks.catalog.PrimitiveType.DECIMAL32;
import static com.starrocks.catalog.PrimitiveType.DECIMAL64;
import static com.starrocks.catalog.PrimitiveType.DECIMALV2;
import static com.starrocks.catalog.PrimitiveType.DOUBLE;
import static com.starrocks.catalog.PrimitiveType.INT;
import static com.starrocks.catalog.PrimitiveType.LARGEINT;
import static com.starrocks.catalog.PrimitiveType.SMALLINT;
import static com.starrocks.catalog.PrimitiveType.TIME;
import static com.starrocks.catalog.PrimitiveType.TINYINT;
import static com.starrocks.catalog.PrimitiveType.VARCHAR;

/**
 * Constant Functions List
 */
public class ScalarOperatorFunctions {
    private static final Set<String> SUPPORT_JAVA_STYLE_DATETIME_FORMATTER =
            ImmutableSet.<String>builder().add("yyyy-MM-dd").add("yyyy-MM-dd HH:mm:ss").add("yyyyMMdd").build();

    private static final Pattern HAS_TIME_PART = Pattern.compile("^.*[HhIiklrSsT]+.*$");

    private static final int CONSTANT_128 = 128;
    private static final BigInteger INT_128_OPENER = BigInteger.ONE.shiftLeft(CONSTANT_128 + 1);
    private static final BigInteger[] INT_128_MASK1_ARR1 = new BigInteger[CONSTANT_128];

    private static final int YEAR_MIN = 0;
    private static final int YEAR_MAX = 9999;
    private static final int DAY_OF_YEAR_MIN = 1;
    private static final int DAY_OF_YEAR_MAX = 366;

    private static final LocalDateTime TIME_SLICE_START = LocalDateTime.of(1, 1, 1, 0, 0);

    private static final Map<String, TemporalUnit> TIME_SLICE_UNIT_MAPPING;

    static {
        for (int shiftBy = 0; shiftBy < CONSTANT_128; ++shiftBy) {
            INT_128_MASK1_ARR1[shiftBy] = INT_128_OPENER.subtract(BigInteger.ONE).shiftRight(shiftBy + 1);
        }

        TIME_SLICE_UNIT_MAPPING = ImmutableMap.<String, TemporalUnit>builder()
            .put("second", ChronoUnit.SECONDS)
            .put("minute", ChronoUnit.MINUTES)
            .put("hour", ChronoUnit.HOURS)
            .put("day", ChronoUnit.DAYS)
            .put("month", ChronoUnit.MONTHS)
            .put("year", ChronoUnit.YEARS)
            .put("week", ChronoUnit.WEEKS)
            .put("quarter", IsoFields.QUARTER_YEARS)
            .build();
    }

    /**
     * date and time function
     */
    @ConstantFunction(name = "timediff", argTypes = {DATETIME, DATETIME}, returnType = TIME)
    public static ConstantOperator timeDiff(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createTime(Duration.between(second.getDatetime(), first.getDatetime()).getSeconds());
    }

    @ConstantFunction(name = "datediff", argTypes = {DATETIME, DATETIME}, returnType = INT)
    public static ConstantOperator dateDiff(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createInt((int) Duration.between(
                second.getDatetime().truncatedTo(ChronoUnit.DAYS),
                first.getDatetime().truncatedTo(ChronoUnit.DAYS)).toDays());
    }

    @ConstantFunction(name = "years_add", argTypes = {DATETIME, INT}, returnType = DATETIME)
    public static ConstantOperator yearsAdd(ConstantOperator date, ConstantOperator year) {
        return ConstantOperator.createDatetime(date.getDatetime().plusYears(year.getInt()));
    }

    @ConstantFunction.List(list = {
            @ConstantFunction(name = "months_add", argTypes = {DATETIME, INT}, returnType = DATETIME),
            @ConstantFunction(name = "add_months", argTypes = {DATETIME, INT}, returnType = DATETIME)
    })
    public static ConstantOperator monthsAdd(ConstantOperator date, ConstantOperator month) {
        return ConstantOperator.createDatetime(date.getDatetime().plusMonths(month.getInt()));
    }

    @ConstantFunction.List(list = {
            @ConstantFunction(name = "adddate", argTypes = {DATETIME, INT}, returnType = DATETIME),
            @ConstantFunction(name = "date_add", argTypes = {DATETIME, INT}, returnType = DATETIME),
            @ConstantFunction(name = "days_add", argTypes = {DATETIME, INT}, returnType = DATETIME)
    })
    public static ConstantOperator daysAdd(ConstantOperator date, ConstantOperator day) {
        return ConstantOperator.createDatetime(date.getDatetime().plusDays(day.getInt()));
    }

    @ConstantFunction(name = "hours_add", argTypes = {DATETIME, INT}, returnType = DATETIME)
    public static ConstantOperator hoursAdd(ConstantOperator date, ConstantOperator hour) {
        return ConstantOperator.createDatetime(date.getDatetime().plusHours(hour.getInt()));
    }

    @ConstantFunction(name = "minutes_add", argTypes = {DATETIME, INT}, returnType = DATETIME)
    public static ConstantOperator minutesAdd(ConstantOperator date, ConstantOperator minute) {
        return ConstantOperator.createDatetime(date.getDatetime().plusMinutes(minute.getInt()));
    }

    @ConstantFunction(name = "seconds_add", argTypes = {DATETIME, INT}, returnType = DATETIME)
    public static ConstantOperator secondsAdd(ConstantOperator date, ConstantOperator second) {
        return ConstantOperator.createDatetime(date.getDatetime().plusSeconds(second.getInt()));
    }

    @ConstantFunction(name = "date_trunc", argTypes = {VARCHAR, DATETIME}, returnType = DATETIME)
    public static ConstantOperator dateTrunc(ConstantOperator fmt, ConstantOperator date) {
        switch (fmt.getVarchar()) {
            case "second":
                return ConstantOperator.createDatetime(date.getDatetime().truncatedTo(ChronoUnit.SECONDS));
            case "minute":
                return ConstantOperator.createDatetime(date.getDatetime().truncatedTo(ChronoUnit.MINUTES));
            case "hour":
                return ConstantOperator.createDatetime(date.getDatetime().truncatedTo(ChronoUnit.HOURS));
            case "day":
                return ConstantOperator.createDatetime(date.getDatetime().truncatedTo(ChronoUnit.DAYS));
            case "month":
                return ConstantOperator.createDatetime(
                        date.getDatetime().with(TemporalAdjusters.firstDayOfMonth()).truncatedTo(ChronoUnit.DAYS));
            case "year":
                return ConstantOperator.createDatetime(
                        date.getDatetime().with(TemporalAdjusters.firstDayOfYear()).truncatedTo(ChronoUnit.DAYS));
            case "week":
                return ConstantOperator.createDatetime(
                        date.getDatetime().with(DayOfWeek.MONDAY).truncatedTo(ChronoUnit.DAYS));
            case "quarter":
                int year = date.getDatetime().getYear();
                int month = date.getDatetime().getMonthValue();
                int quarterMonth = (month - 1) / 3 * 3 + 1;
                LocalDateTime quarterDate = LocalDateTime.of(year, quarterMonth, 1, 0, 0);
                return ConstantOperator.createDatetime(quarterDate);
            default:
                throw new IllegalArgumentException(fmt + " not supported in date_trunc format string");
        }
    }

    @ConstantFunction.List(list = {
            @ConstantFunction(name = "date_format", argTypes = {DATETIME, VARCHAR}, returnType = VARCHAR),
            @ConstantFunction(name = "date_format", argTypes = {DATE, VARCHAR}, returnType = VARCHAR)
    })
    public static ConstantOperator dateFormat(ConstantOperator date, ConstantOperator fmtLiteral) {
        String format = fmtLiteral.getVarchar();
        if (format.isEmpty()) {
            return ConstantOperator.createNull(Type.VARCHAR);
        }
        // unix style
        if (!SUPPORT_JAVA_STYLE_DATETIME_FORMATTER.contains(format.trim())) {
            DateTimeFormatter builder = DateUtils.unixDatetimeFormatter(fmtLiteral.getVarchar());
            return ConstantOperator.createVarchar(builder.format(date.getDatetime()));
        } else {
            String result = date.getDatetime().format(DateTimeFormatter.ofPattern(fmtLiteral.getVarchar()));
            return ConstantOperator.createVarchar(result);
        }
    }

    @ConstantFunction(name = "str_to_date", argTypes = {VARCHAR, VARCHAR}, returnType = DATETIME)
    public static ConstantOperator dateParse(ConstantOperator date, ConstantOperator fmtLiteral) {
        DateTimeFormatter builder = DateUtils.unixDatetimeFormatter(fmtLiteral.getVarchar(), false);
        String dateStr = StringUtils.strip(date.getVarchar(), "\r\n\t ");
        if (HAS_TIME_PART.matcher(fmtLiteral.getVarchar()).matches()) {
            LocalDateTime ldt;
            try {
                ldt = LocalDateTime.from(builder.withResolverStyle(ResolverStyle.STRICT).parse(dateStr));
            } catch (DateTimeParseException e) {
                // If parsing fails, it can be re-parsed from the position of the successful prefix string.
                // This way datetime string can use incomplete format
                // eg. str_to_date('2022-10-18 00:00:00','%Y-%m-%d %H:%s');
                ldt = LocalDateTime.from(builder.withResolverStyle(ResolverStyle.STRICT)
                        .parse(dateStr.substring(0, e.getErrorIndex())));
            }
            return ConstantOperator.createDatetime(ldt, Type.DATETIME);
        } else {
            LocalDate ld = LocalDate.from(builder.withResolverStyle(ResolverStyle.STRICT).parse(dateStr));
            return ConstantOperator.createDatetime(ld.atTime(0, 0, 0), Type.DATETIME);
        }
    }

    @ConstantFunction(name = "str2date", argTypes = {VARCHAR, VARCHAR}, returnType = DATE)
    public static ConstantOperator str2Date(ConstantOperator date, ConstantOperator fmtLiteral) {
        DateTimeFormatterBuilder builder = DateUtils.unixDatetimeFormatBuilder(fmtLiteral.getVarchar(), false);
        LocalDate ld = LocalDate.from(builder.toFormatter().withResolverStyle(ResolverStyle.STRICT).parse(
                StringUtils.strip(date.getVarchar(), "\r\n\t ")));
        return ConstantOperator.createDatetime(ld.atTime(0, 0, 0), Type.DATE);
    }

    @ConstantFunction(name = "years_sub", argTypes = {DATETIME, INT}, returnType = DATETIME)
    public static ConstantOperator yearsSub(ConstantOperator date, ConstantOperator year) {
        return ConstantOperator.createDatetime(date.getDatetime().minusYears(year.getInt()));
    }

    @ConstantFunction(name = "months_sub", argTypes = {DATETIME, INT}, returnType = DATETIME)
    public static ConstantOperator monthsSub(ConstantOperator date, ConstantOperator month) {
        return ConstantOperator.createDatetime(date.getDatetime().minusMonths(month.getInt()));
    }

    @ConstantFunction.List(list = {
            @ConstantFunction(name = "subdate", argTypes = {DATETIME, INT}, returnType = DATETIME),
            @ConstantFunction(name = "date_sub", argTypes = {DATETIME, INT}, returnType = DATETIME),
            @ConstantFunction(name = "days_sub", argTypes = {DATETIME, INT}, returnType = DATETIME)
    })
    public static ConstantOperator daysSub(ConstantOperator date, ConstantOperator day) {
        return ConstantOperator.createDatetime(date.getDatetime().minusDays(day.getInt()));
    }

    @ConstantFunction(name = "hours_sub", argTypes = {DATETIME, INT}, returnType = DATETIME)
    public static ConstantOperator hoursSub(ConstantOperator date, ConstantOperator hour) {
        return ConstantOperator.createDatetime(date.getDatetime().minusHours(hour.getInt()));
    }

    @ConstantFunction(name = "minutes_sub", argTypes = {DATETIME, INT}, returnType = DATETIME)
    public static ConstantOperator minutesSub(ConstantOperator date, ConstantOperator minute) {
        return ConstantOperator.createDatetime(date.getDatetime().minusMinutes(minute.getInt()));
    }

    @ConstantFunction(name = "seconds_sub", argTypes = {DATETIME, INT}, returnType = DATETIME)
    public static ConstantOperator secondsSub(ConstantOperator date, ConstantOperator second) {
        return ConstantOperator.createDatetime(date.getDatetime().minusSeconds(second.getInt()));
    }

    @ConstantFunction.List(list = {
            @ConstantFunction(name = "year", argTypes = {DATETIME}, returnType = SMALLINT),
            @ConstantFunction(name = "year", argTypes = {DATE}, returnType = SMALLINT)
    })
    public static ConstantOperator year(ConstantOperator arg) {
        return ConstantOperator.createSmallInt((short) arg.getDatetime().getYear());
    }

    @ConstantFunction.List(list = {
            @ConstantFunction(name = "month", argTypes = {DATETIME}, returnType = TINYINT),
            @ConstantFunction(name = "month", argTypes = {DATE}, returnType = TINYINT)
    })
    public static ConstantOperator month(ConstantOperator arg) {
        return ConstantOperator.createTinyInt((byte) arg.getDatetime().getMonthValue());
    }

    @ConstantFunction.List(list = {
            @ConstantFunction(name = "day", argTypes = {DATETIME}, returnType = TINYINT),
            @ConstantFunction(name = "day", argTypes = {DATE}, returnType = TINYINT)
    })
    public static ConstantOperator day(ConstantOperator arg) {
        return ConstantOperator.createTinyInt((byte) arg.getDatetime().getDayOfMonth());
    }

    @ConstantFunction(name = "date", argTypes = {DATETIME}, returnType = DATE)
    public static ConstantOperator date(ConstantOperator arg) {
        LocalDateTime datetime = LocalDateTime.of(arg.getDate().toLocalDate(), LocalTime.MIN);
        return ConstantOperator.createDate(datetime);
    }

    @ConstantFunction(name = "timestamp", argTypes = {DATETIME}, returnType = DATETIME)
    public static ConstantOperator timestamp(ConstantOperator arg) throws AnalysisException {
        return arg;
    }

    @ConstantFunction(name = "unix_timestamp", argTypes = {}, returnType = BIGINT)
    public static ConstantOperator unixTimestampNow() {
        return unixTimestamp(now());
    }

    @ConstantFunction.List(list = {
            @ConstantFunction(name = "unix_timestamp", argTypes = {DATETIME}, returnType = BIGINT),
            @ConstantFunction(name = "unix_timestamp", argTypes = {DATE}, returnType = BIGINT)
    })
    public static ConstantOperator unixTimestamp(ConstantOperator arg) {
        LocalDateTime dt = arg.getDatetime();
        ZonedDateTime zdt = ZonedDateTime.of(dt, TimeUtils.getTimeZone().toZoneId());
        long value = zdt.toEpochSecond();
        if (value < 0 || value > TimeUtils.MAX_UNIX_TIMESTAMP) {
            value = 0;
        }
        return ConstantOperator.createBigint(value);
    }

    @ConstantFunction.List(list = {
            @ConstantFunction(name = "from_unixtime", argTypes = {INT}, returnType = VARCHAR),
            @ConstantFunction(name = "from_unixtime", argTypes = {BIGINT}, returnType = VARCHAR)
    })
    public static ConstantOperator fromUnixTime(ConstantOperator unixTime) throws AnalysisException {
        long value = 0;
        if (unixTime.getType().isInt()) {
            value = unixTime.getInt();
        } else {
            value = unixTime.getBigint();
        }
        if (value < 0 || value > TimeUtils.MAX_UNIX_TIMESTAMP) {
            throw new AnalysisException(
                    "unixtime should larger than zero and less than " + TimeUtils.MAX_UNIX_TIMESTAMP);
        }
        ConstantOperator dl = ConstantOperator.createDatetime(
                LocalDateTime.ofInstant(Instant.ofEpochSecond(value), TimeUtils.getTimeZone().toZoneId()));
        return ConstantOperator.createVarchar(dl.toString());
    }

    @ConstantFunction.List(list = {
            @ConstantFunction(name = "from_unixtime", argTypes = {INT, VARCHAR}, returnType = VARCHAR),
            @ConstantFunction(name = "from_unixtime", argTypes = {BIGINT, VARCHAR}, returnType = VARCHAR)
    })
    public static ConstantOperator fromUnixTime(ConstantOperator unixTime, ConstantOperator fmtLiteral)
            throws AnalysisException {
        long value = 0;
        if (unixTime.getType().isInt()) {
            value = unixTime.getInt();
        } else {
            value = unixTime.getBigint();
        }
        if (value < 0 || value > TimeUtils.MAX_UNIX_TIMESTAMP) {
            throw new AnalysisException(
                    "unixtime should larger than zero and less than " + TimeUtils.MAX_UNIX_TIMESTAMP);
        }
        ConstantOperator dl = ConstantOperator.createDatetime(
                LocalDateTime.ofInstant(Instant.ofEpochSecond(value), TimeUtils.getTimeZone().toZoneId()));
        return dateFormat(dl, fmtLiteral);
    }
    @ConstantFunction.List(list = {
        @ConstantFunction(name = "now", argTypes = {}, returnType = DATETIME),
        @ConstantFunction(name = "current_timestamp", argTypes = {}, returnType = DATETIME),
        @ConstantFunction(name = "localtime", argTypes = {}, returnType = DATETIME),
        @ConstantFunction(name = "localtimestamp", argTypes = {}, returnType = DATETIME)
    })
    public static ConstantOperator now() {
        ConnectContext connectContext = ConnectContext.get();
        LocalDateTime startTime = Instant.ofEpochMilli(connectContext.getStartTime())
                .atZone(TimeUtils.getTimeZone().toZoneId()).toLocalDateTime();
        return ConstantOperator.createDatetime(startTime);
    }

    @ConstantFunction(name = "now", argTypes = {INT}, returnType = DATETIME)
    public static ConstantOperator now(ConstantOperator fsp) throws AnalysisException {
        int fspVal = fsp.getInt();
        if (fspVal == 0) {
            return now();
        }
        // Although there is a check here, it will not take effect and will be forwarded to BE.
        if (fspVal < 0) {
            throw new AnalysisException("precision must be greater than 0.");
        }
        if (fspVal > 6) {
            throw new AnalysisException("Too-big precision " + fspVal + "specified for 'now'. Maximum is 6.");
        }
        // Here only the syntax is implemented for the metabase to use.
        // If you want to achieve a precise type, you need to change the BE code
        // and consider the transitivity of the FE expression.
        ConnectContext connectContext = ConnectContext.get();
        LocalDateTime startTime = Instant.ofEpochMilli(connectContext.getStartTime())
                .atZone(TimeUtils.getTimeZone().toZoneId()).toLocalDateTime();
        return ConstantOperator.createDatetime(startTime);
    }

    @ConstantFunction.List(list = {
            @ConstantFunction(name = "curdate", argTypes = {}, returnType = DATE),
            @ConstantFunction(name = "current_date", argTypes = {}, returnType = DATE)
    })
    public static ConstantOperator curDate() {
        ConnectContext connectContext = ConnectContext.get();
        LocalDateTime startTime = Instant.ofEpochMilli(connectContext.getStartTime())
                .atZone(TimeUtils.getTimeZone().toZoneId()).toLocalDateTime();
        return ConstantOperator.createDate(startTime.truncatedTo(ChronoUnit.DAYS));
    }

    @ConstantFunction(name = "utc_timestamp", argTypes = {}, returnType = DATETIME)
    public static ConstantOperator utcTimestamp() {
        // for consistency with mysql, ignore milliseconds
        LocalDateTime utcStartTime = Instant.ofEpochMilli(ConnectContext.get().getStartTime())
                .atZone(ZoneOffset.UTC).toLocalDateTime();
        return ConstantOperator.createDatetime(utcStartTime);
    }

    @ConstantFunction(name = "next_day", argTypes = {DATETIME, VARCHAR}, returnType = DATE)
    public static ConstantOperator nextDay(ConstantOperator date, ConstantOperator dow) {
        int dateDowValue = date.getDate().getDayOfWeek().getValue();
        switch (dow.getVarchar()) {
            case "Sunday":
            case "Sun":
            case "Su":
                return ConstantOperator.createDate(date.getDate().plusDays((13L - dateDowValue) % 7 + 1L));
            case "Monday":
            case "Mon":
            case "Mo":
                return ConstantOperator.createDate(date.getDate().plusDays((7L - dateDowValue) % 7 + 1L));
            case "Tuesday":
            case "Tue":
            case "Tu":
                return ConstantOperator.createDate(date.getDate().plusDays((8L - dateDowValue) % 7 + 1L));
            case "Wednesday":
            case "Wed":
            case "We":
                return ConstantOperator.createDate(date.getDate().plusDays((9L - dateDowValue) % 7 + 1L));
            case "Thursday":
            case "Thu":
            case "Th":
                return ConstantOperator.createDate(date.getDate().plusDays((10L - dateDowValue) % 7 + 1L));
            case "Friday":
            case "Fri":
            case "Fr":
                return ConstantOperator.createDate(date.getDate().plusDays((11L - dateDowValue) % 7 + 1L));
            case "Saturday":
            case "Sat":
            case "Sa":
                return ConstantOperator.createDate(date.getDate().plusDays((12L - dateDowValue) % 7 + 1L));
            default:
                throw new IllegalArgumentException(dow + " not supported in next_day dow_string");
        }
    }

    @ConstantFunction(name = "previous_day", argTypes = {DATETIME, VARCHAR}, returnType = DATE)
    public static ConstantOperator previousDay(ConstantOperator date, ConstantOperator dow) {
        int dateDowValue = date.getDate().getDayOfWeek().getValue();
        switch (dow.getVarchar()) {
            case "Sunday":
            case "Sun":
            case "Su":
                return ConstantOperator.createDate(date.getDate().minusDays((dateDowValue - 1L) % 7 + 1L));
            case "Monday":
            case "Mon":
            case "Mo":
                return ConstantOperator.createDate(date.getDate().minusDays((dateDowValue + 5L) % 7 + 1L));
            case "Tuesday":
            case "Tue":
            case "Tu":
                return ConstantOperator.createDate(date.getDate().minusDays((dateDowValue + 4L) % 7 + 1L));
            case "Wednesday":
            case "Wed":
            case "We":
                return ConstantOperator.createDate(date.getDate().minusDays((dateDowValue + 3L) % 7 + 1L));
            case "Thursday":
            case "Thu":
            case "Th":
                return ConstantOperator.createDate(date.getDate().minusDays((dateDowValue + 2L) % 7 + 1L));
            case "Friday":
            case "Fri":
            case "Fr":
                return ConstantOperator.createDate(date.getDate().minusDays((dateDowValue + 1L) % 7 + 1L));
            case "Saturday":
            case "Sat":
            case "Sa":
                return ConstantOperator.createDate(date.getDate().minusDays(dateDowValue % 7 + 1L));
            default:
                throw new IllegalArgumentException(dow + " not supported in previous_day dow_string");
        }
    }

    @ConstantFunction(name = "makedate", argTypes = {INT, INT}, returnType = DATETIME)
    public static ConstantOperator makeDate(ConstantOperator year, ConstantOperator dayOfYear) {
        if (year.isNull() || dayOfYear.isNull()) {
            return ConstantOperator.createNull(Type.DATE);
        }

        int yearInt = year.getInt();
        if (yearInt < YEAR_MIN || yearInt > YEAR_MAX) {
            return ConstantOperator.createNull(Type.DATE);
        }

        int dayOfYearInt = dayOfYear.getInt();
        if (dayOfYearInt < DAY_OF_YEAR_MIN || dayOfYearInt > DAY_OF_YEAR_MAX) {
            return ConstantOperator.createNull(Type.DATE);
        }

        LocalDate ld = LocalDate.of(yearInt, 1, 1)
                .plusDays(dayOfYearInt - 1);

        if (ld.getYear() != year.getInt()) {
            return ConstantOperator.createNull(Type.DATE);
        }

        return ConstantOperator.createDate(ld.atTime(0, 0, 0));
    }

    @ConstantFunction(name = "time_slice", argTypes = {DATETIME, INT, VARCHAR}, returnType = DATETIME)
    public static ConstantOperator timeSlice(ConstantOperator datetime, ConstantOperator interval,
                                             ConstantOperator unit) throws AnalysisException {
        return timeSlice(datetime, interval, unit, ConstantOperator.createVarchar("floor"));
    }

    @ConstantFunction(name = "time_slice", argTypes = {DATETIME, INT, VARCHAR, VARCHAR}, returnType = DATETIME)
    public static ConstantOperator timeSlice(ConstantOperator datetime, ConstantOperator interval,
                                             ConstantOperator unit, ConstantOperator boundary) throws AnalysisException {
        TemporalUnit timeUnit = TIME_SLICE_UNIT_MAPPING.get(unit.getVarchar());
        if (timeUnit == null) {
            throw new IllegalArgumentException(unit + " not supported in time_slice unit param");
        }
        boolean isEnd;
        switch (boundary.getVarchar()) {
            case "floor":
                isEnd = false;
                break;
            case "ceil":
                isEnd = true;
                break;
            default:
                throw new IllegalArgumentException(boundary + " not supported in time_slice boundary param");
        }
        long duration = TIME_SLICE_START.until(datetime.getDatetime(), timeUnit);
        if (duration < 0) {
            throw new AnalysisException("time used with time_slice can't before 0001-01-01 00:00:00");
        }
        long epoch = duration - (duration % interval.getInt());
        if (isEnd) {
            epoch += interval.getInt();
        }
        return ConstantOperator.createDatetime(TIME_SLICE_START.plus(epoch, timeUnit));
    }

    /**
     * Math function
     */

    @ConstantFunction(name = "floor", argTypes = {DOUBLE}, returnType = BIGINT)
    public static ConstantOperator floor(ConstantOperator expr) {
        return ConstantOperator.createBigint((long) Math.floor(expr.getDouble()));
    }

    /**
     * Arithmetic function
     */
    @ConstantFunction(name = "add", argTypes = {SMALLINT, SMALLINT}, returnType = SMALLINT)
    public static ConstantOperator addSmallInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createSmallInt((short) Math.addExact(first.getSmallint(), second.getSmallint()));
    }

    @ConstantFunction(name = "add", argTypes = {INT, INT}, returnType = INT)
    public static ConstantOperator addInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createInt(Math.addExact(first.getInt(), second.getInt()));
    }

    @ConstantFunction(name = "add", argTypes = {BIGINT, BIGINT}, returnType = BIGINT)
    public static ConstantOperator addBigInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createBigint(Math.addExact(first.getBigint(), second.getBigint()));
    }

    @ConstantFunction(name = "add", argTypes = {DOUBLE, DOUBLE}, returnType = DOUBLE)
    public static ConstantOperator addDouble(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createDouble(first.getDouble() + second.getDouble());
    }

    @ConstantFunction.List(list = {
            @ConstantFunction(name = "add", argTypes = {DECIMALV2, DECIMALV2}, returnType = DECIMALV2),
            @ConstantFunction(name = "add", argTypes = {DECIMAL32, DECIMAL32}, returnType = DECIMAL32),
            @ConstantFunction(name = "add", argTypes = {DECIMAL64, DECIMAL64}, returnType = DECIMAL64),
            @ConstantFunction(name = "add", argTypes = {DECIMAL128, DECIMAL128}, returnType = DECIMAL128)
    })
    public static ConstantOperator addDecimal(ConstantOperator first, ConstantOperator second) {
        return createDecimalConstant(first.getDecimal().add(second.getDecimal()));
    }

    @ConstantFunction(name = "add", argTypes = {LARGEINT, LARGEINT}, returnType = LARGEINT)
    public static ConstantOperator addLargeInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createLargeInt(first.getLargeInt().add(second.getLargeInt()));
    }

    @ConstantFunction(name = "subtract", argTypes = {SMALLINT, SMALLINT}, returnType = SMALLINT)
    public static ConstantOperator subtractSmallInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createSmallInt((short) Math.subtractExact(first.getSmallint(), second.getSmallint()));
    }

    @ConstantFunction(name = "subtract", argTypes = {INT, INT}, returnType = INT)
    public static ConstantOperator subtractInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createInt(Math.subtractExact(first.getInt(), second.getInt()));
    }

    @ConstantFunction(name = "subtract", argTypes = {BIGINT, BIGINT}, returnType = BIGINT)
    public static ConstantOperator subtractBigInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createBigint(Math.subtractExact(first.getBigint(), second.getBigint()));
    }

    @ConstantFunction(name = "subtract", argTypes = {DOUBLE, DOUBLE}, returnType = DOUBLE)
    public static ConstantOperator subtractDouble(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createDouble(first.getDouble() - second.getDouble());
    }

    @ConstantFunction.List(list = {
            @ConstantFunction(name = "subtract", argTypes = {DECIMALV2, DECIMALV2}, returnType = DECIMALV2),
            @ConstantFunction(name = "subtract", argTypes = {DECIMAL32, DECIMAL32}, returnType = DECIMAL32),
            @ConstantFunction(name = "subtract", argTypes = {DECIMAL64, DECIMAL64}, returnType = DECIMAL64),
            @ConstantFunction(name = "subtract", argTypes = {DECIMAL128, DECIMAL128}, returnType = DECIMAL128)
    })
    public static ConstantOperator subtractDecimal(ConstantOperator first, ConstantOperator second) {
        return createDecimalConstant(first.getDecimal().subtract(second.getDecimal()));
    }

    @ConstantFunction(name = "subtract", argTypes = {LARGEINT, LARGEINT}, returnType = LARGEINT)
    public static ConstantOperator subtractLargeInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createLargeInt(first.getLargeInt().subtract(second.getLargeInt()));
    }

    @ConstantFunction(name = "multiply", argTypes = {SMALLINT, SMALLINT}, returnType = SMALLINT)
    public static ConstantOperator multiplySmallInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createSmallInt((short) Math.multiplyExact(first.getSmallint(), second.getSmallint()));
    }

    @ConstantFunction(name = "multiply", argTypes = {INT, INT}, returnType = INT)
    public static ConstantOperator multiplyInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createInt(Math.multiplyExact(first.getInt(), second.getInt()));
    }

    @ConstantFunction(name = "multiply", argTypes = {BIGINT, BIGINT}, returnType = BIGINT)
    public static ConstantOperator multiplyBigInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createBigint(Math.multiplyExact(first.getBigint(), second.getBigint()));
    }

    @ConstantFunction(name = "multiply", argTypes = {DOUBLE, DOUBLE}, returnType = DOUBLE)
    public static ConstantOperator multiplyDouble(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createDouble(first.getDouble() * second.getDouble());
    }

    @ConstantFunction.List(list = {
            @ConstantFunction(name = "multiply", argTypes = {DECIMALV2, DECIMALV2}, returnType = DECIMALV2),
            @ConstantFunction(name = "multiply", argTypes = {DECIMAL32, DECIMAL32}, returnType = DECIMAL32),
            @ConstantFunction(name = "multiply", argTypes = {DECIMAL64, DECIMAL64}, returnType = DECIMAL64),
            @ConstantFunction(name = "multiply", argTypes = {DECIMAL128, DECIMAL128}, returnType = DECIMAL128)
    })
    public static ConstantOperator multiplyDecimal(ConstantOperator first, ConstantOperator second) {
        return createDecimalConstant(first.getDecimal().multiply(second.getDecimal()));
    }

    @ConstantFunction(name = "multiply", argTypes = {LARGEINT, LARGEINT}, returnType = LARGEINT)
    public static ConstantOperator multiplyLargeInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createLargeInt(first.getLargeInt().multiply(second.getLargeInt()));
    }

    @ConstantFunction(name = "divide", argTypes = {DOUBLE, DOUBLE}, returnType = DOUBLE)
    public static ConstantOperator divideDouble(ConstantOperator first, ConstantOperator second) {
        if (second.getDouble() == 0.0) {
            return ConstantOperator.createNull(Type.DOUBLE);
        }
        return ConstantOperator.createDouble(first.getDouble() / second.getDouble());
    }

    @ConstantFunction.List(list = {
            @ConstantFunction(name = "divide", argTypes = {DECIMALV2, DECIMALV2}, returnType = DECIMALV2),
            @ConstantFunction(name = "divide", argTypes = {DECIMAL32, DECIMAL32}, returnType = DECIMAL32),
            @ConstantFunction(name = "divide", argTypes = {DECIMAL64, DECIMAL64}, returnType = DECIMAL64),
            @ConstantFunction(name = "divide", argTypes = {DECIMAL128, DECIMAL128}, returnType = DECIMAL128)
    })
    public static ConstantOperator divideDecimal(ConstantOperator first, ConstantOperator second) {
        if (BigDecimal.ZERO.compareTo(second.getDecimal()) == 0) {
            return ConstantOperator.createNull(second.getType());
        }
        return createDecimalConstant(first.getDecimal().divide(second.getDecimal()));
    }

    @ConstantFunction(name = "int_divide", argTypes = {TINYINT, TINYINT}, returnType = TINYINT)
    public static ConstantOperator intDivideTinyInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createTinyInt((byte) (first.getTinyInt() / second.getTinyInt()));
    }

    @ConstantFunction(name = "int_divide", argTypes = {SMALLINT, SMALLINT}, returnType = SMALLINT)
    public static ConstantOperator intDivideSmallInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createSmallInt((short) (first.getSmallint() / second.getSmallint()));
    }

    @ConstantFunction(name = "int_divide", argTypes = {INT, INT}, returnType = INT)
    public static ConstantOperator intDivideInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createInt(first.getInt() / second.getInt());
    }

    @ConstantFunction(name = "int_divide", argTypes = {BIGINT, BIGINT}, returnType = BIGINT)
    public static ConstantOperator intDivideBigint(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createBigint(first.getBigint() / second.getBigint());
    }

    @ConstantFunction(name = "int_divide", argTypes = {LARGEINT, LARGEINT}, returnType = LARGEINT)
    public static ConstantOperator intDivideLargeInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createLargeInt(first.getLargeInt().divide(second.getLargeInt()));
    }

    @ConstantFunction(name = "mod", argTypes = {TINYINT, TINYINT}, returnType = TINYINT)
    public static ConstantOperator modTinyInt(ConstantOperator first, ConstantOperator second) {
        if (second.getTinyInt() == 0) {
            return ConstantOperator.createNull(Type.TINYINT);
        }
        return ConstantOperator.createTinyInt((byte) (first.getTinyInt() % second.getTinyInt()));
    }

    @ConstantFunction(name = "mod", argTypes = {SMALLINT, SMALLINT}, returnType = SMALLINT)
    public static ConstantOperator modSMALLINT(ConstantOperator first, ConstantOperator second) {
        if (second.getSmallint() == 0) {
            return ConstantOperator.createNull(Type.SMALLINT);
        }
        return ConstantOperator.createSmallInt((short) (first.getSmallint() % second.getSmallint()));
    }

    @ConstantFunction(name = "mod", argTypes = {INT, INT}, returnType = INT)
    public static ConstantOperator modInt(ConstantOperator first, ConstantOperator second) {
        if (second.getInt() == 0) {
            return ConstantOperator.createNull(Type.INT);
        }
        return ConstantOperator.createInt(first.getInt() % second.getInt());
    }

    @ConstantFunction(name = "mod", argTypes = {BIGINT, BIGINT}, returnType = BIGINT)
    public static ConstantOperator modBigInt(ConstantOperator first, ConstantOperator second) {
        if (second.getBigint() == 0) {
            return ConstantOperator.createNull(Type.BIGINT);
        }
        return ConstantOperator.createBigint(first.getBigint() % second.getBigint());
    }

    @ConstantFunction(name = "mod", argTypes = {LARGEINT, LARGEINT}, returnType = LARGEINT)
    public static ConstantOperator modLargeInt(ConstantOperator first, ConstantOperator second) {
        if (second.getLargeInt().equals(new BigInteger("0"))) {
            return ConstantOperator.createNull(Type.LARGEINT);
        }
        return ConstantOperator.createLargeInt(first.getLargeInt().remainder(second.getLargeInt()));
    }

    @ConstantFunction.List(list = {
            @ConstantFunction(name = "mod", argTypes = {DECIMALV2, DECIMALV2}, returnType = DECIMALV2),
            @ConstantFunction(name = "mod", argTypes = {DECIMAL32, DECIMAL32}, returnType = DECIMAL32),
            @ConstantFunction(name = "mod", argTypes = {DECIMAL64, DECIMAL64}, returnType = DECIMAL64),
            @ConstantFunction(name = "mod", argTypes = {DECIMAL128, DECIMAL128}, returnType = DECIMAL128)
    })
    public static ConstantOperator modDecimal(ConstantOperator first, ConstantOperator second) {
        if (BigDecimal.ZERO.compareTo(second.getDecimal()) == 0) {
            return ConstantOperator.createNull(first.getType());
        }

        return createDecimalConstant(first.getDecimal().remainder(second.getDecimal()));
    }

    /**
     * Bitwise operation function
     */
    @ConstantFunction(name = "bitand", argTypes = {TINYINT, TINYINT}, returnType = TINYINT)
    public static ConstantOperator bitandTinyInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createTinyInt((byte) (first.getTinyInt() & second.getTinyInt()));
    }

    @ConstantFunction(name = "bitand", argTypes = {SMALLINT, SMALLINT}, returnType = SMALLINT)
    public static ConstantOperator bitandSmallInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createSmallInt((short) (first.getSmallint() & second.getSmallint()));
    }

    @ConstantFunction(name = "bitand", argTypes = {INT, INT}, returnType = INT)
    public static ConstantOperator bitandInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createInt(first.getInt() & second.getInt());
    }

    @ConstantFunction(name = "bitand", argTypes = {BIGINT, BIGINT}, returnType = BIGINT)
    public static ConstantOperator bitandBigint(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createBigint(first.getBigint() & second.getBigint());
    }

    @ConstantFunction(name = "bitand", argTypes = {LARGEINT, LARGEINT}, returnType = LARGEINT)
    public static ConstantOperator bitandLargeInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createLargeInt(first.getLargeInt().and(second.getLargeInt()));
    }

    @ConstantFunction(name = "bitor", argTypes = {TINYINT, TINYINT}, returnType = TINYINT)
    public static ConstantOperator bitorTinyInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createTinyInt((byte) (first.getTinyInt() | second.getTinyInt()));
    }

    @ConstantFunction(name = "bitor", argTypes = {SMALLINT, SMALLINT}, returnType = SMALLINT)
    public static ConstantOperator bitorSmallInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createSmallInt((short) (first.getSmallint() | second.getSmallint()));
    }

    @ConstantFunction(name = "bitor", argTypes = {INT, INT}, returnType = INT)
    public static ConstantOperator bitorInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createInt(first.getInt() | second.getInt());
    }

    @ConstantFunction(name = "bitor", argTypes = {BIGINT, BIGINT}, returnType = BIGINT)
    public static ConstantOperator bitorBigint(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createBigint(first.getBigint() | second.getBigint());
    }

    @ConstantFunction(name = "bitor", argTypes = {LARGEINT, LARGEINT}, returnType = LARGEINT)
    public static ConstantOperator bitorLargeInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createLargeInt(first.getLargeInt().or(second.getLargeInt()));
    }

    @ConstantFunction(name = "bitxor", argTypes = {TINYINT, TINYINT}, returnType = TINYINT)
    public static ConstantOperator bitxorTinyInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createTinyInt((byte) (first.getTinyInt() ^ second.getTinyInt()));
    }

    @ConstantFunction(name = "bitxor", argTypes = {SMALLINT, SMALLINT}, returnType = SMALLINT)
    public static ConstantOperator bitxorSmallInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createSmallInt((short) (first.getSmallint() ^ second.getSmallint()));
    }

    @ConstantFunction(name = "bitxor", argTypes = {INT, INT}, returnType = INT)
    public static ConstantOperator bitxorInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createInt(first.getInt() ^ second.getInt());
    }

    @ConstantFunction(name = "bitxor", argTypes = {BIGINT, BIGINT}, returnType = BIGINT)
    public static ConstantOperator bitxorBigint(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createBigint(first.getBigint() ^ second.getBigint());
    }

    @ConstantFunction(name = "bitxor", argTypes = {LARGEINT, LARGEINT}, returnType = LARGEINT)
    public static ConstantOperator bitxorLargeInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createLargeInt(first.getLargeInt().xor(second.getLargeInt()));
    }

    @ConstantFunction(name = "bitShiftLeft", argTypes = {TINYINT, BIGINT}, returnType = TINYINT)
    public static ConstantOperator bitShiftLeftTinyInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createTinyInt((byte) (first.getTinyInt() << second.getBigint()));
    }

    @ConstantFunction(name = "bitShiftLeft", argTypes = {SMALLINT, BIGINT}, returnType = SMALLINT)
    public static ConstantOperator bitShiftLeftSmallInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createSmallInt((short) (first.getSmallint() << second.getBigint()));
    }

    @ConstantFunction(name = "bitShiftLeft", argTypes = {INT, BIGINT}, returnType = INT)
    public static ConstantOperator bitShiftLeftInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createInt(first.getInt() << second.getBigint());
    }

    @ConstantFunction(name = "bitShiftLeft", argTypes = {BIGINT, BIGINT}, returnType = BIGINT)
    public static ConstantOperator bitShiftLeftBigint(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createBigint(first.getBigint() << second.getBigint());
    }

    @ConstantFunction(name = "bitShiftLeft", argTypes = {LARGEINT, BIGINT}, returnType = LARGEINT)
    public static ConstantOperator bitShiftLeftLargeInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createLargeInt(first.getLargeInt().shiftLeft((int) second.getBigint()));
    }

    @ConstantFunction(name = "bitShiftRight", argTypes = {TINYINT, BIGINT}, returnType = TINYINT)
    public static ConstantOperator bitShiftRightTinyInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createTinyInt((byte) (first.getTinyInt() >> second.getBigint()));
    }

    @ConstantFunction(name = "bitShiftRight", argTypes = {SMALLINT, BIGINT}, returnType = SMALLINT)
    public static ConstantOperator bitShiftRightSmallInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createSmallInt((short) (first.getSmallint() >> second.getBigint()));
    }

    @ConstantFunction(name = "bitShiftRight", argTypes = {INT, BIGINT}, returnType = INT)
    public static ConstantOperator bitShiftRightInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createInt(first.getInt() >> second.getBigint());
    }

    @ConstantFunction(name = "bitShiftRight", argTypes = {BIGINT, BIGINT}, returnType = BIGINT)
    public static ConstantOperator bitShiftRightBigint(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createBigint(first.getBigint() >> second.getBigint());
    }

    @ConstantFunction(name = "bitShiftRight", argTypes = {LARGEINT, BIGINT}, returnType = LARGEINT)
    public static ConstantOperator bitShiftRightLargeInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createLargeInt(first.getLargeInt().shiftRight((int) second.getBigint()));
    }

    @ConstantFunction(name = "bitShiftRightLogical", argTypes = {TINYINT, BIGINT}, returnType = TINYINT)
    public static ConstantOperator bitShiftRightLogicalTinyInt(ConstantOperator first, ConstantOperator second) {
        byte b = first.getTinyInt();
        int i = b >= 0 ? b : (((int) b) + 256);
        return ConstantOperator.createTinyInt((byte) (i >>> second.getBigint()));
    }

    @ConstantFunction(name = "bitShiftRightLogical", argTypes = {SMALLINT, BIGINT}, returnType = SMALLINT)
    public static ConstantOperator bitShiftRightLogicalSmallInt(ConstantOperator first, ConstantOperator second) {
        short s = first.getSmallint();
        int i = s >= 0 ? s : (((int) s) + 65536);
        return ConstantOperator.createSmallInt((short) (i >>> second.getBigint()));
    }

    @ConstantFunction(name = "bitShiftRightLogical", argTypes = {INT, BIGINT}, returnType = INT)
    public static ConstantOperator bitShiftRightLogicalInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createInt(first.getInt() >>> second.getBigint());
    }

    @ConstantFunction(name = "bitShiftRightLogical", argTypes = {BIGINT, BIGINT}, returnType = BIGINT)
    public static ConstantOperator bitShiftRightLogicalBigint(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createBigint(first.getBigint() >>> second.getBigint());
    }

    @ConstantFunction(name = "bitShiftRightLogical", argTypes = {LARGEINT, BIGINT}, returnType = LARGEINT)
    public static ConstantOperator bitShiftRightLogicalLargeInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createLargeInt(
                bitShiftRightLogicalForInt128(first.getLargeInt(), (int) second.getBigint()));
    }

    @ConstantFunction(name = "concat", argTypes = {VARCHAR}, returnType = VARCHAR)
    public static ConstantOperator concat(ConstantOperator... values) {
        Preconditions.checkArgument(values.length > 0);
        final StringBuilder resultBuilder = new StringBuilder();
        for (ConstantOperator value : values) {
            resultBuilder.append(value.getVarchar());
        }
        return ConstantOperator.createVarchar(resultBuilder.toString());
    }

    @ConstantFunction(name = "concat_ws", argTypes = {VARCHAR, VARCHAR}, returnType = VARCHAR)
    public static ConstantOperator concat_ws(ConstantOperator split, ConstantOperator... values) {
        Preconditions.checkArgument(values.length > 0);
        if (split.isNull()) {
            return ConstantOperator.createNull(Type.VARCHAR);
        }
        final StringBuilder resultBuilder = new StringBuilder();
        for (int i = 0; i < values.length - 1; i++) {
            if (values[i].isNull()) {
                continue;
            }
            resultBuilder.append(values[i].getVarchar()).append(split.getVarchar());
        }
        resultBuilder.append(values[values.length - 1].getVarchar());
        return ConstantOperator.createVarchar(resultBuilder.toString());
    }

    @ConstantFunction(name = "version", argTypes = {}, returnType = VARCHAR)
    public static ConstantOperator version() {
        return ConstantOperator.createVarchar(Config.mysql_server_version);
    }

    @ConstantFunction.List(list = {
            @ConstantFunction(name = "substring", argTypes = {VARCHAR, INT}, returnType = VARCHAR),
            @ConstantFunction(name = "substring", argTypes = {VARCHAR, INT, INT}, returnType = VARCHAR),
            @ConstantFunction(name = "substr", argTypes = {VARCHAR, INT}, returnType = VARCHAR),
            @ConstantFunction(name = "substr", argTypes = {VARCHAR, INT, INT}, returnType = VARCHAR)
    })
    public static ConstantOperator substring(ConstantOperator value, ConstantOperator... index) {
        Preconditions.checkArgument(index.length == 1 || index.length == 2);

        String string = value.getVarchar();
        /// If index out of bounds, the substring method will throw exception, we need avoid it,
        /// otherwise, the Constant Evaluation will fail.
        /// Besides, the implementation of `substring` function in starrocks includes beginIndex and length,
        /// and the index is start from 1 and can negative, so we need carefully handle it.
        int beginIndex = index[0].getInt() >= 0 ? index[0].getInt() - 1 : string.length() + index[0].getInt();
        int endIndex = (index.length == 2) ? Math.min(beginIndex + index[1].getInt(), string.length()) : string.length();

        if (beginIndex < 0 || beginIndex > endIndex) {
            return ConstantOperator.createVarchar("");
        }
        return ConstantOperator.createVarchar(string.substring(beginIndex, endIndex));
    }

    private static ConstantOperator createDecimalConstant(BigDecimal result) {
        Type type;
        if (!Config.enable_decimal_v3) {
            type = ScalarType.DECIMALV2;
        } else {
            int precision = DecimalLiteral.getRealPrecision(result);
            int scale = DecimalLiteral.getRealScale(result);
            type = ScalarType.createDecimalV3NarrowestType(precision, scale);
        }

        return ConstantOperator.createDecimal(result, type);
    }

    private static BigInteger bitShiftRightLogicalForInt128(BigInteger l, int shiftBy) {
        if (shiftBy <= 0) {
            return l.shiftRight(shiftBy);
        }
        if (shiftBy >= CONSTANT_128) {
            shiftBy = shiftBy & 127;
        }
        if (l.signum() >= 0) {
            return l.shiftRight(shiftBy);
        }
        BigInteger opened = l.subtract(INT_128_OPENER);
        return opened.shiftRight(shiftBy).and(INT_128_MASK1_ARR1[shiftBy]);
    }
}
