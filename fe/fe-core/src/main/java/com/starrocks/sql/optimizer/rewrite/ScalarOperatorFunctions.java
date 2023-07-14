// This file is made available under Elastic License 2.0.
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
import com.google.common.collect.ImmutableSet;
import com.starrocks.analysis.DecimalLiteral;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.rewrite.FEFunction;
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
import java.time.temporal.TemporalAdjusters;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Constant Functions List
 */
public class ScalarOperatorFunctions {
    private static final Set<String> SUPPORT_JAVA_STYLE_DATETIME_FORMATTER =
            ImmutableSet.<String>builder().add("yyyy-MM-dd").add("yyyy-MM-dd HH:mm:ss").add("yyyyMMdd").build();

    private static final Pattern HAS_TIME_PART = Pattern.compile("^.*[HhIiklrSsT]+.*$");

    /**
     * date and time function
     */
    @FEFunction(name = "timediff", argTypes = {"DATETIME", "DATETIME"}, returnType = "TIME")
    public static ConstantOperator timeDiff(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createTime(Duration.between(second.getDatetime(), first.getDatetime()).getSeconds());
    }

    @FEFunction(name = "datediff", argTypes = {"DATETIME", "DATETIME"}, returnType = "INT")
    public static ConstantOperator dateDiff(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createInt((int) Duration.between(
                second.getDatetime().truncatedTo(ChronoUnit.DAYS),
                first.getDatetime().truncatedTo(ChronoUnit.DAYS)).toDays());
    }

    @FEFunction(name = "years_add", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static ConstantOperator yearsAdd(ConstantOperator date, ConstantOperator year) {
        return ConstantOperator.createDatetime(date.getDatetime().plusYears(year.getInt()));
    }

    @FEFunction(name = "months_add", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static ConstantOperator monthsAdd(ConstantOperator date, ConstantOperator month) {
        return ConstantOperator.createDatetime(date.getDatetime().plusMonths(month.getInt()));
    }

    @FEFunction.List(list = {
            @FEFunction(name = "adddate", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME"),
            @FEFunction(name = "date_add", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME"),
            @FEFunction(name = "days_add", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    })
    public static ConstantOperator daysAdd(ConstantOperator date, ConstantOperator day) {
        return ConstantOperator.createDatetime(date.getDatetime().plusDays(day.getInt()));
    }

    @FEFunction(name = "hours_add", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static ConstantOperator hoursAdd(ConstantOperator date, ConstantOperator hour) {
        return ConstantOperator.createDatetime(date.getDatetime().plusHours(hour.getInt()));
    }

    @FEFunction(name = "minutes_add", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static ConstantOperator minutesAdd(ConstantOperator date, ConstantOperator minute) {
        return ConstantOperator.createDatetime(date.getDatetime().plusMinutes(minute.getInt()));
    }

    @FEFunction(name = "seconds_add", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static ConstantOperator secondsAdd(ConstantOperator date, ConstantOperator second) {
        return ConstantOperator.createDatetime(date.getDatetime().plusSeconds(second.getInt()));
    }

    @FEFunction(name = "date_trunc", argTypes = {"VARCHAR", "DATETIME"}, returnType = "DATETIME")
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

    @FEFunction.List(list = {
            @FEFunction(name = "date_format", argTypes = {"DATETIME", "VARCHAR"}, returnType = "VARCHAR"),
            @FEFunction(name = "date_format", argTypes = {"DATE", "VARCHAR"}, returnType = "VARCHAR")
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

    @FEFunction(name = "str_to_date", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "DATETIME")
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

    @FEFunction(name = "str2date", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "DATE")
    public static ConstantOperator str2Date(ConstantOperator date, ConstantOperator fmtLiteral) {
        DateTimeFormatterBuilder builder = DateUtils.unixDatetimeFormatBuilder(fmtLiteral.getVarchar());
        LocalDate ld = LocalDate.from(builder.toFormatter().withResolverStyle(ResolverStyle.STRICT).parse(
                StringUtils.strip(date.getVarchar(), "\r\n\t ")));
        return ConstantOperator.createDatetime(ld.atTime(0, 0, 0), Type.DATE);
    }

    @FEFunction(name = "years_sub", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static ConstantOperator yearsSub(ConstantOperator date, ConstantOperator year) {
        return ConstantOperator.createDatetime(date.getDatetime().minusYears(year.getInt()));
    }

    @FEFunction(name = "months_sub", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static ConstantOperator monthsSub(ConstantOperator date, ConstantOperator month) {
        return ConstantOperator.createDatetime(date.getDatetime().minusMonths(month.getInt()));
    }

    @FEFunction.List(list = {
            @FEFunction(name = "subdate", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME"),
            @FEFunction(name = "date_sub", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME"),
            @FEFunction(name = "days_sub", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    })
    public static ConstantOperator daysSub(ConstantOperator date, ConstantOperator day) {
        return ConstantOperator.createDatetime(date.getDatetime().minusDays(day.getInt()));
    }

    @FEFunction(name = "hours_sub", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static ConstantOperator hoursSub(ConstantOperator date, ConstantOperator hour) {
        return ConstantOperator.createDatetime(date.getDatetime().minusHours(hour.getInt()));
    }

    @FEFunction(name = "minutes_sub", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static ConstantOperator minutesSub(ConstantOperator date, ConstantOperator minute) {
        return ConstantOperator.createDatetime(date.getDatetime().minusMinutes(minute.getInt()));
    }

    @FEFunction(name = "seconds_sub", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static ConstantOperator secondsSub(ConstantOperator date, ConstantOperator second) {
        return ConstantOperator.createDatetime(date.getDatetime().minusSeconds(second.getInt()));
    }

    @FEFunction.List(list = {
            @FEFunction(name = "year", argTypes = {"DATETIME"}, returnType = "SMALLINT"),
            @FEFunction(name = "year", argTypes = {"DATE"}, returnType = "SMALLINT")
    })
    public static ConstantOperator year(ConstantOperator arg) {
        return ConstantOperator.createSmallInt((short) arg.getDatetime().getYear());
    }

    @FEFunction.List(list = {
            @FEFunction(name = "month", argTypes = {"DATETIME"}, returnType = "TINYINT"),
            @FEFunction(name = "month", argTypes = {"DATE"}, returnType = "TINYINT")
    })
    public static ConstantOperator month(ConstantOperator arg) {
        return ConstantOperator.createTinyInt((byte) arg.getDatetime().getMonthValue());
    }

    @FEFunction.List(list = {
            @FEFunction(name = "day", argTypes = {"DATETIME"}, returnType = "TINYINT"),
            @FEFunction(name = "day", argTypes = {"DATE"}, returnType = "TINYINT")
    })
    public static ConstantOperator day(ConstantOperator arg) {
        return ConstantOperator.createTinyInt((byte) arg.getDatetime().getDayOfMonth());
    }

    @FEFunction(name = "date", argTypes = {"DATETIME"}, returnType = "DATE")
    public static ConstantOperator date(ConstantOperator arg) {
        LocalDateTime datetime = LocalDateTime.of(arg.getDate().toLocalDate(), LocalTime.MIN);
        return ConstantOperator.createDate(datetime);
    }

    @FEFunction(name = "timestamp", argTypes = {"DATETIME"}, returnType = "DATETIME")
    public static ConstantOperator timestamp(ConstantOperator arg) throws AnalysisException {
        return arg;
    }

    @FEFunction(name = "unix_timestamp", argTypes = {}, returnType = "INT")
    public static ConstantOperator unixTimestampNow() {
        return unixTimestamp(now());
    }

    @FEFunction.List(list = {
            @FEFunction(name = "unix_timestamp", argTypes = {"DATETIME"}, returnType = "INT"),
            @FEFunction(name = "unix_timestamp", argTypes = {"DATE"}, returnType = "INT")
    })
    public static ConstantOperator unixTimestamp(ConstantOperator arg) {
        LocalDateTime dt = arg.getDatetime();
        ZonedDateTime zdt = ZonedDateTime.of(dt, TimeUtils.getTimeZone().toZoneId());
        if (zdt.toEpochSecond() > Integer.MAX_VALUE || zdt.toEpochSecond() < 0) {
            return ConstantOperator.createInt(0);
        }
        return ConstantOperator.createInt((int) zdt.toEpochSecond());
    }

    @FEFunction(name = "from_unixtime", argTypes = {"INT"}, returnType = "VARCHAR")
    public static ConstantOperator fromUnixTime(ConstantOperator unixTime) throws AnalysisException {
        // if unixTime < 0, we should return null, throw a exception and let BE process
        if (unixTime.getInt() < 0) {
            throw new AnalysisException("unixtime should larger than zero");
        }
        ConstantOperator dl = ConstantOperator.createDatetime(
                LocalDateTime.ofInstant(Instant.ofEpochSecond(unixTime.getInt()), TimeUtils.getTimeZone().toZoneId()));
        return ConstantOperator.createVarchar(dl.toString());
    }

    @FEFunction(name = "from_unixtime", argTypes = {"INT", "VARCHAR"}, returnType = "VARCHAR")
    public static ConstantOperator fromUnixTime(ConstantOperator unixTime, ConstantOperator fmtLiteral)
            throws AnalysisException {
        // if unixTime < 0, we should return null, throw a exception and let BE process
        if (unixTime.getInt() < 0) {
            throw new AnalysisException("unixtime should larger than zero");
        }
        ConstantOperator dl = ConstantOperator.createDatetime(
                LocalDateTime.ofInstant(Instant.ofEpochSecond(unixTime.getInt()), TimeUtils.getTimeZone().toZoneId()));
        return dateFormat(dl, fmtLiteral);
    }

    @FEFunction(name = "now", argTypes = {}, returnType = "DATETIME")
    public static ConstantOperator now() {
        ConnectContext connectContext = ConnectContext.get();
        LocalDateTime startTime = Instant.ofEpochMilli(connectContext.getStartTime())
                .atZone(TimeUtils.getTimeZone().toZoneId()).toLocalDateTime();
        return ConstantOperator.createDatetime(startTime);
    }

    @FEFunction.List(list = {
            @FEFunction(name = "curdate", argTypes = {}, returnType = "DATE"),
            @FEFunction(name = "current_date", argTypes = {}, returnType = "DATE")
    })
    public static ConstantOperator curDate() {
        ConnectContext connectContext = ConnectContext.get();
        LocalDateTime startTime = Instant.ofEpochMilli(connectContext.getStartTime())
                .atZone(TimeUtils.getTimeZone().toZoneId()).toLocalDateTime();
        return ConstantOperator.createDate(startTime.truncatedTo(ChronoUnit.DAYS));
    }

    @FEFunction(name = "utc_timestamp", argTypes = {}, returnType = "DATETIME")
    public static ConstantOperator utcTimestamp() {
        LocalDateTime utcStartTime = Instant.ofEpochMilli(ConnectContext.get().getStartTime())
                .atZone(ZoneOffset.UTC).toLocalDateTime();
        return ConstantOperator.createDatetime(utcStartTime);
    }

    /**
     * Math function
     */

    @FEFunction(name = "floor", argTypes = {"DOUBLE"}, returnType = "BIGINT")
    public static ConstantOperator floor(ConstantOperator expr) {
        return ConstantOperator.createBigint((long) Math.floor(expr.getDouble()));
    }

    /**
     * Arithmetic function
     */
    @FEFunction(name = "add", argTypes = {"SMALLINT", "SMALLINT"}, returnType = "SMALLINT")
    public static ConstantOperator addSmallInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createSmallInt((short) Math.addExact(first.getSmallint(), second.getSmallint()));
    }

    @FEFunction(name = "add", argTypes = {"INT", "INT"}, returnType = "INT")
    public static ConstantOperator addInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createInt(Math.addExact(first.getInt(), second.getInt()));
    }

    @FEFunction(name = "add", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT")
    public static ConstantOperator addBigInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createBigint(Math.addExact(first.getBigint(), second.getBigint()));
    }

    @FEFunction(name = "add", argTypes = {"DOUBLE", "DOUBLE"}, returnType = "DOUBLE")
    public static ConstantOperator addDouble(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createDouble(first.getDouble() + second.getDouble());
    }

    @FEFunction.List(list = {
            @FEFunction(name = "add", argTypes = {"DECIMALV2", "DECIMALV2"}, returnType = "DECIMALV2"),
            @FEFunction(name = "add", argTypes = {"DECIMAL32", "DECIMAL32"}, returnType = "DECIMAL32"),
            @FEFunction(name = "add", argTypes = {"DECIMAL64", "DECIMAL64"}, returnType = "DECIMAL64"),
            @FEFunction(name = "add", argTypes = {"DECIMAL128", "DECIMAL128"}, returnType = "DECIMAL128")
    })
    public static ConstantOperator addDecimal(ConstantOperator first, ConstantOperator second) {
        return createDecimalConstant(first.getDecimal().add(second.getDecimal()));
    }

    @FEFunction(name = "add", argTypes = {"LARGEINT", "LARGEINT"}, returnType = "LARGEINT")
    public static ConstantOperator addLargeInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createLargeInt(first.getLargeInt().add(second.getLargeInt()));
    }

    @FEFunction(name = "subtract", argTypes = {"SMALLINT", "SMALLINT"}, returnType = "SMALLINT")
    public static ConstantOperator subtractSmallInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createSmallInt((short) Math.subtractExact(first.getSmallint(), second.getSmallint()));
    }

    @FEFunction(name = "subtract", argTypes = {"INT", "INT"}, returnType = "INT")
    public static ConstantOperator subtractInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createInt(Math.subtractExact(first.getInt(), second.getInt()));
    }

    @FEFunction(name = "subtract", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT")
    public static ConstantOperator subtractBigInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createBigint(Math.subtractExact(first.getBigint(), second.getBigint()));
    }

    @FEFunction(name = "subtract", argTypes = {"DOUBLE", "DOUBLE"}, returnType = "DOUBLE")
    public static ConstantOperator subtractDouble(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createDouble(first.getDouble() - second.getDouble());
    }

    @FEFunction.List(list = {
            @FEFunction(name = "subtract", argTypes = {"DECIMALV2", "DECIMALV2"}, returnType = "DECIMALV2"),
            @FEFunction(name = "subtract", argTypes = {"DECIMAL32", "DECIMAL32"}, returnType = "DECIMAL32"),
            @FEFunction(name = "subtract", argTypes = {"DECIMAL64", "DECIMAL64"}, returnType = "DECIMAL64"),
            @FEFunction(name = "subtract", argTypes = {"DECIMAL128", "DECIMAL128"}, returnType = "DECIMAL128")
    })
    public static ConstantOperator subtractDecimal(ConstantOperator first, ConstantOperator second) {
        return createDecimalConstant(first.getDecimal().subtract(second.getDecimal()));
    }

    @FEFunction(name = "subtract", argTypes = {"LARGEINT", "LARGEINT"}, returnType = "LARGEINT")
    public static ConstantOperator subtractLargeInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createLargeInt(first.getLargeInt().subtract(second.getLargeInt()));
    }

    @FEFunction(name = "multiply", argTypes = {"SMALLINT", "SMALLINT"}, returnType = "SMALLINT")
    public static ConstantOperator multiplySmallInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createSmallInt((short) Math.multiplyExact(first.getSmallint(), second.getSmallint()));
    }

    @FEFunction(name = "multiply", argTypes = {"INT", "INT"}, returnType = "INT")
    public static ConstantOperator multiplyInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createInt(Math.multiplyExact(first.getInt(), second.getInt()));
    }

    @FEFunction(name = "multiply", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT")
    public static ConstantOperator multiplyBigInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createBigint(Math.multiplyExact(first.getBigint(), second.getBigint()));
    }

    @FEFunction(name = "multiply", argTypes = {"DOUBLE", "DOUBLE"}, returnType = "DOUBLE")
    public static ConstantOperator multiplyDouble(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createDouble(first.getDouble() * second.getDouble());
    }

    @FEFunction.List(list = {
            @FEFunction(name = "multiply", argTypes = {"DECIMALV2", "DECIMALV2"}, returnType = "DECIMALV2"),
            @FEFunction(name = "multiply", argTypes = {"DECIMAL32", "DECIMAL32"}, returnType = "DECIMAL32"),
            @FEFunction(name = "multiply", argTypes = {"DECIMAL64", "DECIMAL64"}, returnType = "DECIMAL64"),
            @FEFunction(name = "multiply", argTypes = {"DECIMAL128", "DECIMAL128"}, returnType = "DECIMAL128")
    })
    public static ConstantOperator multiplyDecimal(ConstantOperator first, ConstantOperator second) {
        return createDecimalConstant(first.getDecimal().multiply(second.getDecimal()));
    }

    @FEFunction(name = "multiply", argTypes = {"LARGEINT", "LARGEINT"}, returnType = "LARGEINT")
    public static ConstantOperator multiplyLargeInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createLargeInt(first.getLargeInt().multiply(second.getLargeInt()));
    }

    @FEFunction(name = "divide", argTypes = {"DOUBLE", "DOUBLE"}, returnType = "DOUBLE")
    public static ConstantOperator divideDouble(ConstantOperator first, ConstantOperator second) {
        if (second.getDouble() == 0.0) {
            return ConstantOperator.createNull(Type.DOUBLE);
        }
        return ConstantOperator.createDouble(first.getDouble() / second.getDouble());
    }

    @FEFunction.List(list = {
            @FEFunction(name = "divide", argTypes = {"DECIMALV2", "DECIMALV2"}, returnType = "DECIMALV2"),
            @FEFunction(name = "divide", argTypes = {"DECIMAL32", "DECIMAL32"}, returnType = "DECIMAL32"),
            @FEFunction(name = "divide", argTypes = {"DECIMAL64", "DECIMAL64"}, returnType = "DECIMAL64"),
            @FEFunction(name = "divide", argTypes = {"DECIMAL128", "DECIMAL128"}, returnType = "DECIMAL128")
    })
    public static ConstantOperator divideDecimal(ConstantOperator first, ConstantOperator second) {
        if (BigDecimal.ZERO.compareTo(second.getDecimal()) == 0) {
            return ConstantOperator.createNull(second.getType());
        }
        return createDecimalConstant(first.getDecimal().divide(second.getDecimal()));
    }

    @FEFunction(name = "int_divide", argTypes = {"TINYINT", "TINYINT"}, returnType = "TINYINT")
    public static ConstantOperator intDivideTinyInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createTinyInt((byte) (first.getTinyInt() / second.getTinyInt()));
    }

    @FEFunction(name = "int_divide", argTypes = {"SMALLINT", "SMALLINT"}, returnType = "SMALLINT")
    public static ConstantOperator intDivideSmallInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createSmallInt((short) (first.getSmallint() / second.getSmallint()));
    }

    @FEFunction(name = "int_divide", argTypes = {"INT", "INT"}, returnType = "INT")
    public static ConstantOperator intDivideInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createInt(first.getInt() / second.getInt());
    }

    @FEFunction(name = "int_divide", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT")
    public static ConstantOperator intDivideBigint(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createBigint(first.getBigint() / second.getBigint());
    }

    @FEFunction(name = "int_divide", argTypes = {"LARGEINT", "LARGEINT"}, returnType = "LARGEINT")
    public static ConstantOperator intDivideLargeInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createLargeInt(first.getLargeInt().divide(second.getLargeInt()));
    }

    @FEFunction(name = "mod", argTypes = {"TINYINT", "TINYINT"}, returnType = "TINYINT")
    public static ConstantOperator modTinyInt(ConstantOperator first, ConstantOperator second) {
        if (second.getTinyInt() == 0) {
            return ConstantOperator.createNull(Type.TINYINT);
        }
        return ConstantOperator.createTinyInt((byte) (first.getTinyInt() % second.getTinyInt()));
    }

    @FEFunction(name = "mod", argTypes = {"SMALLINT", "SMALLINT"}, returnType = "SMALLINT")
    public static ConstantOperator modSMALLINT(ConstantOperator first, ConstantOperator second) {
        if (second.getSmallint() == 0) {
            return ConstantOperator.createNull(Type.SMALLINT);
        }
        return ConstantOperator.createSmallInt((short) (first.getSmallint() % second.getSmallint()));
    }

    @FEFunction(name = "mod", argTypes = {"INT", "INT"}, returnType = "INT")
    public static ConstantOperator modInt(ConstantOperator first, ConstantOperator second) {
        if (second.getInt() == 0) {
            return ConstantOperator.createNull(Type.INT);
        }
        return ConstantOperator.createInt(first.getInt() % second.getInt());
    }

    @FEFunction(name = "mod", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT")
    public static ConstantOperator modBigInt(ConstantOperator first, ConstantOperator second) {
        if (second.getBigint() == 0) {
            return ConstantOperator.createNull(Type.BIGINT);
        }
        return ConstantOperator.createBigint(first.getBigint() % second.getBigint());
    }

    @FEFunction(name = "mod", argTypes = {"LARGEINT", "LARGEINT"}, returnType = "LARGEINT")
    public static ConstantOperator modLargeInt(ConstantOperator first, ConstantOperator second) {
        if (second.getLargeInt().equals(new BigInteger("0"))) {
            return ConstantOperator.createNull(Type.LARGEINT);
        }
        return ConstantOperator.createLargeInt(first.getLargeInt().remainder(second.getLargeInt()));
    }

    @FEFunction.List(list = {
            @FEFunction(name = "mod", argTypes = {"DECIMALV2", "DECIMALV2"}, returnType = "DECIMALV2"),
            @FEFunction(name = "mod", argTypes = {"DECIMAL32", "DECIMAL32"}, returnType = "DECIMAL32"),
            @FEFunction(name = "mod", argTypes = {"DECIMAL64", "DECIMAL64"}, returnType = "DECIMAL64"),
            @FEFunction(name = "mod", argTypes = {"DECIMAL128", "DECIMAL128"}, returnType = "DECIMAL128")
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
    @FEFunction(name = "bitand", argTypes = {"TINYINT", "TINYINT"}, returnType = "TINYINT")
    public static ConstantOperator bitandTinyInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createTinyInt((byte) (first.getTinyInt() & second.getTinyInt()));
    }

    @FEFunction(name = "bitand", argTypes = {"SMALLINT", "SMALLINT"}, returnType = "SMALLINT")
    public static ConstantOperator bitandSmallInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createSmallInt((short) (first.getSmallint() & second.getSmallint()));
    }

    @FEFunction(name = "bitand", argTypes = {"INT", "INT"}, returnType = "INT")
    public static ConstantOperator bitandInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createInt(first.getInt() & second.getInt());
    }

    @FEFunction(name = "bitand", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT")
    public static ConstantOperator bitandBigint(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createBigint(first.getBigint() & second.getBigint());
    }

    @FEFunction(name = "bitand", argTypes = {"LARGEINT", "LARGEINT"}, returnType = "LARGEINT")
    public static ConstantOperator bitandLargeInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createLargeInt(first.getLargeInt().and(second.getLargeInt()));
    }

    @FEFunction(name = "bitor", argTypes = {"TINYINT", "TINYINT"}, returnType = "TINYINT")
    public static ConstantOperator bitorTinyInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createTinyInt((byte) (first.getTinyInt() | second.getTinyInt()));
    }

    @FEFunction(name = "bitor", argTypes = {"SMALLINT", "SMALLINT"}, returnType = "SMALLINT")
    public static ConstantOperator bitorSmallInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createSmallInt((short) (first.getSmallint() | second.getSmallint()));
    }

    @FEFunction(name = "bitor", argTypes = {"INT", "INT"}, returnType = "INT")
    public static ConstantOperator bitorInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createInt(first.getInt() | second.getInt());
    }

    @FEFunction(name = "bitor", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT")
    public static ConstantOperator bitorBigint(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createBigint(first.getBigint() | second.getBigint());
    }

    @FEFunction(name = "bitor", argTypes = {"LARGEINT", "LARGEINT"}, returnType = "LARGEINT")
    public static ConstantOperator bitorLargeInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createLargeInt(first.getLargeInt().or(second.getLargeInt()));
    }

    @FEFunction(name = "bitxor", argTypes = {"TINYINT", "TINYINT"}, returnType = "TINYINT")
    public static ConstantOperator bitxorTinyInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createTinyInt((byte) (first.getTinyInt() ^ second.getTinyInt()));
    }

    @FEFunction(name = "bitxor", argTypes = {"SMALLINT", "SMALLINT"}, returnType = "SMALLINT")
    public static ConstantOperator bitxorSmallInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createSmallInt((short) (first.getSmallint() ^ second.getSmallint()));
    }

    @FEFunction(name = "bitxor", argTypes = {"INT", "INT"}, returnType = "INT")
    public static ConstantOperator bitxorInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createInt(first.getInt() ^ second.getInt());
    }

    @FEFunction(name = "bitxor", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT")
    public static ConstantOperator bitxorBigint(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createBigint(first.getBigint() ^ second.getBigint());
    }

    @FEFunction(name = "bitxor", argTypes = {"LARGEINT", "LARGEINT"}, returnType = "LARGEINT")
    public static ConstantOperator bitxorLargeInt(ConstantOperator first, ConstantOperator second) {
        return ConstantOperator.createLargeInt(first.getLargeInt().xor(second.getLargeInt()));
    }

    @FEFunction(name = "concat", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static ConstantOperator concat(ConstantOperator... values) {
        Preconditions.checkArgument(values.length > 0);
        final StringBuilder resultBuilder = new StringBuilder();
        for (ConstantOperator value : values) {
            resultBuilder.append(value.getVarchar());
        }
        return ConstantOperator.createVarchar(resultBuilder.toString());
    }

    @FEFunction(name = "concat_ws", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "VARCHAR")
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

    private static ConstantOperator createDecimalConstant(BigDecimal result) {
        Type type;
        if (!Config.enable_decimal_v3) {
            type = ScalarType.DECIMALV2;
        } else {
            result = result.stripTrailingZeros();
            int precision = DecimalLiteral.getRealPrecision(result);
            int scale = DecimalLiteral.getRealScale(result);
            type = ScalarType.createDecimalV3NarrowestType(precision, scale);
        }

        return ConstantOperator.createDecimal(result, type);
    }
}
