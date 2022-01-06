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
import com.starrocks.common.util.TimeUtils;
import com.starrocks.rewrite.FEFunction;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
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

    @FEFunction.List(list = {
            @FEFunction(name = "date_format", argTypes = {"DATETIME", "VARCHAR"}, returnType = "VARCHAR"),
            @FEFunction(name = "date_format", argTypes = {"DATE", "VARCHAR"}, returnType = "VARCHAR")
    })
    public static ConstantOperator dateFormat(ConstantOperator date, ConstantOperator fmtLiteral) {
        String format = fmtLiteral.getVarchar();
        // unix style
        if (!SUPPORT_JAVA_STYLE_DATETIME_FORMATTER.contains(format.trim())) {
            DateTimeFormatterBuilder builder = unixDatetimeFormatBuilder(fmtLiteral.getVarchar());
            return ConstantOperator.createVarchar(builder.toFormatter().format(date.getDatetime()));
        } else {
            String result = date.getDatetime().format(DateTimeFormatter.ofPattern(fmtLiteral.getVarchar()));
            return ConstantOperator.createVarchar(result);
        }
    }

    @FEFunction(name = "str_to_date", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "DATETIME")
    public static ConstantOperator dateParse(ConstantOperator date, ConstantOperator fmtLiteral) {
        DateTimeFormatterBuilder builder = unixDatetimeFormatBuilder(fmtLiteral.getVarchar());

        if (HAS_TIME_PART.matcher(fmtLiteral.getVarchar()).matches()) {
            LocalDateTime ldt = LocalDateTime.from(builder.toFormatter().parse(date.getVarchar()));
            return ConstantOperator.createDatetime(ldt, Type.DATETIME);
        } else {
            LocalDate ld = LocalDate.from(builder.toFormatter().parse(date.getVarchar()));
            return ConstantOperator.createDatetime(ld.atTime(0, 0, 0), Type.DATETIME);
        }
    }

    @FEFunction(name = "str2date", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "DATE")
    public static ConstantOperator str2Date(ConstantOperator date, ConstantOperator fmtLiteral) {
        DateTimeFormatterBuilder builder = unixDatetimeFormatBuilder(fmtLiteral.getVarchar());
        LocalDate ld = LocalDate.from(builder.toFormatter().parse(date.getVarchar()));
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

    @FEFunction(name = "year", argTypes = {"DATETIME"}, returnType = "INT")
    public static ConstantOperator year(ConstantOperator arg) {
        return ConstantOperator.createInt(arg.getDatetime().getYear());
    }

    @FEFunction(name = "month", argTypes = {"DATETIME"}, returnType = "INT")
    public static ConstantOperator month(ConstantOperator arg) {
        return ConstantOperator.createInt(arg.getDatetime().getMonthValue());
    }

    @FEFunction(name = "day", argTypes = {"DATETIME"}, returnType = "INT")
    public static ConstantOperator day(ConstantOperator arg) {
        return ConstantOperator.createInt(arg.getDatetime().getDayOfMonth());
    }

    @FEFunction(name = "date", argTypes = {"DATETIME"}, returnType = "DATE")
    public static ConstantOperator date(ConstantOperator arg) {
        LocalDateTime datetime = LocalDateTime.of(arg.getDate().toLocalDate(), LocalTime.MIN);
        return ConstantOperator.createDate(datetime);
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
        return ConstantOperator.createDatetime(LocalDateTime.now());
    }

    @FEFunction.List(list = {
            @FEFunction(name = "curdate", argTypes = {}, returnType = "DATE"),
            @FEFunction(name = "current_date", argTypes = {}, returnType = "DATE")
    })
    public static ConstantOperator curDate() {
        return ConstantOperator.createDate(LocalDateTime.now().truncatedTo(ChronoUnit.DAYS));
    }

    @FEFunction(name = "utc_timestamp", argTypes = {}, returnType = "DATETIME")
    public static ConstantOperator utcTimestamp() {
        return ConstantOperator.createDatetime(LocalDateTime.now(ZoneOffset.UTC));
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

    @FEFunction(name = "mod", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT")
    public static ConstantOperator modBigInt(ConstantOperator first, ConstantOperator second) {
        if (second.getBigint() == 0) {
            return ConstantOperator.createNull(Type.BIGINT);
        }
        return ConstantOperator.createBigint(first.getBigint() % second.getBigint());
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

    @FEFunction(name = "mod", argTypes = {"LARGEINT", "LARGEINT"}, returnType = "LARGEINT")
    public static ConstantOperator modLargeInt(ConstantOperator first, ConstantOperator second) {
        if (second.getLargeInt().compareTo(BigInteger.ZERO) == 0) {
            return ConstantOperator.createNull(Type.LARGEINT);
        }
        return ConstantOperator.createLargeInt(first.getLargeInt().mod(second.getLargeInt()));
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

    private static DateTimeFormatterBuilder unixDatetimeFormatBuilder(String pattern) {
        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
        boolean escaped = false;
        for (int i = 0; i < pattern.length(); i++) {
            char character = pattern.charAt(i);
            if (escaped) {
                switch (character) {
                    case 'c': // %c Month, numeric (0..12)
                        builder.appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL);
                        break;
                    case 'm': // %m Month, numeric (00..12)
                        builder.appendValue(ChronoField.MONTH_OF_YEAR, 2);
                        break;
                    case 'd': // %d Day of the month, numeric (00..31)
                        builder.appendValue(ChronoField.DAY_OF_MONTH, 2);
                        break;
                    case 'e': // %e Day of the month, numeric (0..31)
                        builder.appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NORMAL);
                        break;
                    case 'H': // %H Hour (00..23)
                        builder.appendValue(ChronoField.HOUR_OF_DAY, 2);
                        break;
                    case 'k': // %k Hour (0..23)
                        builder.appendValue(ChronoField.HOUR_OF_DAY, 1, 2, SignStyle.NORMAL);
                        break;
                    case 'h': // %h Hour (01..12)
                    case 'I': // %I Hour (01..12)
                        builder.appendValue(ChronoField.CLOCK_HOUR_OF_AMPM, 2);
                        break;
                    case 'l': // %l Hour (1..12)
                        builder.appendValue(ChronoField.CLOCK_HOUR_OF_AMPM, 1, 2, SignStyle.NORMAL);
                        break;
                    case 'i': // %i Minutes, numeric (00..59)
                        builder.appendValue(ChronoField.MINUTE_OF_HOUR, 2);
                        break;
                    case 'j': // %j Day of year (001..366)
                        builder.appendValue(ChronoField.DAY_OF_YEAR, 3);
                        break;
                    case 'p': // %p AM or PM
                        builder.appendText(ChronoField.AMPM_OF_DAY, TextStyle.FULL);
                        break;
                    case 'r': // %r Time, 12-hour (hh:mm:ss followed by AM or PM)
                        builder.appendValue(ChronoField.CLOCK_HOUR_OF_AMPM, 2)
                                .appendLiteral(':')
                                .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
                                .appendLiteral(':')
                                .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
                                .appendLiteral(' ')
                                .appendText(ChronoField.AMPM_OF_DAY, TextStyle.FULL);
                        break;
                    case 'S': // %S Seconds (00..59)
                    case 's': // %s Seconds (00..59)
                        builder.appendValue(ChronoField.SECOND_OF_MINUTE, 2);
                        break;
                    case 'T': // %T Time, 24-hour (hh:mm:ss)
                        builder.appendValue(ChronoField.HOUR_OF_DAY, 2)
                                .appendLiteral(':')
                                .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
                                .appendLiteral(':')
                                .appendValue(ChronoField.SECOND_OF_MINUTE, 2);
                        break;
                    case 'v': // %v Week (01..53), where Monday is the first day of the week; used with %x
                        builder.appendValue(ChronoField.ALIGNED_WEEK_OF_YEAR, 2);
                        break;
                    case 'Y': // %Y Year, numeric, four digits
                        builder.appendValue(ChronoField.YEAR, 4);
                        break;
                    case 'y': // %y Year, numeric (two digits)
                        builder.appendValueReduced(ChronoField.YEAR_OF_ERA, 2, 2, 2020);
                        break;
                    case 'w': // %w Day of the week (0=Sunday..6=Saturday)
                        builder.appendValue(ChronoField.DAY_OF_WEEK, 1);
                        break;
                    case 'f': // %f Microseconds (000000..999999)
                        builder.appendValue(ChronoField.MICRO_OF_SECOND, 6);
                        break;
                    case 'u': // %u Week (00..53), where Monday is the first day of the week
                        builder.appendValueReduced(ChronoField.ALIGNED_WEEK_OF_YEAR, 2, 2, 0);
                        break;
                    case 'U': // %U Week (00..53), where Sunday is the first day of the week
                    case 'W': // %W Weekday name (Sunday..Saturday)
                    case 'x': // %x Year for the week, where Monday is the first day of the week, numeric, four digits; used with %v
                    case 'M': // %M Month name (January..December)
                    case 'a': // %a Abbreviated weekday name (Sun..Sat)
                    case 'b': // %b Abbreviated month name (Jan..Dec)
                    case 'V': // %V Week (01..53), where Sunday is the first day of the week; used with %X
                    case 'X': // %X Year for the week where Sunday is the first day of the week, numeric, four digits; used with %V
                    case 'D': // %D Day of the month with English suffix (0th, 1st, 2nd, 3rd, ...)
                        throw new IllegalArgumentException(
                                String.format("%%%s not supported in date format string", character));
                    case '%': // %% A literal "%" character
                        builder.appendLiteral('%');
                        break;
                    default: // %<x> The literal character represented by <x>
                        builder.appendLiteral(character);
                        break;
                }
                escaped = false;
            } else if (character == '%') {
                escaped = true;
            } else {
                builder.appendLiteral(character);
            }
        }
        return builder;
    }
}
