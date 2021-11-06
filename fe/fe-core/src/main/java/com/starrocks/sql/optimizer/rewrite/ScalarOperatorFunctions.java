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
import com.starrocks.analysis.DateLiteral;
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
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Set;

/**
 * Constant Functions List
 */
public class ScalarOperatorFunctions {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final Set<String> SUPPORT_DATETIME_FORMATTER =
            ImmutableSet.<String>builder().add("yyyy-MM-dd").add("yyyy-MM-dd HH:mm:ss").add("yyyyMMdd").build();

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
        try {
            // unix style
            if (!SUPPORT_DATETIME_FORMATTER.contains(format.trim())) {
                DateLiteral literal = new DateLiteral(date.getDatetime().format(DATE_TIME_FORMATTER), Type.DATETIME);
                literal.setType(date.getType());
                return ConstantOperator.createVarchar(literal.dateFormat(fmtLiteral.getVarchar()));
            } else {
                String result = date.getDatetime().format(DateTimeFormatter.ofPattern(fmtLiteral.getVarchar()));
                return ConstantOperator.createVarchar(result);
            }
        } catch (Exception e) {
            return fmtLiteral;
        }
    }

    @FEFunction(name = "str_to_date", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "DATETIME")
    public static ConstantOperator dateParse(ConstantOperator date, ConstantOperator fmtLiteral)
            throws AnalysisException {
        DateLiteral literal = DateLiteral.dateParser(date.getVarchar(), fmtLiteral.getVarchar());
        return ConstantOperator.createDatetime(literal.toLocalDateTime(), literal.getType());
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
        DateLiteral dl = new DateLiteral(((long) unixTime.getInt()) * 1000, TimeUtils.getTimeZone(), Type.DATETIME);
        return ConstantOperator.createVarchar(dl.getStringValue());
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
        return createDecimalLiteral(first.getDecimal().add(second.getDecimal()));
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
        return createDecimalLiteral(first.getDecimal().subtract(second.getDecimal()));
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
        return createDecimalLiteral(first.getDecimal().multiply(second.getDecimal()));
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
        return createDecimalLiteral(first.getDecimal().divide(second.getDecimal()));
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

        return createDecimalLiteral(first.getDecimal().remainder(second.getDecimal()));
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
        final StringBuilder resultBuilder = new StringBuilder();
        for (int i = 0; i < values.length - 1; i++) {
            resultBuilder.append(values[i].getVarchar()).append(split.getVarchar());
        }
        resultBuilder.append(values[values.length - 1].getVarchar());
        return ConstantOperator.createVarchar(resultBuilder.toString());
    }

    private static ConstantOperator createDecimalLiteral(BigDecimal result) {
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
