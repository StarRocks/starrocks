// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/rewrite/FEFunctions.java

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

package com.starrocks.rewrite;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.DecimalLiteral;
import com.starrocks.analysis.FloatLiteral;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LargeIntLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.TimeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * compute functions in FE.
 * <p>
 * when you add a new function, please ensure the name, argTypes , returnType and compute logic are consistent with BE's function
 */
// Our new cost based query optimizer is more powerful and stable than old query optimizer,
// The old query optimizer related codes could be deleted safely.
// TODO: Remove old query optimizer related codes before 2021-09-30
@Deprecated
public class FEFunctions {
    private static final Logger LOG = LogManager.getLogger(FEFunctions.class);

    /**
     * date and time function
     */
    @FEFunction(name = "timediff", argTypes = {"DATETIME", "DATETIME"}, returnType = "TIME")
    public static FloatLiteral timeDiff(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        long firstTimestamp = ((DateLiteral) first).unixTimestamp(TimeUtils.getTimeZone());
        long secondTimestamp = ((DateLiteral) second).unixTimestamp(TimeUtils.getTimeZone());
        return new FloatLiteral((double) (firstTimestamp - secondTimestamp) / 1000, Type.TIME);
    }

    @FEFunction(name = "datediff", argTypes = {"DATETIME", "DATETIME"}, returnType = "INT")
    public static IntLiteral dateDiff(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        DateLiteral firstDate = ((DateLiteral) first);
        DateLiteral secondDate = ((DateLiteral) second);
        // DATEDIFF function only uses the date part for calculations and ignores the time part
        firstDate.castToDate();
        secondDate.castToDate();
        long datediff =
                (firstDate.unixTimestamp(TimeUtils.getTimeZone()) - secondDate.unixTimestamp(TimeUtils.getTimeZone())) /
                        1000 / 60 / 60 / 24;
        return new IntLiteral(datediff, Type.INT);
    }

    @FEFunction(name = "date_add", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static DateLiteral dateAdd(LiteralExpr date, LiteralExpr day) throws AnalysisException {
        return daysAdd(date, day);
    }

    @FEFunction(name = "adddate", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static DateLiteral addDate(LiteralExpr date, LiteralExpr day) throws AnalysisException {
        return daysAdd(date, day);
    }

    @FEFunction(name = "years_add", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static DateLiteral yearsAdd(LiteralExpr date, LiteralExpr year) throws AnalysisException {
        DateLiteral dateLiteral = (DateLiteral) date;
        return dateLiteral.plusYears((int) year.getLongValue());
    }

    @FEFunction(name = "months_add", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static DateLiteral monthsAdd(LiteralExpr date, LiteralExpr month) throws AnalysisException {
        DateLiteral dateLiteral = (DateLiteral) date;
        return dateLiteral.plusMonths((int) month.getLongValue());
    }

    @FEFunction(name = "days_add", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static DateLiteral daysAdd(LiteralExpr date, LiteralExpr day) throws AnalysisException {
        DateLiteral dateLiteral = (DateLiteral) date;
        return dateLiteral.plusDays((int) day.getLongValue());
    }

    @FEFunction(name = "hours_add", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static DateLiteral hoursAdd(LiteralExpr date, LiteralExpr hour) throws AnalysisException {
        DateLiteral dateLiteral = (DateLiteral) date;
        return dateLiteral.plusHours((int) hour.getLongValue());
    }

    @FEFunction(name = "minutes_add", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static DateLiteral minutesAdd(LiteralExpr date, LiteralExpr minute) throws AnalysisException {
        DateLiteral dateLiteral = (DateLiteral) date;
        return dateLiteral.plusMinutes((int) minute.getLongValue());
    }

    @FEFunction(name = "seconds_add", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static DateLiteral secondsAdd(LiteralExpr date, LiteralExpr second) throws AnalysisException {
        DateLiteral dateLiteral = (DateLiteral) date;
        return dateLiteral.plusSeconds((int) second.getLongValue());
    }

    @FEFunction.List(list = {
            @FEFunction(name = "date_format", argTypes = {"DATETIME", "VARCHAR"}, returnType = "VARCHAR"),
            @FEFunction(name = "date_format", argTypes = {"DATE", "VARCHAR"}, returnType = "VARCHAR")
    })
    public static StringLiteral dateFormat(LiteralExpr date, StringLiteral fmtLiteral) throws AnalysisException {
        String result = ((DateLiteral) date).dateFormat(fmtLiteral.getStringValue());
        return new StringLiteral(result);
    }

    @FEFunction(name = "str_to_date", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "DATETIME")
    public static DateLiteral dateParse(StringLiteral date, StringLiteral fmtLiteral) throws AnalysisException {
        return DateLiteral.dateParser(date.getStringValue(), fmtLiteral.getStringValue());
    }

    @FEFunction(name = "date_sub", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static DateLiteral dateSub(LiteralExpr date, LiteralExpr day) throws AnalysisException {
        return dateAdd(date, new IntLiteral(-(int) day.getLongValue()));
    }

    @FEFunction(name = "years_sub", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static DateLiteral yearsSub(LiteralExpr date, LiteralExpr year) throws AnalysisException {
        return yearsAdd(date, new IntLiteral(-(int) year.getLongValue()));
    }

    @FEFunction(name = "months_sub", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static DateLiteral monthsSub(LiteralExpr date, LiteralExpr month) throws AnalysisException {
        return monthsAdd(date, new IntLiteral(-(int) month.getLongValue()));
    }

    @FEFunction(name = "days_sub", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static DateLiteral daysSub(LiteralExpr date, LiteralExpr day) throws AnalysisException {
        return daysAdd(date, new IntLiteral(-(int) day.getLongValue()));
    }

    @FEFunction(name = "hours_sub", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static DateLiteral hoursSub(LiteralExpr date, LiteralExpr hour) throws AnalysisException {
        return hoursAdd(date, new IntLiteral(-(int) hour.getLongValue()));
    }

    @FEFunction(name = "minutes_sub", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static DateLiteral minutesSub(LiteralExpr date, LiteralExpr minute) throws AnalysisException {
        return minutesAdd(date, new IntLiteral(-(int) minute.getLongValue()));
    }

    @FEFunction(name = "seconds_sub", argTypes = {"DATETIME", "INT"}, returnType = "DATETIME")
    public static DateLiteral secondsSub(LiteralExpr date, LiteralExpr second) throws AnalysisException {
        return secondsAdd(date, new IntLiteral(-(int) second.getLongValue()));
    }

    @FEFunction(name = "year", argTypes = {"DATETIME"}, returnType = "INT")
    public static IntLiteral year(LiteralExpr arg) throws AnalysisException {
        return new IntLiteral(((DateLiteral) arg).getYear(), Type.INT);
    }

    @FEFunction(name = "month", argTypes = {"DATETIME"}, returnType = "INT")
    public static IntLiteral month(LiteralExpr arg) throws AnalysisException {
        return new IntLiteral(((DateLiteral) arg).getMonth(), Type.INT);
    }

    @FEFunction(name = "day", argTypes = {"DATETIME"}, returnType = "INT")
    public static IntLiteral day(LiteralExpr arg) throws AnalysisException {
        return new IntLiteral(((DateLiteral) arg).getDay(), Type.INT);
    }

    @FEFunction(name = "unix_timestamp", argTypes = {"DATETIME"}, returnType = "INT")
    public static IntLiteral unixTimestamp(LiteralExpr arg) throws AnalysisException {
        long unixTime = ((DateLiteral) arg).unixTimestamp(TimeUtils.getTimeZone()) / 1000;
        // date before 1970-01-01 or after 2038-01-19 03:14:07 should return 0 for unix_timestamp() function
        unixTime = unixTime < 0 ? 0 : unixTime;
        unixTime = unixTime > Integer.MAX_VALUE ? 0 : unixTime;
        return new IntLiteral(unixTime, Type.INT);
    }

    @FEFunction(name = "unix_timestamp", argTypes = {"DATE"}, returnType = "INT")
    public static IntLiteral unixTimestamp2(LiteralExpr arg) throws AnalysisException {
        long unixTime = ((DateLiteral) arg).unixTimestamp(TimeUtils.getTimeZone()) / 1000;
        // date before 1970-01-01 or after 2038-01-19 03:14:07 should return 0 for unix_timestamp() function
        unixTime = unixTime < 0 ? 0 : unixTime;
        unixTime = unixTime > Integer.MAX_VALUE ? 0 : unixTime;
        return new IntLiteral(unixTime, Type.INT);
    }

    @FEFunction(name = "from_unixtime", argTypes = {"INT"}, returnType = "VARCHAR")
    public static StringLiteral fromUnixTime(LiteralExpr unixTime) throws AnalysisException {
        // if unixTime < 0, we should return null, throw a exception and let BE process
        if (unixTime.getLongValue() < 0) {
            throw new AnalysisException("unixtime should larger than zero");
        }
        DateLiteral dl = new DateLiteral(unixTime.getLongValue() * 1000, TimeUtils.getTimeZone(), Type.DATETIME);
        return new StringLiteral(dl.getStringValue());
    }

    @FEFunction(name = "from_unixtime", argTypes = {"INT", "VARCHAR"}, returnType = "VARCHAR")
    public static StringLiteral fromUnixTime(LiteralExpr unixTime, StringLiteral fmtLiteral) throws AnalysisException {
        // if unixTime < 0, we should return null, throw a exception and let BE process
        if (unixTime.getLongValue() < 0) {
            throw new AnalysisException("unixtime should larger than zero");
        }
        DateLiteral dl = new DateLiteral(unixTime.getLongValue() * 1000, TimeUtils.getTimeZone(), Type.DATETIME);
        return new StringLiteral(dl.dateFormat(fmtLiteral.getStringValue()));
    }

    @FEFunction(name = "now", argTypes = {}, returnType = "DATETIME")
    public static DateLiteral now() throws AnalysisException {
        return new DateLiteral(LocalDateTime.now(DateTimeZone.forTimeZone(TimeUtils.getTimeZone())), Type.DATETIME);
    }

    @FEFunction.List(list = {
            @FEFunction(name = "curdate", argTypes = {}, returnType = "DATE"),
            @FEFunction(name = "current_date", argTypes = {}, returnType = "DATE")
    })
    public static DateLiteral curDate() throws AnalysisException {
        DateLiteral dateLiteral =
                new DateLiteral(LocalDateTime.now(DateTimeZone.forTimeZone(TimeUtils.getTimeZone())), Type.DATE);
        dateLiteral.castToDate();
        return dateLiteral;
    }

    @FEFunction(name = "utc_timestamp", argTypes = {}, returnType = "DATETIME")
    public static DateLiteral utcTimestamp() throws AnalysisException {
        return new DateLiteral(LocalDateTime.now(DateTimeZone.forTimeZone(TimeUtils.getOrSystemTimeZone("+00:00"))),
                Type.DATETIME);
    }

    /*
     ------------------------------------------------------------------------------
     */

    /**
     * Math function
     */

    @FEFunction(name = "floor", argTypes = {"DOUBLE"}, returnType = "BIGINT")
    public static IntLiteral floor(LiteralExpr expr) throws AnalysisException {
        long result = (long) Math.floor(expr.getDoubleValue());
        return new IntLiteral(result, Type.BIGINT);
    }

    /*
     ------------------------------------------------------------------------------
     */

    /**
     * Arithmetic function
     */

    @FEFunction(name = "add", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT")
    public static IntLiteral addInt(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        long result = Math.addExact(first.getLongValue(), second.getLongValue());
        return new IntLiteral(result, Type.BIGINT);
    }

    @FEFunction(name = "add", argTypes = {"DOUBLE", "DOUBLE"}, returnType = "DOUBLE")
    public static FloatLiteral addDouble(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        double result = first.getDoubleValue() + second.getDoubleValue();
        return new FloatLiteral(result, Type.DOUBLE);
    }

    private static DecimalLiteral addDecimalImpl(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        BigDecimal left = new BigDecimal(first.getStringValue());
        BigDecimal right = new BigDecimal(second.getStringValue());
        BigDecimal result = left.add(right);
        return new DecimalLiteral(result);
    }

    @FEFunction(name = "add", argTypes = {"DECIMAL", "DECIMAL"}, returnType = "DECIMAL")
    public static DecimalLiteral addDecimal(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return addDecimalImpl(first, second);
    }

    @FEFunction(name = "add", argTypes = {"DECIMALV2", "DECIMALV2"}, returnType = "DECIMALV2")
    public static DecimalLiteral addDecimalV2(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return addDecimalImpl(first, second);
    }

    @FEFunction(name = "add", argTypes = {"DECIMAL32", "DECIMAL32"}, returnType = "DECIMAL32")
    public static DecimalLiteral addDecimal32(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return addDecimalImpl(first, second);
    }

    @FEFunction(name = "add", argTypes = {"DECIMAL64", "DECIMAL64"}, returnType = "DECIMAL64")
    public static DecimalLiteral addDecimal64(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return addDecimalImpl(first, second);
    }

    @FEFunction(name = "add", argTypes = {"DECIMAL128", "DECIMAL128"}, returnType = "DECIMAL128")
    public static DecimalLiteral addDecimal128(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return addDecimalImpl(first, second);
    }

    @FEFunction(name = "add", argTypes = {"LARGEINT", "LARGEINT"}, returnType = "LARGEINT")
    public static LargeIntLiteral addBigInt(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        BigInteger left = new BigInteger(first.getStringValue());
        BigInteger right = new BigInteger(second.getStringValue());
        BigInteger result = left.add(right);
        return new LargeIntLiteral(result.toString());
    }

    @FEFunction(name = "subtract", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT")
    public static IntLiteral subtractInt(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        long result = Math.subtractExact(first.getLongValue(), second.getLongValue());
        return new IntLiteral(result, Type.BIGINT);
    }

    @FEFunction(name = "subtract", argTypes = {"DOUBLE", "DOUBLE"}, returnType = "DOUBLE")
    public static FloatLiteral subtractDouble(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        double result = first.getDoubleValue() - second.getDoubleValue();
        return new FloatLiteral(result, Type.DOUBLE);
    }

    private static DecimalLiteral subtractDecimalImpl(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        BigDecimal left = new BigDecimal(first.getStringValue());
        BigDecimal right = new BigDecimal(second.getStringValue());
        BigDecimal result = left.subtract(right);
        return new DecimalLiteral(result);
    }

    @FEFunction(name = "subtract", argTypes = {"DECIMAL", "DECIMAL"}, returnType = "DECIMAL")
    public static DecimalLiteral subtractDecimal(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return subtractDecimalImpl(first, second);
    }

    @FEFunction(name = "subtract", argTypes = {"DECIMALV2", "DECIMALV2"}, returnType = "DECIMALV2")
    public static DecimalLiteral subtractDecimalV2(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return subtractDecimalImpl(first, second);
    }

    @FEFunction(name = "subtract", argTypes = {"DECIMAL32", "DECIMAL32"}, returnType = "DECIMAL32")
    public static DecimalLiteral subtractDecimal32(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return subtractDecimalImpl(first, second);
    }

    @FEFunction(name = "subtract", argTypes = {"DECIMAL64", "DECIMAL64"}, returnType = "DECIMAL64")
    public static DecimalLiteral subtractDecimal64(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return subtractDecimalImpl(first, second);
    }

    @FEFunction(name = "subtract", argTypes = {"DECIMAL128", "DECIMAL128"}, returnType = "DECIMAL128")
    public static DecimalLiteral subtractDecimal128(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return subtractDecimalImpl(first, second);
    }

    @FEFunction(name = "subtract", argTypes = {"LARGEINT", "LARGEINT"}, returnType = "LARGEINT")
    public static LargeIntLiteral subtractBigInt(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        BigInteger left = new BigInteger(first.getStringValue());
        BigInteger right = new BigInteger(second.getStringValue());
        BigInteger result = left.subtract(right);
        return new LargeIntLiteral(result.toString());
    }

    @FEFunction(name = "multiply", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT")
    public static IntLiteral multiplyInt(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        long left = first.getLongValue();
        long right = second.getLongValue();
        long result = Math.multiplyExact(left, right);
        return new IntLiteral(result, Type.BIGINT);
    }

    @FEFunction(name = "multiply", argTypes = {"DOUBLE", "DOUBLE"}, returnType = "DOUBLE")
    public static FloatLiteral multiplyDouble(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        double result = first.getDoubleValue() * second.getDoubleValue();
        return new FloatLiteral(result, Type.DOUBLE);
    }

    private static DecimalLiteral multiplyDecimalImpl(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        BigDecimal left = new BigDecimal(first.getStringValue());
        BigDecimal right = new BigDecimal(second.getStringValue());
        BigDecimal result = left.multiply(right);
        return new DecimalLiteral(result);
    }

    @FEFunction(name = "multiply", argTypes = {"DECIMAL", "DECIMAL"}, returnType = "DECIMAL")
    public static DecimalLiteral multiplyDecimal(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return multiplyDecimalImpl(first, second);
    }

    @FEFunction(name = "multiply", argTypes = {"DECIMALV2", "DECIMALV2"}, returnType = "DECIMALV2")
    public static DecimalLiteral multiplyDecimalV2(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return multiplyDecimalImpl(first, second);
    }

    @FEFunction(name = "multiply", argTypes = {"DECIMAL32", "DECIMAL32"}, returnType = "DECIMAL32")
    public static DecimalLiteral multiplyDecimal32(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return multiplyDecimalImpl(first, second);
    }

    @FEFunction(name = "multiply", argTypes = {"DECIMAL64", "DECIMAL64"}, returnType = "DECIMAL64")
    public static DecimalLiteral multiplyDecimal64(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return multiplyDecimalImpl(first, second);
    }

    @FEFunction(name = "multiply", argTypes = {"DECIMAL128", "DECIMAL128"}, returnType = "DECIMAL128")
    public static DecimalLiteral multiplyDecimal128(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return multiplyDecimalImpl(first, second);
    }

    @FEFunction(name = "multiply", argTypes = {"LARGEINT", "LARGEINT"}, returnType = "LARGEINT")
    public static LargeIntLiteral multiplyBigInt(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        BigInteger left = new BigInteger(first.getStringValue());
        BigInteger right = new BigInteger(second.getStringValue());
        BigInteger result = left.multiply(right);
        return new LargeIntLiteral(result.toString());
    }

    @FEFunction(name = "divide", argTypes = {"DOUBLE", "DOUBLE"}, returnType = "DOUBLE")
    public static FloatLiteral divideDouble(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        if (second.getDoubleValue() == 0.0) {
            return null;
        }
        double result = first.getDoubleValue() / second.getDoubleValue();
        return new FloatLiteral(result, Type.DOUBLE);
    }

    private static DecimalLiteral divideDecimalImpl(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        BigDecimal left = new BigDecimal(first.getStringValue());
        BigDecimal right = new BigDecimal(second.getStringValue());
        if (right.compareTo(BigDecimal.ZERO) == 0) {
            return null;
        }
        BigDecimal result = left.divide(right);
        return new DecimalLiteral(result);
    }

    @FEFunction(name = "divide", argTypes = {"DECIMAL", "DECIMAL"}, returnType = "DECIMAL")
    public static DecimalLiteral divideDecimal(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return divideDecimalImpl(first, second);
    }

    @FEFunction(name = "divide", argTypes = {"DECIMALV2", "DECIMALV2"}, returnType = "DECIMALV2")
    public static DecimalLiteral divideDecimalV2(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return divideDecimalImpl(first, second);
    }

    @FEFunction(name = "divide", argTypes = {"DECIMAL32", "DECIMAL32"}, returnType = "DECIMAL32")
    public static DecimalLiteral divideDecimal32(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return divideDecimalImpl(first, second);
    }

    @FEFunction(name = "divide", argTypes = {"DECIMAL64", "DECIMAL64"}, returnType = "DECIMAL64")
    public static DecimalLiteral divideDecimal64(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return divideDecimalImpl(first, second);
    }

    @FEFunction(name = "divide", argTypes = {"DECIMAL128", "DECIMAL128"}, returnType = "DECIMAL128")
    public static DecimalLiteral divideDecimal128(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return divideDecimalImpl(first, second);
    }

    @FEFunction(name = "mod", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT")
    public static IntLiteral modInt(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        long left = first.getLongValue();
        long right = second.getLongValue();
        if (right == 0) {
            return null;
        }
        long result = left % right;
        return new IntLiteral(result, Type.BIGINT);
    }

    private static DecimalLiteral modDecimalImpl(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        BigDecimal left = new BigDecimal(first.getStringValue());
        BigDecimal right = new BigDecimal(second.getStringValue());
        if (right.compareTo(BigDecimal.ZERO) == 0) {
            return null;
        }
        BigDecimal result = left.remainder(right);
        return new DecimalLiteral(result);
    }

    @FEFunction(name = "mod", argTypes = {"DECIMAL", "DECIMAL"}, returnType = "DECIMAL")
    public static DecimalLiteral modDecimal(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return modDecimalImpl(first, second);
    }

    @FEFunction(name = "mod", argTypes = {"DECIMALV2", "DECIMALV2"}, returnType = "DECIMALV2")
    public static DecimalLiteral modDecimalV2(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return modDecimal(first, second);
    }

    @FEFunction(name = "mod", argTypes = {"DECIMAL32", "DECIMAL32"}, returnType = "DECIMAL32")
    public static DecimalLiteral modDecimal32(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return modDecimal(first, second);
    }

    @FEFunction(name = "mod", argTypes = {"DECIMAL64", "DECIMAL64"}, returnType = "DECIMAL64")
    public static DecimalLiteral modDecimal64(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return modDecimal(first, second);
    }

    @FEFunction(name = "mod", argTypes = {"DECIMAL128", "DECIMAL128"}, returnType = "DECIMAL128")
    public static DecimalLiteral modDecimal128(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return modDecimal(first, second);
    }

    @FEFunction(name = "mod", argTypes = {"LARGEINT", "LARGEINT"}, returnType = "LARGEINT")
    public static LargeIntLiteral modLargeInt(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        BigInteger left = new BigInteger(first.getStringValue());
        BigInteger right = new BigInteger(second.getStringValue());
        if (right.compareTo(BigInteger.ZERO) == 0) {
            return null;
        }
        BigInteger result = left.mod(right);
        return new LargeIntLiteral(result.toString());
    }

    @FEFunction(name = "concat", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static StringLiteral concat(StringLiteral... values) throws AnalysisException {
        Preconditions.checkArgument(values.length > 0);
        final StringBuilder resultBuilder = new StringBuilder();
        for (StringLiteral value : values) {
            resultBuilder.append(value.getStringValue());
        }
        return new StringLiteral(resultBuilder.toString());
    }

    @FEFunction(name = "concat_ws", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "VARCHAR")
    public static StringLiteral concat_ws(StringLiteral split, StringLiteral... values) throws AnalysisException {
        Preconditions.checkArgument(values.length > 0);
        final StringBuilder resultBuilder = new StringBuilder();
        for (int i = 0; i < values.length - 1; i++) {
            resultBuilder.append(values[i].getStringValue()).append(split.getStringValue());
        }
        resultBuilder.append(values[values.length - 1].getStringValue());
        return new StringLiteral(resultBuilder.toString());
    }

    @FEFunction.List(list = {
            @FEFunction(name = "ifnull", argTypes = {"VARCHAR", "VARCHAR"}, returnType = "VARCHAR"),
            @FEFunction(name = "ifnull", argTypes = {"TINYINT", "TINYINT"}, returnType = "TINYINT"),
            @FEFunction(name = "ifnull", argTypes = {"INT", "INT"}, returnType = "INT"),
            @FEFunction(name = "ifnull", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT"),
            @FEFunction(name = "ifnull", argTypes = {"DATETIME", "DATETIME"}, returnType = "DATETIME"),
            @FEFunction(name = "ifnull", argTypes = {"DATE", "DATETIME"}, returnType = "DATETIME"),
            @FEFunction(name = "ifnull", argTypes = {"DATETIME", "DATE"}, returnType = "DATETIME")
    })
    public static LiteralExpr ifNull(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return first instanceof NullLiteral ? second : first;
    }
}
