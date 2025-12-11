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

package com.starrocks.sql.ast.expression;

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.type.DateType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.TimeZone;

public class DateLiteral extends LiteralExpr {

    private static final DateLiteral MIN_DATE = new DateLiteral(0, 1, 1);
    private static final DateLiteral MAX_DATE = new DateLiteral(9999, 12, 31);
    private static final DateLiteral MIN_DATETIME = new DateLiteral(0, 1, 1, 0, 0, 0, 0);
    private static final DateLiteral MAX_DATETIME = new DateLiteral(9999, 12, 31, 23, 59, 59, 999999);
    // The default precision of datetime is 0
    private int precision = 0;

    private DateLiteral() {
        super();
    }

    public DateLiteral(Type type, boolean isMax) {
        super();
        this.type = type;
        if (type.isDate()) {
            if (isMax) {
                copy(MAX_DATE);
            } else {
                copy(MIN_DATE);
            }
        } else {
            if (isMax) {
                copy(MAX_DATETIME);
            } else {
                copy(MIN_DATETIME);
            }
        }
        analysisDone();
    }

    public DateLiteral(long year, long month, long day) {
        this.year = year;
        this.month = month;
        this.day = day;
        this.hour = 0;
        this.minute = 0;
        this.second = 0;
        this.microsecond = 0;
        this.type = DateType.DATE;
    }

    public DateLiteral(long year, long month, long day, long hour, long minute, long second, long microsecond) {
        this.year = year;
        this.month = month;
        this.day = day;
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.microsecond = microsecond;
        this.type = DateType.DATETIME;
    }

    public DateLiteral(LocalDateTime dateTime, Type type) {
        this.year = dateTime.getYear();
        this.month = dateTime.getMonthValue();
        this.day = dateTime.getDayOfMonth();
        this.hour = dateTime.getHour();
        this.minute = dateTime.getMinute();
        this.second = dateTime.getSecond();
        this.microsecond = dateTime.getNano() / 1000;
        this.type = type;
        if (type.isDatetime()) {
            precision = inferPrecision(this.microsecond);
        }

        if (isNullable()) {
            throw new ParsingException("Invalid date value");
        }
    }

    public DateLiteral(DateLiteral other) {
        super(other);
        hour = other.hour;
        minute = other.minute;
        second = other.second;
        year = other.year;
        month = other.month;
        day = other.day;
        microsecond = other.microsecond;
        type = other.type;
    }

    public static DateLiteral createMinValue(Type type) {
        return new DateLiteral(type, false);
    }

    public static DateLiteral createMaxValue(Type type) {
        return new DateLiteral(type, true);
    }

    private void copy(DateLiteral other) {
        hour = other.hour;
        minute = other.minute;
        second = other.second;
        year = other.year;
        month = other.month;
        day = other.day;
        microsecond = other.microsecond;
        type = other.type;

    }

    @Override
    public Expr clone() {
        return new DateLiteral(this);
    }

    private int inferPrecision(long microValue) {
        if (microValue == 0) {
            return 0;
        }
        int digits = 6;
        while (digits > 0 && microValue % 10 == 0) {
            microValue /= 10;
            digits--;
        }
        return digits;
    }

    @Override
    public boolean isMinValue() {
        switch (type.getPrimitiveType()) {
            case DATE:
                return this.year == MIN_DATE.year && this.month == MIN_DATE.month && this.day == MIN_DATE.day;
            case DATETIME:
                return this.year == MIN_DATETIME.year && this.month == MIN_DATETIME.month
                        && this.day == MIN_DATETIME.day && this.hour == MIN_DATETIME.hour
                        && this.minute == MIN_DATETIME.minute && this.second == MIN_DATETIME.second
                        && this.microsecond == MIN_DATETIME.microsecond;
            default:
                return false;
        }
    }

    // Date column and Datetime column's hash value is not same.
    @Override
    public ByteBuffer getHashValue(Type type) {
        String value = convertToString(type.getPrimitiveType());
        ByteBuffer buffer;
        try {
            buffer = ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return buffer;
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        if (expr instanceof NullLiteral) {
            return 1;
        }

        if (expr == MaxLiteral.MAX_VALUE) {
            return -1;
        }
        if (getDoubleValue() < expr.getDoubleValue()) {
            return -1;
        } else if (getDoubleValue() > expr.getDoubleValue()) {
            return 1;
        }
        return 0;
    }

    @Override
    public Object getRealObjectValue() {
        if (type.isDate()) {
            return LocalDateTime.of((int) getYear(), (int) getMonth(), (int) getDay(), 0, 0);
        } else if (type.isDatetime()) {
            return LocalDateTime.of((int) getYear(), (int) getMonth(), (int) getDay(),
                    (int) getHour(), (int) getMinute(), (int) getSecond(), (int) microsecond * 1000);
        } else {
            throw new ParsingException("Invalid date type: " + type);
        }
    }

    @Override
    public String getStringValue() {
        return convertToString(type.getPrimitiveType());
    }

    private String convertToString(PrimitiveType type) {
        if (type == PrimitiveType.DATE) {
            return String.format("%04d-%02d-%02d", year, month, day);
        } else if (microsecond == 0) {
            return String.format("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second);
        } else {
            return String.format("%04d-%02d-%02d %02d:%02d:%02d.%06d", year, month, day, hour, minute, second,
                    microsecond);
        }
    }

    @Override
    public long getLongValue() {
        return (year * 10000 + month * 100 + day) * 1000000L + hour * 10000 + minute * 100 + second;
    }

    @Override
    public double getDoubleValue() {
        if (microsecond > 0) {
            return getLongValue() + ((double) microsecond / 1000000);
        } else {
            return getLongValue();
        }
    }


    public void castToDate() {
        this.type = DateType.DATE;
        hour = 0;
        minute = 0;
        second = 0;
        microsecond = 0;
    }

    public java.time.LocalDateTime toLocalDateTime() {
        return java.time.LocalDateTime.of((int) year, (int) month, (int) day, (int) hour, (int) minute, (int) second,
                (int) microsecond * 1000);
    }

    public long unixTimestamp(TimeZone timeZone) {
        return LocalDateTime.of((int) year, (int) month, (int) day, (int) hour, (int) minute, (int) second)
                .atZone(timeZone.toZoneId()).toEpochSecond() * 1000;
    }

    public long getYear() {
        return year;
    }

    public long getMonth() {
        return month;
    }

    public long getDay() {
        return day;
    }

    public long getHour() {
        return hour;
    }

    public long getMinute() {
        return minute;
    }

    public long getSecond() {
        return second;
    }

    public long getMicrosecond() {
        return microsecond;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }

    private long year;
    private long month;
    private long day;
    private long hour;
    private long minute;
    private long second;
    private long microsecond;

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), Objects.hashCode(unixTimestamp(TimeZone.getDefault())));
    }

    @Override
    public boolean isNullable() {
        if (type.isDatetime()) {
            return this.compareLiteral(DateLiteral.MIN_DATETIME) < 0
                    || DateLiteral.MAX_DATETIME.compareLiteral(this) < 0;
        } else if (type.isDate()) {
            return this.compareLiteral(DateLiteral.MIN_DATE) < 0 || DateLiteral.MAX_DATE.compareLiteral(this) < 0;
        }
        return true;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDateLiteral(this, context);
    }
}
