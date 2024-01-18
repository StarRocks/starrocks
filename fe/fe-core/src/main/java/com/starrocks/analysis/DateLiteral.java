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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/DateLiteral.java

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

package com.starrocks.analysis;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.thrift.TDateLiteral;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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

    // Date Literal persist type in meta
    private enum DateLiteralType {
        DATETIME(0),
        DATE(1);

        private final int value;

        DateLiteralType(int value) {
            this.value = value;
        }

        public int value() {
            return value;
        }
    }

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

    public DateLiteral(String s, Type type) throws AnalysisException {
        super();
        init(s, type);
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
        this.type = Type.DATE;
    }

    public DateLiteral(long year, long month, long day, long hour, long minute, long second, long microsecond) {
        this.year = year;
        this.month = month;
        this.day = day;
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.microsecond = microsecond;
        this.type = Type.DATETIME;
    }

    public DateLiteral(LocalDateTime dateTime, Type type) throws AnalysisException {
        this.year = dateTime.getYear();
        this.month = dateTime.getMonthValue();
        this.day = dateTime.getDayOfMonth();
        this.hour = dateTime.getHour();
        this.minute = dateTime.getMinute();
        this.second = dateTime.getSecond();
        this.microsecond = dateTime.getNano() / 1000;
        this.type = type;

        if (isNullable()) {
            throw new AnalysisException("Invalid date value");
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

    private void init(String s, Type type) throws AnalysisException {
        try {
            Preconditions.checkArgument(type.isDateType());
            LocalDateTime dateTime = DateUtils.parseStrictDateTime(s);
            year = dateTime.getYear();
            month = dateTime.getMonthValue();
            day = dateTime.getDayOfMonth();
            hour = dateTime.getHour();
            minute = dateTime.getMinute();
            second = dateTime.getSecond();
            microsecond = dateTime.getNano() / 1000;
            this.type = type;
            if (type.isDatetime() && s.contains(".") && microsecond == 0) {
                precision = s.length() - s.indexOf(".") - 1;
            }
        } catch (Exception ex) {
            throw new AnalysisException("date literal [" + s + "] is invalid");
        }
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

    @Override
    public boolean isMinValue() {
        switch (type.getPrimitiveType()) {
            case DATE:
                return this.getStringValue().compareTo(MIN_DATE.getStringValue()) == 0;
            case DATETIME:
                return this.getStringValue().compareTo(MIN_DATETIME.getStringValue()) == 0;
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
    public String toSqlImpl() {
        return "'" + getStringValue() + "'";
    }

    @Override
    public Object getRealObjectValue() {
        if (type.isDate()) {
            return LocalDateTime.of((int) getYear(), (int) getMonth(), (int) getDay(), 0, 0);
        } else if (type.isDatetime()) {
            return LocalDateTime.of((int) getYear(), (int) getMonth(), (int) getDay(),
                    (int) getHour(), (int) getMinute(), (int) getSecond(), (int) microsecond * 1000);
        } else {
            throw new StarRocksPlannerException("Invalid date type: " + type, ErrorType.INTERNAL_ERROR);
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

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.DATE_LITERAL;
        msg.date_literal = new TDateLiteral(getStringValue());
    }

    @Override
    public Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        if (targetType.isDateType()) {
            if (type.equals(targetType)) {
                return this;
            }
            if (targetType.isDate()) {
                return new DateLiteral(this.year, this.month, this.day);
            } else if (targetType.isDatetime()) {
                return new DateLiteral(this.year, this.month, this.day, this.hour, this.minute, this.second,
                        this.microsecond);
            } else {
                throw new AnalysisException("Error date literal type : " + type);
            }
        } else if (targetType.isStringType()) {
            return new StringLiteral(getStringValue());
        } else if (Type.isImplicitlyCastable(this.type, targetType, true)) {
            return new CastExpr(targetType, this);
        }
        Preconditions.checkState(false);
        return this;
    }

    public void castToDate() {
        this.type = Type.DATE;
        hour = 0;
        minute = 0;
        second = 0;
        microsecond = 0;
    }

    public java.time.LocalDateTime toLocalDateTime() {
        return java.time.LocalDateTime.of((int) year, (int) month, (int) day, (int) hour, (int) minute, (int) second,
                (int) microsecond * 1000);
    }

    private long makePackedDatetime() {
        long ymd = ((year * 13 + month) << 5) | day;
        long hms = (hour << 12) | (minute << 6) | second;
        return ((ymd << 17) | hms) << 24 + microsecond;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        // set flag bit in meta, 0 is DATETIME and 1 is DATE
        if (this.type.isDatetime()) {
            out.writeShort(DateLiteralType.DATETIME.value());
        } else if (this.type.isDate()) {
            out.writeShort(DateLiteralType.DATE.value());
        } else {
            throw new IOException("Error date literal type : " + type);
        }
        out.writeLong(makePackedDatetime());
    }

    private void fromPackedDatetime(long packed_time) {
        microsecond = (packed_time % (1L << 24));
        long ymdhms = (packed_time >> 24);
        long ymd = ymdhms >> 17;
        long hms = ymdhms % (1 << 17);

        day = ymd % (1 << 5);
        long ym = ymd >> 5;
        month = ym % 13;
        year = ym / 13;
        year %= 10000;
        second = hms % (1 << 6);
        minute = (hms >> 6) % (1 << 6);
        hour = (hms >> 12);
        // set default date literal type to DATETIME
        // date literal read from meta will set type by flag bit;
        this.type = Type.DATETIME;
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        short date_literal_type = in.readShort();
        fromPackedDatetime(in.readLong());
        if (date_literal_type == DateLiteralType.DATETIME.value()) {
            this.type = Type.DATETIME;
        } else if (date_literal_type == DateLiteralType.DATE.value()) {
            this.type = Type.DATE;
        } else {
            throw new IOException("Error date literal type : " + type);
        }
    }

    public static DateLiteral read(DataInput in) throws IOException {
        DateLiteral literal = new DateLiteral();
        literal.readFields(in);
        return literal;
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
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public boolean isNullable() {
        if (type == Type.DATETIME) {
            return this.compareLiteral(DateLiteral.MIN_DATETIME) < 0
                    || DateLiteral.MAX_DATETIME.compareLiteral(this) < 0;
        } else if (type == Type.DATE) {
            return this.compareLiteral(DateLiteral.MIN_DATE) < 0 || DateLiteral.MAX_DATE.compareLiteral(this) < 0;
        }
        return true;
    }

    @Override
    public void parseMysqlParam(ByteBuffer data) {
        int len = getParamLen(data);
        if (type.getPrimitiveType() == PrimitiveType.DATE) {
            if (len >= 4) {
                year = (int) data.getChar();
                month = (int) data.get();
                day = (int) data.get();
                hour = 0;
                minute = 0;
                second = 0;
                microsecond = 0;
            } else {
                copy(MIN_DATE);
            }
            return;
        }
        if (type.getPrimitiveType() == PrimitiveType.DATETIME) {
            if (len >= 4) {
                year = (int) data.getChar();
                month = (int) data.get();
                day = (int) data.get();
                microsecond = 0;
                if (len > 4) {
                    hour = (int) data.get();
                    minute = (int) data.get();
                    second = (int) data.get();
                } else {
                    hour = 0;
                    minute = 0;
                    second = 0;
                    microsecond = 0;
                }
                if (len > 7) {
                    microsecond = data.getInt();
                    // choose the highest scale to keep microsecond value
                    type = ScalarType.createDecimalV2Type(6);
                }
            } else {
                copy(MIN_DATETIME);
            }
        }
    }
}
