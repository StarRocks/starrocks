// This file is made available under Elastic License 2.0.
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
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TDateLiteral;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
<<<<<<< HEAD
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import java.util.Date;
=======
>>>>>>> d88b4657a ([BugFix] Fix double/float/date cast to string in FE (#27070))
import java.util.Objects;
import java.util.TimeZone;

public class DateLiteral extends LiteralExpr {

    private static final DateLiteral MIN_DATE = new DateLiteral(0, 1, 1);
    private static final DateLiteral MAX_DATE = new DateLiteral(9999, 12, 31);
    private static final DateLiteral MIN_DATETIME = new DateLiteral(0, 1, 1, 0, 0, 0);
    private static final DateLiteral MAX_DATETIME = new DateLiteral(9999, 12, 31, 23, 59, 59);

<<<<<<< HEAD
    private static final DateTimeFormatter DATE_TIME_FORMATTER;
    private static final DateTimeFormatter DATE_FORMATTER;
    private static final DateTimeFormatter DATE_NO_SPLIT_FORMATTER;
    /*
     * Dates containing two-digit year values are ambiguous because the century is unknown.
     * MySQL interprets two-digit year values using these rules:
     * Year values in the range 70-99 are converted to 1970-1999.
     * Year values in the range 00-69 are converted to 2000-2069.
     * */
    private static final DateTimeFormatter DATE_TIME_FORMATTER_TWO_DIGIT;
    private static final DateTimeFormatter DATE_FORMATTER_TWO_DIGIT;

    static {
        DATE_TIME_FORMATTER = DateUtils.unixDatetimeFormatBuilder("%Y-%m-%e %H:%i:%s")
                .toFormatter().withResolverStyle(ResolverStyle.STRICT);
        DATE_FORMATTER = DateUtils.unixDatetimeFormatBuilder("%Y-%m-%e")
                .toFormatter().withResolverStyle(ResolverStyle.STRICT);
        DATE_TIME_FORMATTER_TWO_DIGIT = DateUtils.unixDatetimeFormatBuilder("%y-%m-%e %H:%i:%s")
                .toFormatter().withResolverStyle(ResolverStyle.STRICT);
        DATE_FORMATTER_TWO_DIGIT = DateUtils.unixDatetimeFormatBuilder("%y-%m-%e")
                .toFormatter().withResolverStyle(ResolverStyle.STRICT);
        DATE_NO_SPLIT_FORMATTER = DateUtils.unixDatetimeFormatBuilder("%Y%m%e")
                .toFormatter().withResolverStyle(ResolverStyle.STRICT);
    }

    //Date Literal persist type in meta
=======
    // Date Literal persist type in meta
>>>>>>> d88b4657a ([BugFix] Fix double/float/date cast to string in FE (#27070))
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

    public DateLiteral(Type type, boolean isMax) throws AnalysisException {
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
        this.hour = 0;
        this.minute = 0;
        this.second = 0;
        this.year = year;
        this.month = month;
        this.day = day;
        this.type = Type.DATE;
    }

    public DateLiteral(long year, long month, long day, long hour, long minute, long second) {
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.year = year;
        this.month = month;
        this.day = day;
        this.type = Type.DATETIME;
    }

    public DateLiteral(LocalDateTime dateTime, Type type) throws AnalysisException {
        this.year = dateTime.getYear();
        this.month = dateTime.getMonthValue();
        this.day = dateTime.getDayOfMonth();
        this.hour = dateTime.getHour();
        this.minute = dateTime.getMinute();
        this.second = dateTime.getSecond();
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

    public static DateLiteral createMinValue(Type type) throws AnalysisException {
        return new DateLiteral(type, false);
    }

    private void init(String s, Type type) throws AnalysisException {
        try {
            Preconditions.checkArgument(type.isDateType());
<<<<<<< HEAD
            LocalDateTime dateTime;
            if (type.isDate()) {
                if (s.split("-")[0].length() == 2) {
                    dateTime = DateUtils.parseStringWithDefaultHSM(s, DATE_FORMATTER_TWO_DIGIT);
                } else if (s.length() == 8) {
                    // 20200202
                    dateTime = DateUtils.parseStringWithDefaultHSM(s, DATE_NO_SPLIT_FORMATTER);
                } else {
                    dateTime = DateUtils.parseStringWithDefaultHSM(s, DATE_FORMATTER);
                }
            } else {
                if (s.split("-")[0].length() == 2) {
                    dateTime = DateUtils.parseStringWithDefaultHSM(s, DATE_TIME_FORMATTER_TWO_DIGIT);
                } else {
                    dateTime = DateUtils.parseStringWithDefaultHSM(s, DATE_TIME_FORMATTER);
                }
            }

=======
            LocalDateTime dateTime = DateUtils.parseStrictDateTime(s);
>>>>>>> d88b4657a ([BugFix] Fix double/float/date cast to string in FE (#27070))
            year = dateTime.getYear();
            month = dateTime.getMonthValue();
            day = dateTime.getDayOfMonth();
            hour = dateTime.getHour();
            minute = dateTime.getMinute();
            second = dateTime.getSecond();
            this.type = type;
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

    @Override
    public Object getRealValue() {
        if (type.isDate()) {
            return year * 16 * 32L + month * 32 + day;
        } else if (type.isDatetime()) {
            return (year * 10000 + month * 100 + day) * 1000000L + hour * 10000 + minute * 100 + second;
        } else {
            Preconditions.checkState(false, "invalid date type: " + type);
            return -1L;
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
        // date time will not overflow when doing addition and subtraction
        return Long.signum(getLongValue() - expr.getLongValue());
    }

    @Override
    public String toSqlImpl() {
        return "'" + getStringValue() + "'";
    }

    @Override
    public String getStringValue() {
        return convertToString(type.getPrimitiveType());
    }

    private String convertToString(PrimitiveType type) {
        if (type == PrimitiveType.DATE) {
            return String.format("%04d-%02d-%02d", year, month, day);
        } else {
<<<<<<< HEAD
            return String.format("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second);
=======
            return String.format("%04d-%02d-%02d %02d:%02d:%02d.%06d", year, month, day, hour, minute, second,
                    microsecond);
>>>>>>> d88b4657a ([BugFix] Fix double/float/date cast to string in FE (#27070))
        }
    }

    @Override
    public long getLongValue() {
        return (year * 10000 + month * 100 + day) * 1000000L + hour * 10000 + minute * 100 + second;
    }

    @Override
    public double getDoubleValue() {
<<<<<<< HEAD
        return getLongValue();
=======
        if (microsecond > 0) {
            return getLongValue() + ((double) microsecond / 1000000);
        } else {
            return getLongValue();
        }
>>>>>>> d88b4657a ([BugFix] Fix double/float/date cast to string in FE (#27070))
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
<<<<<<< HEAD
                return new DateLiteral(this.year, this.month, this.day, this.hour, this.minute, this.second);
=======
                return new DateLiteral(this.year, this.month, this.day, this.hour, this.minute, this.second,
                        this.microsecond);
>>>>>>> d88b4657a ([BugFix] Fix double/float/date cast to string in FE (#27070))
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
    }

    public java.time.LocalDateTime toLocalDateTime() {
<<<<<<< HEAD
        return java.time.LocalDateTime.of((int) year, (int) month, (int) day, (int) hour, (int) minute, (int) second);
=======
        return java.time.LocalDateTime.of((int) year, (int) month, (int) day, (int) hour, (int) minute, (int) second,
                (int) microsecond * 1000);
>>>>>>> d88b4657a ([BugFix] Fix double/float/date cast to string in FE (#27070))
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
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_60) {
            short date_literal_type = in.readShort();
            fromPackedDatetime(in.readLong());
            if (date_literal_type == DateLiteralType.DATETIME.value()) {
                this.type = Type.DATETIME;
            } else if (date_literal_type == DateLiteralType.DATE.value()) {
                this.type = Type.DATE;
            } else {
                throw new IOException("Error date literal type : " + type);
            }
        } else {
            Date date = new Date(in.readLong());
            String date_str = TimeUtils.format(date, Type.DATETIME);
            try {
                init(date_str, Type.DATETIME);
            } catch (AnalysisException ex) {
                throw new IOException(ex.getMessage());
            }
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
        if (type == Type.DATETIME) {
            return this.compareLiteral(DateLiteral.MIN_DATETIME) < 0
                    || DateLiteral.MAX_DATETIME.compareLiteral(this) < 0;
        } else if (type == Type.DATE) {
            return this.compareLiteral(DateLiteral.MIN_DATE) < 0 || DateLiteral.MAX_DATE.compareLiteral(this) < 0;
        }
        return true;
    }
}
