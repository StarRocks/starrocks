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

package com.starrocks.sql.optimizer.operator.scalar;

import com.starrocks.analysis.DecimalLiteral;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static com.starrocks.catalog.Type.TINYINT;
import static java.util.Collections.emptyList;

/**
 * TYPE            |  JAVA_TYPE
 * TYPE_INVALID    |    null
 * TYPE_NULL       |    null
 * TYPE_BOOLEAN    |    boolean
 * TYPE_TINYINT    |    byte
 * TYPE_SMALLINT   |    short
 * TYPE_INT        |    int
 * TYPE_BIGINT     |    long
 * TYPE_LARGEINT   |    BigInteger
 * TYPE_FLOAT      |    double
 * TYPE_DOUBLE     |    double
 * TYPE_DATE       |    LocalDateTime
 * TYPE_DATETIME   |    LocalDateTime
 * TYPE_TIME       |    LocalDateTime
 * TYPE_DECIMAL    |    BigDecimal
 * TYPE_DECIMALV2  |    BigDecimal
 * TYPE_VARCHAR    |    String
 * TYPE_CHAR       |    String
 * TYPE_HLL        |    NOT_SUPPORT
 * TYPE_BITMAP     |    NOT_SUPPORT
 * TYPE_PERCENTILE |    NOT_SUPPORT
 */
public final class ConstantOperator extends ScalarOperator implements Comparable<ConstantOperator> {

    private static final LocalDateTime MAX_DATETIME = LocalDateTime.of(9999, 12, 31, 23, 59, 59);
    private static final LocalDateTime MIN_DATETIME = LocalDateTime.of(0, 1, 1, 0, 0, 0);

    public static final ConstantOperator NULL = ConstantOperator.createNull(Type.BOOLEAN);
    public static final ConstantOperator TRUE = ConstantOperator.createBoolean(true);
    public static final ConstantOperator FALSE = ConstantOperator.createBoolean(false);

    private static final BigInteger MAX_LARGE_INT = new BigInteger("2").pow(127).subtract(BigInteger.ONE);
    private static final BigInteger MIN_LARGE_INT = new BigInteger("2").pow(128).multiply(BigInteger.valueOf(-1));

    private static void requiredValid(LocalDateTime dateTime) throws SemanticException {
        if (null == dateTime || dateTime.isBefore(MIN_DATETIME) || dateTime.isAfter(MAX_DATETIME)) {
            throw new SemanticException("Invalid date value: " + (dateTime == null ? "NULL" : dateTime.toString()));
        }
    }

    private static void requiredValid(double value) throws SemanticException {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new SemanticException("Invalid float/double value: " + value);
        }
    }

    private final Object value;
    private final boolean isNull;

    private ConstantOperator(Type type) {
        super(OperatorType.CONSTANT, type);
        this.value = null;
        this.isNull = true;
    }

    public ConstantOperator(Object value, Type type) {
        super(OperatorType.CONSTANT, type);
        Objects.requireNonNull(value, "constant value is null");
        this.value = value;
        this.isNull = false;
    }

    public static ConstantOperator createObject(Object value, Type type) {
        return new ConstantOperator(value, type);
    }

    public static ConstantOperator createNull(Type type) {
        return new ConstantOperator(type);
    }

    public static ConstantOperator createBoolean(boolean value) {
        return new ConstantOperator(value, Type.BOOLEAN);
    }

    public static ConstantOperator createTinyInt(byte value) {
        return new ConstantOperator(value, TINYINT);
    }

    public static ConstantOperator createSmallInt(short value) {
        return new ConstantOperator(value, Type.SMALLINT);
    }

    public static ConstantOperator createInt(int value) {
        return new ConstantOperator(value, Type.INT);
    }

    public static ConstantOperator createBigint(long value) {
        return new ConstantOperator(value, Type.BIGINT);
    }

    public static ConstantOperator createLargeInt(BigInteger value) {
        return new ConstantOperator(value, Type.LARGEINT);
    }

    public static ConstantOperator createFloat(double value) throws SemanticException {
        requiredValid(value);
        return new ConstantOperator(value, Type.FLOAT);
    }

    public static ConstantOperator createDouble(double value) throws SemanticException {
        requiredValid(value);
        return new ConstantOperator(value, Type.DOUBLE);
    }

    public static ConstantOperator createDate(LocalDateTime value) throws SemanticException {
        requiredValid(value);
        return new ConstantOperator(value, Type.DATE);
    }

    public static ConstantOperator createDatetime(LocalDateTime value) throws SemanticException {
        requiredValid(value);
        return new ConstantOperator(value, Type.DATETIME);
    }

    public static ConstantOperator createDatetime(LocalDateTime value, Type dateType) {
        return new ConstantOperator(value, dateType);
    }

    public static ConstantOperator createTime(double value) {
        return new ConstantOperator(value, Type.TIME);
    }

    public static ConstantOperator createDecimal(BigDecimal value, Type type) {
        return new ConstantOperator(value, type);
    }

    public static ConstantOperator createVarchar(String value) {
        return new ConstantOperator(value, Type.VARCHAR);
    }

    public static ConstantOperator createChar(String value) {
        return new ConstantOperator(value, Type.CHAR);
    }

    public static ConstantOperator createChar(String value, Type charType) {
        return new ConstantOperator(value, charType);
    }

    public static ConstantOperator createBinary(byte[] value, Type binaryType) {
        return new ConstantOperator(value, binaryType);
    }

    public static ConstantOperator createExampleValueByType(Type type) {
        if (type.isTinyint()) {
            return createTinyInt((byte) 1);
        } else if (type.isSmallint()) {
            return createSmallInt((short) 1);
        } else if (type.isInt()) {
            return createInt(1);
        } else if (type.isBigint()) {
            return createBigint(1L);
        } else if (type.isLargeint()) {
            return createLargeInt(new BigInteger("1"));
        } else if (type.isDate()) {
            return createDate(LocalDateTime.of(2000, 1, 1, 00, 00, 00));
        } else if (type.isDatetime()) {
            return createDatetime(LocalDateTime.of(2000, 1, 1, 00, 00, 00));
        } else {
            throw new IllegalArgumentException("unsupported type: " + type);
        }
    }

    public boolean isNull() {
        return isNull;
    }

    public boolean isZero() {
        boolean isZero;
        if (isNull || value == null) {
            return false;
        }
        if (type.isInt()) {
            Integer val = (Integer) value;
            isZero = (val.compareTo(0) == 0);
        } else if (type.isBigint()) {
            Long val = (Long) value;
            isZero = (val.compareTo(0L) == 0);
        } else if (type.isLargeint()) {
            BigInteger val = (BigInteger) value;
            isZero = (val.compareTo(BigInteger.ZERO) == 0);
        } else if (type.isFloat()) {
            Float val = (Float) value;
            isZero = (val.compareTo(0.0f) == 0);
        } else if (type.isDouble()) {
            Double val = (Double) value;
            isZero = (val.compareTo(0.0) == 0);
        } else if (type.isDecimalV3()) {
            BigDecimal val = (BigDecimal) value;
            isZero = (val.compareTo(BigDecimal.ZERO) == 0);
        } else {
            isZero = false;
        }
        return isZero;
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    @Override
    public boolean isVariable() {
        return false;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public List<ScalarOperator> getChildren() {
        // constant scalar operator should be the leaf node
        return emptyList();
    }

    @Override
    public ScalarOperator getChild(int index) {
        return null;
    }

    @Override
    public void setChild(int index, ScalarOperator child) {
    }

    public ColumnRefSet getUsedColumns() {
        return new ColumnRefSet();
    }

    public boolean getBoolean() {
        return (boolean) Optional.ofNullable(value).orElse(false);
    }

    public byte getTinyInt() {
        return (byte) Optional.ofNullable(value).orElse((byte) 0);
    }

    public short getSmallint() {
        return (short) Optional.ofNullable(value).orElse((short) 0);
    }

    public int getInt() {
        return (int) Optional.ofNullable(value).orElse(0);
    }

    public long getBigint() {
        return (long) Optional.ofNullable(value).orElse((long) 0);
    }

    public BigInteger getLargeInt() {
        return (BigInteger) Optional.ofNullable(value).orElse(new BigInteger("0"));
    }

    public double getDouble() {
        return (double) Optional.ofNullable(value).orElse((double) 0);
    }

    public double getFloat() {
        return (double) Optional.ofNullable(value).orElse((double) 0);
    }

    public LocalDateTime getDate() {
        return (LocalDateTime) Optional.ofNullable(value).orElse(LocalDateTime.MIN);
    }

    public LocalDateTime getDatetime() {
        return (LocalDateTime) Optional.ofNullable(value).orElse(LocalDateTime.MIN);
    }

    public double getTime() {
        return (double) Optional.ofNullable(value).orElse(0);
    }

    public BigDecimal getDecimal() {
        return (BigDecimal) Optional.ofNullable(value).orElse(new BigDecimal(0));
    }

    public String getVarchar() {
        return (String) Optional.ofNullable(value).orElse("");
    }

    public String getChar() {
        return (String) Optional.ofNullable(value).orElse("");
    }

    public byte[] getBinary() {
        return (byte[]) (value);
    }

    @Override
    public String toString() {
        if (isNull()) {
            return "null";
        } else if (type.isDatetime()) {
            LocalDateTime time = (LocalDateTime) Optional.ofNullable(value).orElse(LocalDateTime.MIN);
            if (time.getNano() != 0) {
                return time.format(DateUtils.DATE_TIME_MS_FORMATTER_UNIX);
            }
            return time.format(DateUtils.DATE_TIME_FORMATTER_UNIX);
        } else if (type.isDate()) {
            LocalDateTime time = (LocalDateTime) Optional.ofNullable(value).orElse(LocalDateTime.MIN);
            return time.format(DateUtils.DATE_FORMATTER_UNIX);
        } else if (type.isFloatingPointType()) {
            double val = (double) Optional.ofNullable(value).orElse((double) 0);
            BigDecimal decimal = BigDecimal.valueOf(val);
            return decimal.stripTrailingZeros().toPlainString();
        }

        if (ConnectContext.get() != null &&
                !ConnectContext.get().getSessionVariable().isCboDecimalCastStringStrict()) {
            return String.valueOf(value);
        }

        if (type.isDecimalV2()) {
            // remove trailing zero and use plain string, keep same with BE
            return ((BigDecimal) value).stripTrailingZeros().toPlainString();
        } else if (type.isDecimalOfAnyVersion()) {
            // align zero, keep same with BE
            int scale = ((ScalarType) type).getScalarScale();
            BigDecimal val = (BigDecimal) value;
            DecimalFormat df = new DecimalFormat((scale == 0 ? "0" : "0.") + StringUtils.repeat("0", scale));
            return df.format(val);
        }

        return String.valueOf(value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, type.getPrimitiveType(), isNull);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ConstantOperator that = (ConstantOperator) obj;
        return isNull == that.isNull &&
                Objects.equals(value, that.value) &&
                type.matchesType(that.getType());
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitConstant(this, context);
    }

    @Override
    public int compareTo(ConstantOperator o) {
        // must keep type same
        if (isNull() && o.isNull()) {
            return 0;
        } else if (isNull() || o.isNull()) {
            return isNull() ? -1 : 1;
        }

        PrimitiveType t = type.getPrimitiveType();
        // char is same with varchar, but equivalence expression deriver can't keep same in some expression
        if (t != o.getType().getPrimitiveType()
                && (!t.isCharFamily() && !o.getType().getPrimitiveType().isCharFamily())
                && (!t.isDecimalOfAnyVersion() && !o.getType().getPrimitiveType().isDecimalOfAnyVersion())) {
            throw new StarRocksPlannerException("Constant " + this + " can't compare with Constant " + o,
                    ErrorType.INTERNAL_ERROR);
        }

        if (t == PrimitiveType.BOOLEAN) {
            return Boolean.compare(getBoolean(), o.getBoolean());
        } else if (t == PrimitiveType.TINYINT) {
            return Byte.compare(getTinyInt(), o.getTinyInt());
        } else if (t == PrimitiveType.SMALLINT) {
            return Short.compare(getSmallint(), o.getSmallint());
        } else if (t == PrimitiveType.INT) {
            return Integer.compare(getInt(), o.getInt());
        } else if (t == PrimitiveType.BIGINT) {
            return Long.compare(getBigint(), o.getBigint());
        } else if (t == PrimitiveType.LARGEINT) {
            return getLargeInt().compareTo(o.getLargeInt());
        } else if (t == PrimitiveType.FLOAT || t == PrimitiveType.TIME) {
            return Double.compare(getDouble(), o.getDouble());
        } else if (t == PrimitiveType.DOUBLE) {
            return Double.compare(getDouble(), o.getDouble());
        } else if (t == PrimitiveType.DATE || t == PrimitiveType.DATETIME) {
            return getDatetime().compareTo(o.getDatetime());
        } else if (t.isDecimalOfAnyVersion()) {
            return getDecimal().compareTo(o.getDecimal());
        } else if (t == PrimitiveType.CHAR || t == PrimitiveType.VARCHAR) {
            return getVarchar().compareTo(o.getVarchar());
        }

        return -1;
    }

    @Override
    public boolean isNullable() {
        return type.equals(Type.NULL) || isNull;
    }

    public ConstantOperator castToStrictly(Type type) throws Exception {
        if (!type.isDecimalV3()) {
            return castTo(type);
        }

        BigDecimal decimal = new BigDecimal(value.toString());
        ScalarType scalarType = (ScalarType) type;
        try {
            DecimalLiteral.checkLiteralOverflowInDecimalStyle(decimal, scalarType);
        } catch (AnalysisException ignored) {
            return ConstantOperator.createNull(type);
        }
        int realScale = DecimalLiteral.getRealScale(decimal);
        int scale = scalarType.getScalarScale();
        if (scale <= realScale) {
            decimal = decimal.setScale(scale, RoundingMode.HALF_UP);
        }

        if (scalarType.getScalarScale() == 0 && scalarType.getScalarPrecision() == 0) {
            throw new SemanticException("Forbidden cast to decimal(precision=0, scale=0)");
        }
        return ConstantOperator.createDecimal(decimal, type);
    }

    public ConstantOperator castTo(Type desc) throws Exception {
        if (type.isTime() || desc.isTime()) {
            // Don't support constant time cast in FE
            throw UnsupportedException
                    .unsupportedException(this + " cast to " + desc.getPrimitiveType().toString());
        }

        String childString = toString();
        if (getType().isBoolean()) {
            childString = getBoolean() ? "1" : "0";
        }

        if (desc.isBoolean()) {
            if ("FALSE".equalsIgnoreCase(childString) || "0".equalsIgnoreCase(childString)) {
                return ConstantOperator.createBoolean(false);
            } else if ("TRUE".equalsIgnoreCase(childString) || "1".equalsIgnoreCase(childString)) {
                return ConstantOperator.createBoolean(true);
            }
        } else if (desc.isTinyint()) {
            return ConstantOperator.createTinyInt(Byte.parseByte(childString.trim()));
        } else if (desc.isSmallint()) {
            return ConstantOperator.createSmallInt(Short.parseShort(childString.trim()));
        } else if (desc.isInt()) {
            if (Type.DATE.equals(type)) {
                childString = DateUtils.convertDateFormaterToDateKeyFormater(childString);
            }
            return ConstantOperator.createInt(Integer.parseInt(childString.trim()));
        } else if (desc.isBigint()) {
            if (Type.DATE.equals(type)) {
                childString = DateUtils.convertDateFormaterToDateKeyFormater(childString);
            } else if (Type.DATETIME.equals(type)) {
                childString = DateUtils.convertDateTimeFormaterToSecondFormater(childString);
            }
            return ConstantOperator.createBigint(Long.parseLong(childString.trim()));
        } else if (desc.isLargeint()) {
            if (Type.DATE.equals(type)) {
                childString = DateUtils.convertDateFormaterToDateKeyFormater(childString);
            } else if (Type.DATETIME.equals(type)) {
                childString = DateUtils.convertDateTimeFormaterToSecondFormater(childString);
            }
            return ConstantOperator.createLargeInt(new BigInteger(childString.trim()));
        } else if (desc.isFloat()) {
            if (Type.DATE.equals(type)) {
                childString = DateUtils.convertDateFormaterToDateKeyFormater(childString);
            } else if (Type.DATETIME.equals(type)) {
                childString = DateUtils.convertDateTimeFormaterToSecondFormater(childString);
            }
            return ConstantOperator.createFloat(Double.parseDouble(childString));
        } else if (desc.isDouble()) {
            if (Type.DATE.equals(type)) {
                childString = DateUtils.convertDateFormaterToDateKeyFormater(childString);
            } else if (Type.DATETIME.equals(type)) {
                childString = DateUtils.convertDateTimeFormaterToSecondFormater(childString);
            }
            return ConstantOperator.createDouble(Double.parseDouble(childString));
        } else if (desc.isDate() || desc.isDatetime()) {
            String dateStr = StringUtils.strip(childString, "\r\n\t ");
            LocalDateTime dateTime = DateUtils.parseStrictDateTime(dateStr);
            if (Type.DATE.equals(desc)) {
                dateTime = dateTime.truncatedTo(ChronoUnit.DAYS);
            }
            return ConstantOperator.createDatetime(dateTime, desc);
        } else if (desc.isDecimalV2()) {
            return ConstantOperator.createDecimal(BigDecimal.valueOf(Double.parseDouble(childString)), Type.DECIMALV2);
        } else if (desc.isDecimalV3()) {
            BigDecimal decimal = new BigDecimal(childString);
            ScalarType scalarType = (ScalarType) desc;
            try {
                DecimalLiteral.checkLiteralOverflowInBinaryStyle(decimal, scalarType);
            } catch (AnalysisException ignored) {
                return ConstantOperator.createNull(desc);
            }
            int realScale = DecimalLiteral.getRealScale(decimal);
            int scale = scalarType.getScalarScale();
            if (scale <= realScale) {
                decimal = decimal.setScale(scale, RoundingMode.HALF_UP);
            }

            if (scalarType.getScalarScale() == 0 && scalarType.getScalarPrecision() == 0) {
                throw new SemanticException("Forbidden cast to decimal(precision=0, scale=0)");
            }

            return ConstantOperator.createDecimal(decimal, desc);
        } else if (desc.isChar() || desc.isVarchar()) {
            return ConstantOperator.createChar(childString, desc);
        }

        throw UnsupportedException.unsupportedException(this + " cast to " + desc.getPrimitiveType().toString());
    }

    public Optional<ConstantOperator> successor() {
        return computeValue(1);
    }

    public Optional<ConstantOperator> predecessor() {
        return computeValue(-1);
    }

    private Optional<ConstantOperator> computeValue(int delta) {
        return computeWithLimits(delta,
                v -> (byte) (v + delta),
                v -> (short) (v + delta),
                v -> v + delta,
                v -> (long) v + delta,
                v -> v.add(BigInteger.valueOf(delta)),
                date -> date.plus(delta, ChronoUnit.DAYS),
                date -> date.plus(delta, ChronoUnit.SECONDS)
        );
    }

    private Optional<ConstantOperator> computeWithLimits(int delta,
                                                         Function<Byte, Byte> byteFunc,
                                                         Function<Short, Short> smallFunc,
                                                         Function<Integer, Integer> intFunc,
                                                         Function<Long, Long> longFunc,
                                                         Function<BigInteger, BigInteger> bigintFunc,
                                                         Function<LocalDateTime, LocalDateTime> dateFunc,
                                                         Function<LocalDateTime, LocalDateTime> datetimeFunc) {
        if (type.isTinyint()) {
            return compute(delta, getTinyInt(), Byte.MAX_VALUE, Byte.MIN_VALUE, byteFunc, ConstantOperator::createTinyInt);
        } else if (type.isSmallint()) {
            return compute(delta, getSmallint(), Short.MAX_VALUE, Short.MIN_VALUE, smallFunc, ConstantOperator::createSmallInt);
        } else if (type.isInt()) {
            return compute(delta, getInt(), Integer.MAX_VALUE, Integer.MIN_VALUE, intFunc, ConstantOperator::createInt);
        } else if (type.isBigint()) {
            return compute(delta, getBigint(), Long.MAX_VALUE, Long.MIN_VALUE, longFunc, ConstantOperator::createBigint);
        } else if (type.isLargeint()) {
            return compute(delta, getLargeInt(), MAX_LARGE_INT, MIN_LARGE_INT, bigintFunc, ConstantOperator::createLargeInt);
        } else if (type.isDatetime()) {
            return compute(delta, (LocalDateTime) value, LocalDateTime.MAX, LocalDateTime.MIN,
                    datetimeFunc, ConstantOperator::createDatetime);
        } else if (type.isDateType()) {
            return compute(delta, (LocalDateTime) value, LocalDate.MAX.atStartOfDay(), LocalDate.MIN.atStartOfDay(),
                    dateFunc, ConstantOperator::createDate);
        } else {
            return Optional.empty();
        }
    }

    private <T> Optional<ConstantOperator> compute(int delta,
            T value, T maxValue, T minValue, Function<T, T> func, Function<T, ConstantOperator> creator) {
        if ((delta > 0 && value.equals(maxValue)) || (delta < 0 && value.equals(minValue))) {
            return Optional.empty();
        } else {
            return Optional.of(creator.apply(func.apply(value)));
        }
    }

    public long distance(ConstantOperator other) {
        if (type.isTinyint()) {
            return other.getTinyInt() - getTinyInt();
        } else if (type.isSmallint()) {
            return other.getSmallint() - getSmallint();
        } else if (type.isInt()) {
            return other.getInt() - getInt();
        } else if (type.isBigint()) {
            return other.getBigint() - getBigint();
        } else if (type.isLargeint()) {
            return other.getLargeInt().subtract(getLargeInt()).longValue();
        } else if (type.isDatetime()) {
            return ChronoUnit.SECONDS.between(getDatetime(), other.getDatetime());
        } else if (type.isDateType()) {
            return ChronoUnit.DAYS.between(getDatetime(), other.getDatetime());
        } else {
            throw UnsupportedException.unsupportedException("unsupported distince for type:" + type);
        }
    }
}
