// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.operator.scalar;

import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.DecimalLiteral;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

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

    private static void requiredValid(LocalDateTime dateTime) throws SemanticException {
        if (null == dateTime || dateTime.isBefore(MIN_DATETIME) || dateTime.isAfter(MAX_DATETIME)) {
            throw new SemanticException("Invalid date value");
        }
    }

    private final Object value;
    private final boolean isNull;

    private ConstantOperator(Type type) {
        super(OperatorType.CONSTANT, type);
        this.value = null;
        this.isNull = true;
    }

    private ConstantOperator(Object value, Type type) {
        super(OperatorType.CONSTANT, type);
        Objects.requireNonNull(value, "constant value is null");
        this.value = value;
        this.isNull = false;
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

    public static ConstantOperator createFloat(double value) {
        return new ConstantOperator(value, Type.FLOAT);
    }

    public static ConstantOperator createDouble(double value) {
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

    public boolean isNull() {
        return isNull;
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

    @Override
    public String toString() {
        if (type.isDatetime()) {
            LocalDateTime time = (LocalDateTime) Optional.ofNullable(value).orElse(LocalDateTime.MIN);
            return String.format("%04d-%02d-%02d %02d:%02d:%02d",
                    time.getYear(), time.getMonthValue(), time.getDayOfMonth(),
                    time.getHour(), time.getMinute(), time.getSecond());
        } else if (type.isDate()) {
            LocalDateTime time = (LocalDateTime) Optional.ofNullable(value).orElse(LocalDateTime.MIN);
            return String.format("%04d-%02d-%02d", time.getYear(), time.getMonthValue(), time.getDayOfMonth());
        }

        return String.valueOf(value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, type, isNull);
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
                Objects.equals(type, that.type);
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
        if (t != o.getType().getPrimitiveType()) {
            throw new StarRocksPlannerException(
                    "Constant " + this.toString() + " can't compare with Constant " + o.toString(),
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
        return type.equals(Type.NULL);
    }

    public ConstantOperator castTo(Type desc) throws Exception {
        if (type.isTime() || desc.isTime()) {
            // Don't support constant time cast in FE
            throw UnsupportedException
                    .unsupportedException(toString() + " cast to " + desc.getPrimitiveType().toString());
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
            return ConstantOperator.createTinyInt(Byte.parseByte(childString));
        } else if (desc.isSmallint()) {
            return ConstantOperator.createSmallInt(Short.parseShort(childString));
        } else if (desc.isInt()) {
            return ConstantOperator.createInt(Integer.parseInt(childString));
        } else if (desc.isBigint()) {
            return ConstantOperator.createBigint(Long.parseLong(childString));
        } else if (desc.isLargeint()) {
            return ConstantOperator.createLargeInt(new BigInteger(childString));
        } else if (desc.isFloat()) {
            return ConstantOperator.createFloat(Double.parseDouble(childString));
        } else if (desc.isDouble()) {
            return ConstantOperator.createDouble(Double.parseDouble(childString));
        } else if (desc.isDate() || desc.isDatetime()) {
            DateLiteral literal;
            try {
                // DateLiteral will throw Exception if cast failed
                // 1.try cast by format "yyyy-MM-dd HH:mm:ss"
                literal = new DateLiteral(childString, Type.DATETIME);
            } catch (Exception e) {
                // 2.try cast by format "yyyy-MM-dd", will original operator if failed
                literal = new DateLiteral(childString, Type.DATE);
            }

            if (Type.DATE.equals(desc)) {
                literal.castToDate();
            }

            return ConstantOperator.createDatetime(literal.toLocalDateTime(), desc);
        } else if (desc.isDecimalV2()) {
            return ConstantOperator.createDecimal(BigDecimal.valueOf(Double.parseDouble(childString)), Type.DECIMALV2);
        } else if (desc.isDecimalV3()) {
            BigDecimal value = new BigDecimal(childString);
            try {
                ScalarType scalarType = (ScalarType) desc;
                DecimalLiteral.checkLiteralOverflow(value, scalarType);
                if (scalarType.getScalarScale() == 0 && scalarType.getScalarPrecision() == 0) {
                    throw new SemanticException("Forbidden cast to decimal(precision=0, scale=0)");
                }
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }

            return ConstantOperator.createDecimal(value, desc);
        } else if (desc.isChar() || desc.isVarchar()) {
            return ConstantOperator.createChar(childString, desc);
        }

        throw UnsupportedException.unsupportedException(toString() + " cast to " + desc.getPrimitiveType().toString());
    }
}
