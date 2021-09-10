// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/DecimalLiteral.java

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
import com.google.common.base.Strings;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.NotImplementedException;
import com.starrocks.common.io.Text;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TDecimalLiteral;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

// Our new cost based query optimizer is more powerful and stable than old query optimizer,
// The old query optimizer related codes could be deleted safely.
// TODO: Remove old query optimizer related codes before 2021-09-30
public class DecimalLiteral extends LiteralExpr {
    private BigDecimal value;

    public DecimalLiteral() {
    }

    public DecimalLiteral(BigDecimal value) {
        init(value);
        analysisDone();
    }

    public DecimalLiteral(String value) throws AnalysisException {
        BigDecimal v = null;
        try {
            v = new BigDecimal(value);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid floating-point literal: " + value, e);
        }
        init(v);
        analysisDone();
    }

    public DecimalLiteral(String value, Type type) throws AnalysisException {
        Preconditions.checkArgument(type.isDecimalOfAnyVersion());
        BigDecimal v = null;
        try {
            v = new BigDecimal(value);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid floating-point literal: " + value, e);
        }
        init(v, type);
        analysisDone();
    }

    protected DecimalLiteral(DecimalLiteral other) {
        super(other);
        value = other.value;
    }

    @Override
    public Expr clone() {
        return new DecimalLiteral(this);
    }

    // Precision and scale of BigDecimal is subtly different from Decimal32/64/128.
    // In BigDecimal, the precision is the number of digits in the unscaled BigInteger value.
    // there are two types of subnormal BigDecimal violate the invariants:  0 < P and 0 <= S <= P
    // type 1: 0 <= S but S > P.  i.e. BigDecimal("0.0001"), unscaled BigInteger is 1, the scale is 4
    // type 2: S < 0. i.e. BigDecimal("10000"), unscaled BigInteger is 1, the  scaled is -4
    public static int getRealPrecision(BigDecimal decimal) {
        if (decimal.equals(BigDecimal.ZERO)) {
            return 0;
        }
        int scale = decimal.scale();
        int precision = decimal.precision();
        if (scale < 0) {
            return Math.abs(scale) + precision;
        } else {
            return Math.max(scale, precision);
        }
    }

    // An integer that has trailing zeros represented by BigDecimal with negative scale, i.e.
    // BigDecimal("20000"):  unscaled integer is 2, the scale is -5.
    public static int getRealScale(BigDecimal decimal) {
        if (decimal.equals(BigDecimal.ZERO)) {
            return 0;
        }
        return Math.max(0, decimal.scale());
    }

    private void init(BigDecimal value) {
        // Currently, our storage engine doesn't support scientific notation.
        // So we remove exponent field here.
        this.value = new BigDecimal(value.toPlainString());

        ConnectContext ctx = ConnectContext.get();
        if (!Config.enable_decimal_v3) {
            type = ScalarType.DECIMALV2;
        } else {
            this.value = value.stripTrailingZeros();
            int precision = getRealPrecision(this.value);
            int scale = getRealScale(this.value);
            int integerPartWidth = precision - scale;
            int maxIntegerPartWidth = 38;
            // integer part of decimal literal should not exceed 38
            if (integerPartWidth > maxIntegerPartWidth) {
                String errMsg = String.format(
                        "Non-typed decimal literal is overflow, value='%s' (precision=%d, scale=%d)",
                        value.toPlainString(), precision, scale);
                throw new InternalError(errMsg);
            }
            // round to low-resolution decimal if decimal literal's resolution is too high
            scale = Math.min(maxIntegerPartWidth - integerPartWidth, scale);
            precision = integerPartWidth + scale;
            this.value = this.value.setScale(scale, RoundingMode.HALF_UP);
            type = ScalarType.createDecimalV3NarrowestType(precision, scale);
        }
    }

    private void init(BigDecimal value, Type type) throws AnalysisException {
        Preconditions.checkArgument(type.isDecimalOfAnyVersion());
        ScalarType scalarType = (ScalarType) type;
        this.value = new BigDecimal(value.toPlainString());
        if (type.isDecimalV3()) {
            this.value = value.stripTrailingZeros();
            int precision = scalarType.getScalarPrecision();
            int scale = scalarType.getScalarScale();
            int realPrecision = getRealPrecision(this.value);
            int realScale = getRealScale(this.value);
            int realIntegerPartWidth = realPrecision - realScale;
            int maxIntegerPartWidth = precision - scale;
            // integer part of decimal literal should not exceed precision - scale
            if (realIntegerPartWidth > maxIntegerPartWidth) {
                String errMsg = String.format(
                        "Typed decimal literal(%s) is overflow, value='%s' (precision=%d, scale=%d)",
                        type.toString(), value.toPlainString(), realPrecision, realScale);
                throw new AnalysisException(errMsg);
            }
            realScale = Math.min(scale, realScale);
            realPrecision = realIntegerPartWidth + realScale;
            // round
            this.value = this.value.setScale(realScale, RoundingMode.HALF_UP);
            this.type = ScalarType.createDecimalV3NarrowestType(realPrecision, realScale);
        } else {
            this.type = type;
        }
    }

    public BigDecimal getValue() {
        return value;
    }

    public void checkPrecisionAndScale(int precision, int scale) throws AnalysisException {
        Preconditions.checkNotNull(this.value);
        boolean valid = true;
        int realPrecision = getRealPrecision(this.value);
        int realScale = getRealScale(this.value);
        if (precision != -1 && scale != -1) {
            if (precision < realPrecision || scale < realScale) {
                valid = false;
            }
        } else {
            valid = false;
        }

        if (!valid) {
            String errMsg = String.format(
                    "Type %s is too narrow to hold the DecimalLiteral '%s' (precision=%d, scale=%d)",
                    type.toString(), value.toPlainString(), realPrecision, realScale);
            throw new AnalysisException(errMsg);
        }
    }

    @Override
    public boolean isMinValue() {
        return false;
    }

    final private static BigDecimal[] SCALE_FACTOR = new BigDecimal[39];

    static {
        for (int i = 0; i < 39; ++i) {
            SCALE_FACTOR[i] = new BigDecimal("1" + Strings.repeat("0", i));
        }
    }

    private ByteBuffer getHashValueOfDecimalV2() {
        ByteBuffer buffer = ByteBuffer.allocate(12);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        long integerValue = value.longValue();
        int fracValue = getFracValue();
        buffer.putLong(integerValue);
        buffer.putInt(fracValue);
        return buffer;
    }

    private void checkType(Type type) {
        ScalarType scalarType = (ScalarType) type;
        int precision = scalarType.getScalarPrecision();
        int scale = scalarType.getScalarScale();
        try {
            checkPrecisionAndScale(precision, scale);
        } catch (AnalysisException e) {
            throw new InternalError(e.getMessage());
        }
    }

    @Override
    public ByteBuffer getHashValue(Type type) {
        ByteBuffer buffer;
        // no need to consider the overflow when cast decimal to other type, because this func only be used when querying, not storing.
        // e.g. For column A with type INT, the data stored certainly no overflow.
        switch (type.getPrimitiveType()) {
            case TINYINT:
                buffer = ByteBuffer.allocate(8);
                buffer.order(ByteOrder.LITTLE_ENDIAN);
                buffer.put(value.byteValue());
                break;
            case SMALLINT:
                buffer = ByteBuffer.allocate(8);
                buffer.order(ByteOrder.LITTLE_ENDIAN);
                buffer.putShort(value.shortValue());
                break;
            case INT:
                buffer = ByteBuffer.allocate(8);
                buffer.order(ByteOrder.LITTLE_ENDIAN);
                buffer.putInt(value.intValue());
                break;
            case BIGINT:
                buffer = ByteBuffer.allocate(8);
                buffer.order(ByteOrder.LITTLE_ENDIAN);
                buffer.putLong(value.longValue());
                break;
            case DECIMALV2:
                buffer = getHashValueOfDecimalV2();
                break;
            case DECIMAL32:
            case DECIMAL64: {
                checkType(type);
                buffer = ByteBuffer.allocate(8);
                buffer.order(ByteOrder.LITTLE_ENDIAN);
                int scale = ((ScalarType) type).getScalarScale();
                BigDecimal scaledValue = value.multiply(SCALE_FACTOR[scale]);
                if (type.getPrimitiveType() == PrimitiveType.DECIMAL32) {
                    buffer.putInt(scaledValue.intValue());
                } else {
                    buffer.putLong(scaledValue.longValue());
                }
                break;
            }
            case DECIMAL128: {
                checkType(type);
                int precision = ((ScalarType) type).getScalarPrecision();
                int scale = ((ScalarType) type).getScalarScale();
                if (precision == 27 && scale == 9) {
                    buffer = getHashValueOfDecimalV2();
                } else {
                    BigDecimal scaledValue = value.multiply(SCALE_FACTOR[scale]);
                    try {
                        LargeIntLiteral largeIntLiteral = new LargeIntLiteral(scaledValue.toBigInteger().toString());
                        return largeIntLiteral.getHashValue(Type.LARGEINT);
                    } catch (AnalysisException e) {
                        throw new InternalError(e.getMessage());
                    }
                }
                break;
            }
            default:
                return super.getHashValue(type);
        }
        buffer.flip();
        return buffer;
    }

    @Override
    public Object getRealValue() {
        return value;
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        if (expr instanceof NullLiteral) {
            return 1;
        }
        return this.value.compareTo(((DecimalLiteral) expr).value);
    }

    @Override
    public String toSqlImpl() {
        return getStringValue();
    }

    @Override
    public String getStringValue() {
        // use BigDecimal.toPlainString() instead of BigDecimal.toString()
        // to avoid outputting scientific representation which cannot be
        // parsed in BE that uses regex to validation decimals in string format.
        return value.toPlainString();
    }

    @Override
    public long getLongValue() {
        return value.longValue();
    }

    @Override
    public double getDoubleValue() {
        return value.doubleValue();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        // TODO(hujie01) deal with loss information
        msg.node_type = TExprNodeType.DECIMAL_LITERAL;
        BigDecimal v = new BigDecimal(value.toBigInteger());
        msg.decimal_literal = new TDecimalLiteral(value.toPlainString());
    }

    @Override
    public void swapSign() throws NotImplementedException {
        // swapping sign does not change the type
        value = value.negate();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, value.toString());
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        value = new BigDecimal(Text.readString(in));
    }

    public static DecimalLiteral read(DataInput in) throws IOException {
        DecimalLiteral dec = new DecimalLiteral();
        dec.readFields(in);
        return dec;
    }

    // To be compatible with OLAP, only need 9 digits.
    // Note: the return value is negative if value is negative.
    public int getFracValue() {
        BigDecimal integerPart = new BigDecimal(value.toBigInteger());
        BigDecimal fracPart = value.subtract(integerPart);
        fracPart = fracPart.setScale(9, BigDecimal.ROUND_DOWN);
        fracPart = fracPart.movePointRight(9);

        return fracPart.intValue();
    }

    public static void checkLiteralOverflow(BigDecimal value, ScalarType scalarType) throws AnalysisException {
        int realPrecision = getRealPrecision(value);
        int realScale = getRealScale(value);
        int realIntegerPartWidth = realPrecision - realScale;
        int precision = scalarType.getScalarPrecision();
        int scale = scalarType.getScalarScale();
        int maxIntegerPartWidth = precision - scale;
        // integer part of decimal literal should not exceed precision - scale
        if (realIntegerPartWidth > maxIntegerPartWidth) {
            String errMsg = String.format(
                    "Typed decimal literal(%s) is overflow, value='%s' (precision=%d, scale=%d)",
                    scalarType.toString(), value.toPlainString(), realPrecision, realScale);
            throw new AnalysisException(errMsg);
        }
    }

    @Override
    public Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        if (targetType.getPrimitiveType().isDecimalV3Type()) {
            this.type = targetType;
            checkLiteralOverflow(this.value, (ScalarType) targetType);
            // round
            int realScale = getRealScale(value);
            int scale = ((ScalarType) targetType).getScalarScale();
            if (scale <= realScale) {
                this.value = this.value.setScale(scale, RoundingMode.HALF_UP);
            }
            return this;
        } else if (targetType.getPrimitiveType().isDecimalV2Type()) {
            this.type = targetType;
            return this;
        } else if (targetType.isFloatingPointType()) {
            return new FloatLiteral(value.doubleValue(), targetType);
        } else if (targetType.isIntegerType()) {
            return new IntLiteral(value.longValue(), targetType);
        } else if (targetType.isStringType()) {
            return new StringLiteral(value.toString());
        }
        return super.uncheckedCastTo(targetType);
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hashCode(value);
    }
}
