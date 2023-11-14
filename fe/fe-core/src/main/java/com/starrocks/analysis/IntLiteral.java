// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/IntLiteral.java

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
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.NotImplementedException;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TIntLiteral;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

public class IntLiteral extends LiteralExpr {
    public static final long TINY_INT_MIN = Byte.MIN_VALUE; // -2^7 ~ 2^7 - 1
    public static final long TINY_INT_MAX = Byte.MAX_VALUE;
    public static final long SMALL_INT_MIN = Short.MIN_VALUE; // -2^15 ~ 2^15 - 1
    public static final long SMALL_INT_MAX = Short.MAX_VALUE;
    public static final long INT_MIN = Integer.MIN_VALUE; // -2^31 ~ 2^31 - 1
    public static final long INT_MAX = Integer.MAX_VALUE;
    public static final long BIG_INT_MIN = Long.MIN_VALUE; // -2^63 ~ 2^63 - 1
    public static final long BIG_INT_MAX = Long.MAX_VALUE;
    private long value;

    private String stringValue = null;

    /**
     * C'tor forcing type, e.g., due to implicit cast
     */
    // for restore
    private IntLiteral() {
    }

    public IntLiteral(long value) {
        super();
        init(value);
        analysisDone();
    }

    public IntLiteral(long longValue, Type type) {
        super();
        boolean valid = true;
        switch (type.getPrimitiveType()) {
            case TINYINT:
                if (longValue < TINY_INT_MIN || longValue > TINY_INT_MAX) {
                    valid = false;
                }
                break;
            case SMALLINT:
                if (longValue < SMALL_INT_MIN || longValue > SMALL_INT_MAX) {
                    valid = false;
                }
                break;
            case INT:
                if (longValue < INT_MIN || longValue > INT_MAX) {
                    valid = false;
                }
                break;
            case BIGINT:
                // no need to check upper bound
                break;
            default:
                valid = false;
                break;
        }

        if (!valid) {
            throw new ArithmeticException("Number out of range[" + value + "]. type: " + type);
        }

        this.value = longValue;
        this.type = type;
        analysisDone();
    }

    public IntLiteral(String value, Type type) throws AnalysisException {
        super();
        long longValue = -1L;
        try {
            longValue = Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid number format: " + value);
        }

        boolean valid = true;
        switch (type.getPrimitiveType()) {
            case TINYINT:
                if (longValue < TINY_INT_MIN || longValue > TINY_INT_MAX) {
                    valid = false;
                }
                break;
            case SMALLINT:
                if (longValue < SMALL_INT_MIN || longValue > SMALL_INT_MAX) {
                    valid = false;
                }
                break;
            case INT:
                if (longValue < INT_MIN || longValue > INT_MAX) {
                    valid = false;
                }
                break;
            case BIGINT:
                // no need to check upper bound
                break;
            default:
                valid = false;
                break;
        }

        if (!valid) {
            throw new AnalysisException("Number out of range[" + value + "]. type: " + type);
        }

        this.value = longValue;
        this.type = type;
        this.stringValue = value;
        analysisDone();
    }

    protected IntLiteral(IntLiteral other) {
        super(other);
        value = other.value;
    }

    @Override
    public Expr clone() {
        return new IntLiteral(this);
    }

    private void init(long value) {
        this.value = value;
        if (this.value <= TINY_INT_MAX && this.value >= TINY_INT_MIN) {
            type = Type.TINYINT;
        } else if (this.value <= SMALL_INT_MAX && this.value >= SMALL_INT_MIN) {
            type = Type.SMALLINT;
        } else if (this.value <= INT_MAX && this.value >= INT_MIN) {
            type = Type.INT;
        } else {
            type = Type.BIGINT;
        }
    }

    public static IntLiteral createMinValue(Type type) {
        long value = 0L;
        switch (type.getPrimitiveType()) {
            case TINYINT:
                value = TINY_INT_MIN;
                break;
            case SMALLINT:
                value = SMALL_INT_MIN;
                break;
            case INT:
                value = INT_MIN;
                break;
            case BIGINT:
                value = BIG_INT_MIN;
                break;
            default:
                Preconditions.checkState(false);
        }

        return new IntLiteral(value);
    }

    public static IntLiteral createMaxValue(Type type) {
        long value = 0L;
        switch (type.getPrimitiveType()) {
            case TINYINT:
                value = TINY_INT_MAX;
                break;
            case SMALLINT:
                value = SMALL_INT_MAX;
                break;
            case INT:
                value = INT_MAX;
                break;
            case BIGINT:
                value = BIG_INT_MAX;
                break;
            default:
                Preconditions.checkState(false);
        }

        return new IntLiteral(value);
    }

    public static IntLiteral read(DataInput in) throws IOException {
        IntLiteral literal = new IntLiteral();
        literal.readFields(in);
        return literal;
    }

    @Override
    public boolean isMinValue() {
        switch (type.getPrimitiveType()) {
            case TINYINT:
                return this.value == TINY_INT_MIN;
            case SMALLINT:
                return this.value == SMALL_INT_MIN;
            case INT:
                return this.value == INT_MIN;
            case BIGINT:
                return this.value == BIG_INT_MIN;
            default:
                return false;
        }
    }

    @Override
    public ByteBuffer getHashValue(Type type) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        switch (type.getPrimitiveType()) {
            case TINYINT:
                buffer.put((byte) value);
                break;
            case SMALLINT:
                buffer.putShort((short) value);
                break;
            case INT:
                buffer.putInt((int) value);
                break;
            case BIGINT:
                buffer.putLong(value);
                break;
            default:
                break;
        }
        buffer.flip();
        return buffer;
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        if (expr instanceof NullLiteral) {
            return 1;
        }
        if (expr instanceof StringLiteral) {
            return expr.compareLiteral(this);
        }
        if (expr == MaxLiteral.MAX_VALUE) {
            return -1;
        }
        if (value == expr.getLongValue()) {
            return 0;
        } else {
            return value > expr.getLongValue() ? 1 : -1;
        }
    }

    @Override
    public Object getRealObjectValue() {
        switch (type.getPrimitiveType()) {
            case TINYINT:
                return (byte) value;
            case SMALLINT:
                return (short) value;
            case INT:
                return (int) value;
            case BIGINT:
                return value;
            default:
                throw new StarRocksPlannerException("Error int literal type " + type.getPrimitiveType(),
                        ErrorType.INTERNAL_ERROR);
        }
    }

    public long getValue() {
        return value;
    }

    @Override
    public String getStringValue() {
        return stringValue != null ? stringValue : Long.toString(value);
    }

    @Override
    public long getLongValue() {
        return value;
    }

    @Override
    public double getDoubleValue() {
        return (double) value;
    }

    @Override
    public String toSqlImpl() {
        return getStringValue();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.INT_LITERAL;
        msg.int_literal = new TIntLiteral(value);
    }

    @Override
    public Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        if (!targetType.isNumericType()) {
            return super.uncheckedCastTo(targetType);
        }
        if (targetType.isFixedPointType()) {
            if (!targetType.isLargeint()) {
                if (!type.equals(targetType)) {
                    // When cast large int type to small int type, must add cast expr
                    if (type.getPrimitiveType().ordinal() > targetType.getPrimitiveType().ordinal()) {
                        return super.uncheckedCastTo(targetType);
                    } else {
                        IntLiteral intLiteral = new IntLiteral(this);
                        intLiteral.setType(targetType);
                        return intLiteral;
                    }
                }
                return this;
            } else {
                return new LargeIntLiteral(Long.toString(value));
            }
        } else if (targetType.isFloatingPointType()) {
            return new FloatLiteral((double) value, targetType);
        } else if (targetType.isDecimalOfAnyVersion()) {
            DecimalLiteral decimalLiteral = new DecimalLiteral(new BigDecimal(value));
            decimalLiteral.type = targetType;
            return decimalLiteral;
        }
        return this;
    }

    @Override
    public void swapSign() throws NotImplementedException {
        // swapping sign does not change the type
        value = -value;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeLong(value);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        value = in.readLong();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), value);
    }
}
