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

package com.starrocks.planner.expression;

import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.thrift.TBinaryLiteral;
import com.starrocks.thrift.TBoolLiteral;
import com.starrocks.thrift.TDateLiteral;
import com.starrocks.thrift.TDecimalLiteral;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TFloatLiteral;
import com.starrocks.thrift.TIntLiteral;
import com.starrocks.thrift.TLargeIntLiteral;
import com.starrocks.thrift.TStringLiteral;
import com.starrocks.type.Type;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;

/**
 * Unified execution-plan literal. Holds a {@link ConstantOperator} and
 * dispatches Thrift serialization based on the value's type.
 */
public class ExecLiteral extends ExecExpr {
    private final ConstantOperator value;

    public ExecLiteral(ConstantOperator value, Type type) {
        super(type);
        this.value = value;
    }

    private ExecLiteral(ExecLiteral other) {
        super(other.type);
        this.value = other.value;
        this.originType = other.originType;
        this.isIndexOnlyFilter = other.isIndexOnlyFilter;
    }

    public ConstantOperator getValue() {
        return value;
    }

    @Override
    public boolean isNullable() {
        return value.isNull();
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    @Override
    public boolean isSelfMonotonic() {
        return true;
    }

    @Override
    public TExprNodeType getNodeType() {
        if (value.isNull()) {
            return TExprNodeType.NULL_LITERAL;
        }
        Type t = type;
        if (t.isBoolean()) {
            return TExprNodeType.BOOL_LITERAL;
        } else if (t.isIntegerType() || t.isTinyint() || t.isSmallint()) {
            return TExprNodeType.INT_LITERAL;
        } else if (t.isLargeint()) {
            return TExprNodeType.LARGE_INT_LITERAL;
        } else if (t.isFloatingPointType() || t.isTime()) {
            return TExprNodeType.FLOAT_LITERAL;
        } else if (t.isStringType() || t.isChar() || t.isVarchar()) {
            return TExprNodeType.STRING_LITERAL;
        } else if (t.isDateType()) {
            return TExprNodeType.DATE_LITERAL;
        } else if (t.isDecimalOfAnyVersion()) {
            return TExprNodeType.DECIMAL_LITERAL;
        } else if (t.isBinaryType()) {
            return TExprNodeType.BINARY_LITERAL;
        }
        return TExprNodeType.NULL_LITERAL;
    }

    @Override
    public void toThrift(TExprNode node) {
        if (value.isNull()) {
            // NULL_LITERAL has no extra fields
            return;
        }
        Type t = type;
        if (t.isBoolean()) {
            node.bool_literal = new TBoolLiteral(value.getBoolean());
        } else if (t.isTinyint()) {
            node.int_literal = new TIntLiteral(value.getTinyInt());
        } else if (t.isSmallint()) {
            node.int_literal = new TIntLiteral(value.getSmallint());
        } else if (t.isInt()) {
            node.int_literal = new TIntLiteral(value.getInt());
        } else if (t.isBigint()) {
            node.int_literal = new TIntLiteral(value.getBigint());
        } else if (t.isLargeint()) {
            node.large_int_literal = new TLargeIntLiteral(value.getLargeInt().toString());
        } else if (t.isFloatingPointType() || t.isTime()) {
            node.float_literal = new TFloatLiteral(value.getDouble());
        } else if (t.isStringType() || t.isChar() || t.isVarchar()) {
            node.string_literal = new TStringLiteral(value.getVarchar());
        } else if (t.isDateType()) {
            LocalDateTime dt = value.getDatetime();
            node.date_literal = new TDateLiteral(formatDateLiteral(dt, t));
        } else if (t.isDecimalOfAnyVersion()) {
            TDecimalLiteral decimalLiteral = new TDecimalLiteral();
            decimalLiteral.setValue(value.getDecimal().toPlainString());
            // Pack decimal as byte array for efficient transfer
            decimalLiteral.setInteger_value(packDecimal(value));
            node.decimal_literal = decimalLiteral;
        } else if (t.isBinaryType()) {
            node.binary_literal = new TBinaryLiteral(ByteBuffer.wrap(value.getBinary()));
        }
    }

    private static String formatDateLiteral(LocalDateTime dt, Type type) {
        if (type.isDate()) {
            return String.format("%04d-%02d-%02d", dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth());
        } else {
            return String.format("%04d-%02d-%02d %02d:%02d:%02d",
                    dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(),
                    dt.getHour(), dt.getMinute(), dt.getSecond());
        }
    }

    private byte[] packDecimal(ConstantOperator value) {
        // Match DecimalLiteral.packDecimal() behavior: allocate type-sized buffer,
        // scale the value, and pack in little-endian order.
        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(type.getTypeSize());
        buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        int scale = ((com.starrocks.type.ScalarType) type).getScalarScale();
        java.math.BigDecimal scaledValue = value.getDecimal()
                .multiply(java.math.BigDecimal.TEN.pow(scale));
        switch (type.getPrimitiveType()) {
            case DECIMAL32:
                buffer.putInt(scaledValue.intValue());
                break;
            case DECIMAL64:
                buffer.putLong(scaledValue.longValue());
                break;
            default: // DECIMAL128, DECIMAL256, DECIMALV2
                byte[] bytes = scaledValue.toBigInteger().toByteArray();
                // BigInteger is big-endian, copy in reverse for little-endian
                for (int i = 0; i < buffer.capacity(); i++) {
                    int srcIdx = bytes.length - 1 - i;
                    buffer.put(i, srcIdx >= 0 ? bytes[srcIdx] : (bytes[0] < 0 ? (byte) -1 : 0));
                }
                break;
        }
        return buffer.array();
    }

    @Override
    public <R, C> R accept(ExecExprVisitor<R, C> visitor, C context) {
        return visitor.visitExecLiteral(this, context);
    }

    @Override
    public ExecLiteral clone() {
        return new ExecLiteral(this);
    }
}
