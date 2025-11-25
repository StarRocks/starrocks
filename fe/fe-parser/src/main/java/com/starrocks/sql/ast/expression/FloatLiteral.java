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
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.type.FloatType;
import com.starrocks.type.Type;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

public class FloatLiteral extends LiteralExpr {
    private double value;

    public FloatLiteral() {
    }

    public FloatLiteral(Double value) {
        checkValue(value);
        init(value);
    }

    /**
     * C'tor forcing type, e.g., due to implicit cast
     */
    public FloatLiteral(Double value, Type type) {
        this.value = value;
        this.type = type;
        checkValue(value);
        analysisDone();
    }

    public FloatLiteral(String value) {
        this(value, NodePosition.ZERO);
    }

    public FloatLiteral(String value, Type type) {
        this(value, NodePosition.ZERO);
        this.type = type;
    }

    public FloatLiteral(String value, NodePosition pos) {
        super(pos);
        Double floatValue = null;
        try {
            floatValue = Double.valueOf(value);
            checkValue(floatValue);
        } catch (NumberFormatException e) {
            throw new ParsingException("Invalid floating-point literal: " + value, e);
        }
        init(floatValue);
    }

    protected FloatLiteral(FloatLiteral other) {
        super(other);
        value = other.value;
    }

    @Override
    public Expr clone() {
        return new FloatLiteral(this);
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
                buffer.putLong((long) value);
                break;
            default:
                return super.getHashValue(type);
        }
        buffer.flip();
        return buffer;
    }

    private void init(Double value) {
        this.value = value;
        // Figure out if this will fit in a FLOAT without loosing precision.
        float fvalue;
        fvalue = value.floatValue();
        type = Float.toString(fvalue).equals(Double.toString(value)) ? FloatType.FLOAT : FloatType.DOUBLE;
    }

    private void checkValue(Double value) {
        if (value.isInfinite() || value.isNaN()) {
            throw new ParsingException("Invalid literal:" + value);
        }
    }

    @Override
    public boolean isMinValue() {
        return false;
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        if (expr instanceof NullLiteral) {
            return 1;
        }
        return Double.compare(value, expr.getDoubleValue());
    }

    @Override
    public String getStringValue() {
        return Double.toString(value);
    }

    @Override
    public long getLongValue() {
        return (long) value;
    }

    @Override
    public double getDoubleValue() {
        return value;
    }

    @Override
    public Object getRealObjectValue() {
        return value;
    }

    public double getValue() {
        return value;
    }

    @Override
    public void swapSign() throws UnsupportedOperationException {
        // swapping sign does not change the type
        value = -value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), value);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitFloatLiteral(this, context);
    }
}
