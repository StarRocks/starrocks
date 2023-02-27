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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/FloatLiteral.java

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

import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.NotImplementedException;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TFloatLiteral;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

public class FloatLiteral extends LiteralExpr {
    private double value;

    public FloatLiteral() {
    }

    public FloatLiteral(Double value) throws AnalysisException {
        checkValue(value);
        init(value);
    }

    /**
     * C'tor forcing type, e.g., due to implicit cast
     */
    public FloatLiteral(Double value, Type type) throws AnalysisException {
        this.value = value;
        this.type = type;
        checkValue(value);
        analysisDone();
    }

    public FloatLiteral(String value) throws AnalysisException {
        this(value, NodePosition.ZERO);
    }

    public FloatLiteral(String value, NodePosition pos) throws AnalysisException {
        super(pos);
        Double floatValue = null;
        try {
            floatValue = new Double(value);
            checkValue(floatValue);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid floating-point literal: " + value, e);
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
        if (fvalue == this.value) {
            type = Type.FLOAT;
        } else {
            type = Type.DOUBLE;
        }
    }

    private void checkValue(Double value) throws AnalysisException {
        if (value.isInfinite() || value.isNaN()) {
            throw new AnalysisException("Invalid literal:" + value);
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
    public String toSqlImpl() {
        return getStringValue();
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

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.FLOAT_LITERAL;
        msg.float_literal = new TFloatLiteral(value);
    }

    public double getValue() {
        return value;
    }

    @Override
    public Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        if (!(targetType.isFloatingPointType() || targetType.isDecimalOfAnyVersion())) {
            return super.uncheckedCastTo(targetType);
        }
        if (targetType.isFloatingPointType()) {
            if (!type.equals(targetType)) {
                FloatLiteral floatLiteral = new FloatLiteral(this);
                floatLiteral.setType(targetType);
                return floatLiteral;
            }
            return this;
        } else if (targetType.isDecimalOfAnyVersion()) {
            DecimalLiteral decimalLiteral = new DecimalLiteral(new BigDecimal(Double.toString(value)));
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
        out.writeDouble(value);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        value = in.readDouble();
    }

    public static FloatLiteral read(DataInput in) throws IOException {
        FloatLiteral literal = new FloatLiteral();
        literal.readFields(in);
        return literal;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), value);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
}

