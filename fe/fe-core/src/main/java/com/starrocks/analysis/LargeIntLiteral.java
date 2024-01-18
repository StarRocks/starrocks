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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/LargeIntLiteral.java

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
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.common.exception.NotImplementedException;
import com.starrocks.common.io.Text;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TLargeIntLiteral;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

public class LargeIntLiteral extends LiteralExpr {
    // -2^127
    public static final BigInteger LARGE_INT_MIN = new BigInteger("-170141183460469231731687303715884105728");
    // 2^127 - 1
    public static final BigInteger LARGE_INT_MAX = new BigInteger("170141183460469231731687303715884105727");
    // 2^127
    public static final BigInteger LARGE_INT_MAX_ABS = new BigInteger("170141183460469231731687303715884105728");

    private BigInteger value;

    public LargeIntLiteral() {
        super();
        analysisDone();
    }

    public LargeIntLiteral(boolean isMax) throws AnalysisException {
        super();
        type = Type.LARGEINT;
        value = isMax ? LARGE_INT_MAX : LARGE_INT_MIN;
        analysisDone();
    }

    public LargeIntLiteral(String value) throws AnalysisException {
        this(value, NodePosition.ZERO);
    }
    public LargeIntLiteral(String value, NodePosition pos) throws AnalysisException {
        super(pos);
        BigInteger bigInt;
        try {
            bigInt = new BigInteger(value);
            // ATTN: value from 'sql_parser.y' is always be positive. for example: '-256' will to be
            // 256, and for int8_t, 256 is invalid, while -256 is valid. So we check the right border
            // is LARGE_INT_MAX_ABS
            if (bigInt.compareTo(LARGE_INT_MIN) < 0 || bigInt.compareTo(LARGE_INT_MAX_ABS) > 0) {
                throw new AnalysisException("Large int literal is out of range: " + value);
            }
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid integer literal: " + value, e);
        }
        this.value = bigInt;
        type = Type.LARGEINT;
        analysisDone();
    }

    protected LargeIntLiteral(LargeIntLiteral other) {
        super(other);
        value = other.value;
    }

    public BigInteger getValue() {
        return value;
    }

    @Override
    public Expr clone() {
        return new LargeIntLiteral(this);
    }

    public static LargeIntLiteral createMinValue() {
        LargeIntLiteral largeIntLiteral = new LargeIntLiteral();
        largeIntLiteral.type = Type.LARGEINT;
        largeIntLiteral.value = LARGE_INT_MIN;
        return largeIntLiteral;
    }

    public static LargeIntLiteral createMaxValue() {
        LargeIntLiteral largeIntLiteral = new LargeIntLiteral();
        largeIntLiteral.type = Type.LARGEINT;
        largeIntLiteral.value = LARGE_INT_MAX;
        return largeIntLiteral;
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    }

    @Override
    public boolean isMinValue() {
        return this.value.compareTo(LARGE_INT_MIN) == 0;
    }

    @Override
    public Object getRealObjectValue() {
        return this.value;
    }

    // little endian for hash code
    @Override
    public ByteBuffer getHashValue(Type type) {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        byte[] byteArray = value.toByteArray();
        int len = byteArray.length;
        int end = 0;
        if (len > 16) {
            end = len - 16;
        }

        for (int i = len - 1; i >= end; --i) {
            buffer.put(byteArray[i]);
        }
        if (value.signum() >= 0) {
            while (len++ < 16) {
                buffer.put((byte) 0);
            }
        } else {
            while (len++ < 16) {
                buffer.put((byte) 0xFF);
            }
        }

        buffer.flip();
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
        if (expr.type.isLargeint()) {
            return value.compareTo(((LargeIntLiteral) expr).value);
        } else if (expr.type.isFloatingPointType()) {
            return Double.compare(value.doubleValue(), expr.getDoubleValue());
        } else if (expr.type.isDecimalV2()) {
            return new BigDecimal(value).compareTo(((DecimalLiteral) expr).getValue());
        } else if (expr.type.isBoolean()) {
            return value.compareTo(new BigInteger(String.valueOf(expr.getLongValue())));
        } else {
            BigInteger intValue = new BigInteger(expr.getStringValue());
            return value.compareTo(intValue);
        }
    }

    @Override
    public String getStringValue() {
        return value.toString();
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
    public String toSqlImpl() {
        return getStringValue();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.LARGE_INT_LITERAL;
        msg.large_int_literal = new TLargeIntLiteral(value.toString());
    }

    @Override
    public Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        if (targetType.isFloatingPointType()) {
            return new FloatLiteral(value.doubleValue(), targetType);
        } else if (targetType.isDecimalOfAnyVersion()) {
            DecimalLiteral decimalLiteral = new DecimalLiteral(new BigDecimal(value));
            decimalLiteral.type = targetType;
            return decimalLiteral;
        } else if (targetType.isNumericType()) {
            try {
                return new IntLiteral(value.longValueExact(), targetType);
            } catch (ArithmeticException e) {
                throw new AnalysisException("Number out of range[" + value + "]. type: " + targetType);
            }
        }
        return super.uncheckedCastTo(targetType);
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
        value = new BigInteger(Text.readString(in));
    }

    public static LargeIntLiteral read(DataInput in) throws IOException {
        LargeIntLiteral largeIntLiteral = new LargeIntLiteral();
        largeIntLiteral.readFields(in);
        return largeIntLiteral;
    }

    @Override
    public int hashCode() {
        // IntLiteral(0) equals to LargeIntLiteral(0), so their hash codes must equal.
        return Objects.hash(getLongValue());
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
}
