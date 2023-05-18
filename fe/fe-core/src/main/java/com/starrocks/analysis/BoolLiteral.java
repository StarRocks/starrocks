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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/BoolLiteral.java

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

import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TBoolLiteral;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

public class BoolLiteral extends LiteralExpr {
    private boolean value;

    private BoolLiteral() {
    }

    public BoolLiteral(boolean value) {
        this(value, NodePosition.ZERO);
    }

    public BoolLiteral(boolean value, NodePosition pos) {
        super(pos);
        this.value = value;
        type = Type.BOOLEAN;
    }

    public BoolLiteral(String value) throws AnalysisException {
        this(value, NodePosition.ZERO);
    }

    public BoolLiteral(String value, NodePosition pos) throws AnalysisException {
        super(pos);
        this.type = Type.BOOLEAN;
        if (value.trim().equalsIgnoreCase("true") || value.trim().equals("1")) {
            this.value = true;
        } else if (value.trim().equalsIgnoreCase("false") || value.trim().equals("0")) {
            this.value = false;
        } else {
            throw new AnalysisException("Invalid BOOLEAN literal: " + value);
        }
    }

    protected BoolLiteral(BoolLiteral other) {
        super(other);
        value = other.value;
    }

    @Override
    public Expr clone() {
        return new BoolLiteral(this);
    }

    @Override
    public ByteBuffer getHashValue(Type type) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        if (type.getPrimitiveType() == PrimitiveType.BOOLEAN) {
            if (value) {
                buffer.put((byte) 1);
            } else {
                buffer.put((byte) 0);
            }
        } else {
            return super.getHashValue(type);
        }
        buffer.flip();
        return buffer;
    }

    public boolean getValue() {
        return value;
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
        return Long.signum(getLongValue() - expr.getLongValue());
    }

    @Override
    public String toSqlImpl() {
        return value ? "TRUE" : "FALSE";
    }

    @Override
    public Object getRealObjectValue() {
        return value;
    }

    @Override
    public String getStringValue() {
        return value ? "TRUE" : "FALSE";
    }

    @Override
    public long getLongValue() {
        return value ? 1 : 0;
    }

    @Override
    public double getDoubleValue() {
        return value ? 1.0 : 0.0;
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.BOOL_LITERAL;
        msg.bool_literal = new TBoolLiteral(value);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeBoolean(value);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        value = in.readBoolean();
    }

    public static BoolLiteral read(DataInput in) throws IOException {
        BoolLiteral literal = new BoolLiteral();
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
