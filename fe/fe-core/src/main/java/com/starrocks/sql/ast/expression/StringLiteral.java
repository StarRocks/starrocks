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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/StringLiteral.java

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

package com.starrocks.sql.ast.expression;

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.type.VarcharType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class StringLiteral extends LiteralExpr {
    protected String value;

    protected String sqlStr;

    public StringLiteral() {
        super(NodePosition.ZERO);
        type = VarcharType.VARCHAR;
    }

    public StringLiteral(String value) {
        this(value, NodePosition.ZERO);
    }

    public StringLiteral(String value, NodePosition pos) {
        super(pos);
        this.value = value;
        type = VarcharType.VARCHAR;
        analysisDone();
    }

    protected StringLiteral(StringLiteral other) {
        super(other);
        value = other.value;
    }

    @Override
    public Expr clone() {
        return new StringLiteral(this);
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        if (expr instanceof NullLiteral) {
            return 1;
        }
        if (expr == MaxLiteral.MAX_VALUE) {
            return -1;
        }
        // compare string with utf-8 byte array, same with DM,BE,StorageEngine
        byte[] thisBytes = null;
        byte[] otherBytes = null;
        thisBytes = value.getBytes(StandardCharsets.UTF_8);
        otherBytes = expr.getStringValue().getBytes(StandardCharsets.UTF_8);

        int minLength = Math.min(thisBytes.length, otherBytes.length);
        int i;
        for (i = 0; i < minLength; i++) {
            if (thisBytes[i] < otherBytes[i]) {
                return -1;
            } else if (thisBytes[i] > otherBytes[i]) {
                return 1;
            }
        }
        if (thisBytes.length > otherBytes.length) {
            if (thisBytes[i] == 0x00) {
                return 0;
            } else {
                return 1;
            }
        } else if (thisBytes.length < otherBytes.length) {
            if (otherBytes[i] == 0x00) {
                return 0;
            } else {
                return -1;
            }
        } else {
            return 0;
        }
    }

    @Override
    public Object getRealObjectValue() {
        return value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean isMinValue() {
        return false;
    }


    // FIXME: modify by zhaochun
    public String getUnescapedValue() {
        // Unescape string exactly like Hive does. Hive's method assumes
        // quotes so we add them here to reuse Hive's code.
        return value;
    }

    @Override
    public String getStringValue() {
        return value;
    }

    @Override
    public long getLongValue() {
        return Long.parseLong(value);
    }

    @Override
    public double getDoubleValue() {
        return Double.parseDouble(value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), value);
    }

    @Override
    public void parseMysqlParam(ByteBuffer data) {
        int strLen = getParamLen(data);
        if (strLen > data.remaining()) {
            strLen = data.remaining();
        }
        byte[] bytes = new byte[strLen];
        data.get(bytes);
        value = new String(bytes, StandardCharsets.UTF_8);
    }

    public static StringLiteral create(String value) {
        if (value.length() > LargeStringLiteral.LEN_LIMIT) {
            return new LargeStringLiteral(value, NodePosition.ZERO);
        } else {
            return new StringLiteral(value);
        }
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitStringLiteral(this, context);
    }
}
