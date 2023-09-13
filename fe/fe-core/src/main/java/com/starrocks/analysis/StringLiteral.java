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

package com.starrocks.analysis;

import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.io.Text;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TStringLiteral;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class StringLiteral extends LiteralExpr {
    private String value;

    public StringLiteral() {
        super(NodePosition.ZERO);
        type = Type.VARCHAR;
    }

    public StringLiteral(String value) {
        this(value, NodePosition.ZERO);
    }

    public StringLiteral(String value, NodePosition pos) {
        super(pos);
        this.value = value;
        type = Type.VARCHAR;
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

    @Override
    public String toSqlImpl() {
        String sql = value;
        if (value != null) {
            if (value.contains("\\")) {
                sql = value.replace("\\", "\\\\");
            }
            sql = sql.replace("'", "\\'");
        }
        return "'" + sql + "'";
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.STRING_LITERAL;
        msg.string_literal = new TStringLiteral(getUnescapedValue());
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

    /**
     * Convert a string literal to a date literal
     *
     * @param targetType is the desired type
     * @return new converted literal (not null)
     * @throws AnalysisException when entire given string cannot be transformed into a date
     */
    private LiteralExpr convertToDate(Type targetType) throws AnalysisException {
        LiteralExpr newLiteral;
        try {
            newLiteral = new DateLiteral(value, targetType);
        } catch (AnalysisException e) {
            if (targetType.isDatetime()) {
                newLiteral = new DateLiteral(value, Type.DATE);
                newLiteral.setType(Type.DATETIME);
            } else {
                throw e;
            }
        }
        return newLiteral;
    }

    @Override
    public Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        if (targetType.isNumericType()) {
            switch (targetType.getPrimitiveType()) {
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                    return new IntLiteral(value, targetType);
                case LARGEINT:
                    return new LargeIntLiteral(value);
                case FLOAT:
                case DOUBLE:
                    try {
                        return new FloatLiteral(Double.valueOf(value), targetType);
                    } catch (NumberFormatException e) {
                        ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_NUMBER, value);
                    }
                    break;
                case DECIMALV2:
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                    return new DecimalLiteral(value).uncheckedCastTo(targetType);
                default:
                    break;
            }
        } else if (targetType.isDateType()) {
            // FE only support 'yyyy-MM-dd hh:mm:ss' && 'yyyy-MM-dd' format
            // so if FE unchecked cast fail, we also build CastExpr for BE
            // BE support other format suck as 'yyyyMMdd'...
            try {
                return convertToDate(targetType);
            } catch (AnalysisException e) {
                // pass;
            }
        } else if (targetType.getPrimitiveType() == type.getPrimitiveType()) {
            return this;
        } else if (targetType.isStringType()) {
            StringLiteral stringLiteral = new StringLiteral(this);
            stringLiteral.setType(targetType);
            return stringLiteral;
        }
        return super.uncheckedCastTo(targetType);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, value);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        value = Text.readString(in);
    }

    public static StringLiteral read(DataInput in) throws IOException {
        StringLiteral literal = new StringLiteral();
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

    @Override
    public void parseMysqlParam(ByteBuffer data) {
        int strLen = getParamLen(data);
        if (strLen > data.remaining()) {
            strLen = data.remaining();
        }
        byte[] bytes = new byte[strLen];
        data.get(bytes);
        value = new String(bytes);
    }
}
