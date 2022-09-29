// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/DateLiteral.java

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
import com.starrocks.thrift.TDateLiteral;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.TimeZone;

public class Ipv4Literal extends LiteralExpr {

    private static final Ipv4Literal MIN_IP = new Ipv4Literal(0);
    private static final Ipv4Literal MAX_IP = new Ipv4Literal(Integer.MAX_VALUE);

    private Ipv4Literal() {
        super();
    }

    public Ipv4Literal(Type type, boolean isMax) throws AnalysisException {
        super();
        this.type = type;
        if (isMax) {
            copy(MAX_IP);
        } else {
            copy(MIN_IP);
        }

        analysisDone();
    }

    public Ipv4Literal(String s) throws AnalysisException {
        super();
        init(s);
        analysisDone();
    }

    public Ipv4Literal(int ip) {
        this.ip = ip;
    }

    public Ipv4Literal(Ipv4Literal other) {
        super(other);
        ip = other.ip;
    }

    public static Ipv4Literal createMinValue(Type type) throws AnalysisException {
        return new Ipv4Literal(type, false);
    }

    private void init(String s) throws AnalysisException {
        try {
            // TODO: 2022/9/29 string to ip
            this.ip = 123;
        } catch (Exception ex) {
            throw new AnalysisException("ipv4 literal [" + s + "] is invalid");
        }
    }

    private void copy(Ipv4Literal other) {
        ip = other.ip;
    }

    @Override
    public Expr clone() {
        return new Ipv4Literal(this);
    }

    @Override
    public boolean isMinValue() {
        return this.getStringValue().compareTo(MIN_IP.getStringValue()) == 0;

    }

    @Override
    public Object getRealValue() {
        return "234.234.234.234";
    }

    // Date column and Datetime column's hash value is not same.
    @Override
    public ByteBuffer getHashValue(Type type) {
        String value = convertToString(type.getPrimitiveType());
        ByteBuffer buffer;
        try {
            buffer = ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
        // date time will not overflow when doing addition and subtraction
        return Long.signum(getLongValue() - expr.getLongValue());
    }

    @Override
    public String toSqlImpl() {
        return "'" + getStringValue() + "'";
    }

    @Override
    public String getStringValue() {
        return convertToString(type.getPrimitiveType());
    }

    private String convertToString(PrimitiveType type) {
        return "123.123.123.123";
    }

    @Override
    public long getLongValue() {
        return 1234567l;
    }

    @Override
    public double getDoubleValue() {
        return getLongValue();
    }

    // TODO: 2022/9/29 ip type
    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.DATE_LITERAL;
        msg.date_literal = new TDateLiteral(getStringValue());
    }

    @Override
    public Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        return this;
    }

    private long makePackedDatetime() {
        return 456l;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        //set flag bit in meta, 0 is DATETIME and 1 is DATE
        out.writeShort(0);
        out.writeLong(makePackedDatetime());
    }

    private void fromPackedDatetime(long packed_time) {
        ip = 90;
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        ip = 89;
    }

    public static Ipv4Literal read(DataInput in) throws IOException {
        Ipv4Literal literal = new Ipv4Literal();
        literal.readFields(in);
        return literal;
    }

    public long unixTimestamp(TimeZone timeZone) {
        return 56;
    }

    public long getIp() {
        return ip;
    }

    private int ip;

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hashCode(unixTimestamp(TimeZone.getDefault()));
    }

    @Override
    public boolean isNullable() {
        return this.compareLiteral(Ipv4Literal.MIN_IP) < 0 || Ipv4Literal.MAX_IP.compareLiteral(this) < 0;
    }
}
