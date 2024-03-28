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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/LiteralExpr.java

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
import com.starrocks.mysql.MysqlProto;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.parser.NodePosition;
import org.apache.groovy.util.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

public abstract class LiteralExpr extends Expr implements Comparable<LiteralExpr> {
    public LiteralExpr() {
        super();
        numDistinctValues = 1;
    }

    protected LiteralExpr(NodePosition pos) {
        super(pos);
    }

    protected LiteralExpr(LiteralExpr other) {
        super(other);
    }

    public static LiteralExpr create(String value, Type type) throws AnalysisException {
        Preconditions.checkArgument(type.isValid());
        LiteralExpr literalExpr = null;
        switch (type.getPrimitiveType()) {
            case NULL_TYPE:
                literalExpr = new NullLiteral();
                break;
            case BOOLEAN:
                literalExpr = new BoolLiteral(value);
                break;
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                literalExpr = new IntLiteral(value, type);
                break;
            case LARGEINT:
                literalExpr = new LargeIntLiteral(value);
                break;
            case FLOAT:
            case DOUBLE:
                literalExpr = new FloatLiteral(value);
                break;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                literalExpr = new DecimalLiteral(value);
                break;
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
            case HLL:
                literalExpr = new StringLiteral(value);
                break;
            case DATE:
            case DATETIME:
                literalExpr = new DateLiteral(value, type);
                break;
            default:
                throw new AnalysisException("Type[" + type.toSql() + "] not supported.");
        }

        Preconditions.checkNotNull(literalExpr);
        return literalExpr;
    }

    public static LiteralExpr createInfinity(Type type, boolean isMax) throws AnalysisException {
        Preconditions.checkArgument(type.isValid());
        if (isMax) {
            return MaxLiteral.MAX_VALUE;
        }
        switch (type.getPrimitiveType()) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                return IntLiteral.createMinValue(type);
            case LARGEINT:
                return LargeIntLiteral.createMinValue();
            case DATE:
            case DATETIME:
                return DateLiteral.createMinValue(type);
            default:
                throw new AnalysisException("Invalid data type for creating infinity: " + type);
        }
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        // Literals require no analysis.
    }

    /*
     * return real object value
     */
    public Object getRealObjectValue() {
        throw new StarRocksPlannerException("Not implement getRealObjectValue in derived class. ", ErrorType.INTERNAL_ERROR);
    }

    public abstract boolean isMinValue();

    // Only used by partition pruning and the derived class which can be used for pruning
    // must handle MaxLiteral.
    public abstract int compareLiteral(LiteralExpr expr);

    // Returns the string representation of the literal's value. Used when passing
    // literal values to the metastore rather than to backends. This is similar to
    // the toSql() method, but does not perform any formatting of the string values. Neither
    // method unescapes string values.
    public String getStringValue() {
        return null;
    }

    public long getLongValue() {
        return 0;
    }

    public double getDoubleValue() {
        return 0;
    }

    public ByteBuffer getHashValue(Type type) {
        String value = getStringValue();
        ByteBuffer buffer;
        try {
            buffer = ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return buffer;
    }

    // Swaps the sign of numeric literals.
    // Throws for non-numeric literals.
    public void swapSign() throws NotImplementedException {
        throw new NotImplementedException("swapSign() only implemented for numeric" + "literals");
    }

    @Override
    public boolean supportSerializable() {
        return true;
    }

    @Override
    public void write(DataOutput out) throws IOException {
    }

    public void readFields(DataInput in) throws IOException {
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof LiteralExpr)) {
            return false;
        }

        if ((obj instanceof StringLiteral && !(this instanceof StringLiteral))
                || (this instanceof StringLiteral && !(obj instanceof StringLiteral))
                || (obj instanceof DecimalLiteral && !(this instanceof DecimalLiteral))
                || (this instanceof DecimalLiteral && !(obj instanceof DecimalLiteral))
                || (obj instanceof BoolLiteral && !(this instanceof BoolLiteral))
                || (this instanceof BoolLiteral && !(obj instanceof BoolLiteral))
                || (obj instanceof FloatLiteral && !(this instanceof FloatLiteral))
                || (this instanceof FloatLiteral && !(obj instanceof FloatLiteral))) {
            return false;
        }
        return this.compareLiteral(((LiteralExpr) obj)) == 0 && Objects.equals(type, ((LiteralExpr) obj).type);
    }

    @Override
    public boolean isNullable() {
        return this instanceof NullLiteral;
    }

    public boolean isConstantNull() {
        return this instanceof NullLiteral;
    }

    @Override
    public int compareTo(LiteralExpr o) {
        return compareLiteral(o);
    }

    /**
     * Below function is added by new analyzer
     */
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitLiteral(this, context);
    }

    @Override
    public boolean isSelfMonotonic() {
        return true;
    }

    // Parse from binary data, the format follows mysql binary protocal
    // see https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html.
    public void parseMysqlParam(ByteBuffer data) {
        throw new StarRocksPlannerException("Not implement parseMysqlParam in derived class. ", ErrorType.INTERNAL_ERROR);
    }

    public static LiteralExpr parseLiteral(int encode) throws AnalysisException {
        LiteralExpr literalExpr = MYSQL_LITERAL_TYPE_ENCODE_MAP.get(encode);
        if (null != literalExpr) {
            return (LiteralExpr) literalExpr.clone();
        } else {
            throw new AnalysisException("unknown mysql type code " + encode);
        }
    }

    private static final LiteralExpr tinyIntExpr;
    private static final LiteralExpr smallIntExpr;
    private static final LiteralExpr intExpr;
    private static final LiteralExpr bigIntExpr;
    private static final LiteralExpr floatExpr;
    private static final LiteralExpr doubleExpr;
    private static final LiteralExpr decimal32Expr;
    private static final LiteralExpr dateExpr;
    private static final LiteralExpr dateTimeExpr;
    private static final LiteralExpr stringExpr;
    private static final LiteralExpr varcharExpr;

    static {
        try {
            tinyIntExpr = LiteralExpr.create("0", Type.TINYINT);
            smallIntExpr = LiteralExpr.create("0", Type.SMALLINT);
            intExpr = LiteralExpr.create("0", Type.INT);
            bigIntExpr = LiteralExpr.create("0", Type.BIGINT);
            floatExpr = LiteralExpr.create("0", Type.FLOAT);
            doubleExpr = LiteralExpr.create("0", Type.DOUBLE);
            decimal32Expr = LiteralExpr.create("0", Type.DECIMAL32);
            dateExpr = LiteralExpr.create("1970-01-01", Type.DATE);
            dateTimeExpr = LiteralExpr.create("1970-01-01 00:00:00", Type.DATETIME);
            stringExpr = LiteralExpr.create("", Type.STRING);
            varcharExpr = LiteralExpr.create("", Type.VARCHAR);
        } catch (AnalysisException e) {
            throw new RuntimeException(e);
        }
    }

    private static final Map<Integer, LiteralExpr> MYSQL_LITERAL_TYPE_ENCODE_MAP = Maps.of(
            0, decimal32Expr,       // MYSQL_TYPE_DECIMAL
            1, tinyIntExpr,             // MYSQL_TYPE_TINY
            2, smallIntExpr,            // MYSQL_TYPE_SHORT
            3, intExpr,                 // MYSQL_TYPE_LONG
            4, floatExpr,               // MYSQL_TYPE_FLOAT
            5, doubleExpr,              // MYSQL_TYPE_DOUBLE
            7, dateTimeExpr,            // MYSQL_TYPE_TIMESTAMP2
            8, bigIntExpr,              // MYSQL_TYPE_LONGLONG
            10, dateExpr,               // MYSQL_TYPE_DATE
            12, dateTimeExpr,           // MYSQL_TYPE_DATETIME
            15, varcharExpr,            // MYSQL_TYPE_VARCHAR
            17, dateTimeExpr,           // MYSQL_TYPE_TIMESTAMP2
            253, stringExpr,            // MYSQL_TYPE_STRING
            254, stringExpr             // MYSQL_TYPE_STRING
    );

    // Port from mysql get_param_length
    public static int getParamLen(ByteBuffer data) {
        int maxLen = data.remaining();
        if (maxLen < 1) {
            return 0;
        }
        // get and advance 1 byte
        int len = MysqlProto.readInt1(data);
        if (len == 252) {
            if (maxLen < 3) {
                return 0;
            }
            // get and advance 2 bytes
            return MysqlProto.readInt2(data);
        } else if (len == 253) {
            if (maxLen < 4) {
                return 0;
            }
            // get and advance 3 bytes
            return MysqlProto.readInt3(data);
        } else if (len == 254) {
            /*
            In our client-server protocol all numbers bigger than 2^24
            stored as 8 bytes with uint8korr. Here we always know that
            parameter length is less than 2^4 so we don't look at the second
            4 bytes. But still we need to obey the protocol hence 9 in the
            assignment below.
            */
            if (maxLen < 9) {
                return 0;
            }
            len = MysqlProto.readInt4(data);
            MysqlProto.readFixedString(data, 4);
            return len;
        } else if (len == 255) {
            return 0;
        } else {
            return len;
        }
    }
}
