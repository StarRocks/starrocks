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


package com.starrocks.analysis;

import com.google.common.base.CharMatcher;
import com.google.common.io.BaseEncoding;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.io.Text;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.thrift.TBinaryLiteral;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

import javax.validation.constraints.NotNull;

import static com.starrocks.sql.common.ErrorMsgProxy.PARSER_ERROR_MSG;
import static java.util.Locale.ENGLISH;

public class VarBinaryLiteral extends LiteralExpr {
    private static final CharMatcher WHITESPACE_MATCHER = CharMatcher.whitespace();
    private static final CharMatcher HEX_DIGIT_MATCHER = CharMatcher.inRange('A', 'F')
            .or(CharMatcher.inRange('0', '9'))
            .precomputed();

    private byte[] value;

    public VarBinaryLiteral() {
        super();
        this.type = Type.VARBINARY;
    }

    public VarBinaryLiteral(byte[] value) {
        super();
        this.value = value;
        this.type = Type.VARBINARY;
        analysisDone();
    }

    public VarBinaryLiteral(@NotNull String value) {
        this(value, NodePosition.ZERO);
    }


    public VarBinaryLiteral(@NotNull String value, NodePosition pos) {
        super(pos);
        String hexString = WHITESPACE_MATCHER.removeFrom(value).toUpperCase(ENGLISH);
        if (!HEX_DIGIT_MATCHER.matchesAllOf(hexString)) {
            throw new ParsingException(PARSER_ERROR_MSG.invalidBinaryFormat(), pos);
        }
        if (hexString.length() % 2 != 0) {
            throw new ParsingException(PARSER_ERROR_MSG.invalidBinaryFormat(), pos);
        }
        this.type = Type.VARBINARY;
        this.value = BaseEncoding.base16().decode(hexString);
    }

    protected VarBinaryLiteral(VarBinaryLiteral other) {
        super(other);
        this.value = other.value;
        this.type = other.type;
    }

    @Override
    public Expr clone() {
        return new VarBinaryLiteral(this);
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        if (expr instanceof NullLiteral) {
            return 1;
        }
        // compare string with utf-8 byte array, same with DM,BE,StorageEngine
        byte[] thisBytes = value;
        byte[] otherBytes = null;
        thisBytes = getStringValue().getBytes(StandardCharsets.UTF_8);
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

    @Override
    public boolean isMinValue() {
        return false;
    }

    @Override
    public String toSqlImpl() {
        return "'" + getStringValue() + "'";
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.BINARY_LITERAL;
        msg.binary_literal = new TBinaryLiteral(ByteBuffer.wrap(value));
    }

    @Override
    public String getStringValue() {
        return BaseEncoding.base16().encode(value);
    }

    @Override
    public long getLongValue() {
        return 0;
    }

    @Override
    public double getDoubleValue() {
        return 0.0;
    }

    @Override
    public Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        return super.uncheckedCastTo(targetType);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeBinary(out, value);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        value = Text.readBinary(in);
    }

    public static VarBinaryLiteral read(DataInput in) throws IOException {
        VarBinaryLiteral literal = new VarBinaryLiteral();
        literal.readFields(in);
        return literal;
    }
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof VarBinaryLiteral)) {
            return false;
        }
        return Arrays.equals(value, ((VarBinaryLiteral)obj).value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), value);
    }
}
