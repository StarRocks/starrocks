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
import com.starrocks.type.Type;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public abstract class LiteralExpr extends Expr implements Comparable<LiteralExpr> {
    public LiteralExpr() {
        super();
    }

    protected LiteralExpr(NodePosition pos) {
        super(pos);
    }

    protected LiteralExpr(LiteralExpr other) {
        super(other);
    }

    /*
     * return real object value
     */
    public Object getRealObjectValue() {
        throw new ParsingException("Not implement getRealObjectValue in derived class. ");
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
    public void swapSign() {
        throw new UnsupportedOperationException("swapSign() only implemented for numeric" + "literals");
    }

    @Override
    public boolean supportSerializable() {
        return true;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equalsWithoutChild(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof LiteralExpr)) {
            return false;
        }

        LiteralExpr other = (LiteralExpr) obj;
        if (!belongsToSameLiteralFamily(other)) {
            return false;
        }
        return compareLiteral(other) == 0;
    }

    private boolean belongsToSameLiteralFamily(LiteralExpr other) {
        Type thisType = getType();
        Type otherType = other.getType();
        if (isStringLiteralType(thisType) != isStringLiteralType(otherType)) {
            return false;
        }
        if (isDecimalLiteralType(thisType) != isDecimalLiteralType(otherType)) {
            return false;
        }
        if (isBooleanLiteralType(thisType) != isBooleanLiteralType(otherType)) {
            return false;
        }
        if (isFloatingPointLiteralType(thisType) != isFloatingPointLiteralType(otherType)) {
            return false;
        }
        return true;
    }

    private static boolean isStringLiteralType(Type type) {
        return type != null && type.isStringType();
    }

    private static boolean isDecimalLiteralType(Type type) {
        return type != null && type.isDecimalOfAnyVersion();
    }

    private static boolean isBooleanLiteralType(Type type) {
        return type != null && type.isBoolean();
    }

    private static boolean isFloatingPointLiteralType(Type type) {
        return type != null && type.isFloatingPointType();
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

}
