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

import com.google.common.base.Preconditions;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;

import java.util.Objects;

/**
 * Most predicates with two operands..
 */
public class BinaryPredicate extends Predicate {

    public static final com.google.common.base.Predicate<BinaryPredicate> IS_RANGE_PREDICATE =
            arg -> arg.getOp() == BinaryType.LT
                    || arg.getOp() == BinaryType.LE
                    || arg.getOp() == BinaryType.GT
                    || arg.getOp() == BinaryType.GE;

    public static final com.google.common.base.Predicate<BinaryPredicate> IS_EQ_PREDICATE =
            arg -> arg.getOp() == BinaryType.EQ;

    public static final com.google.common.base.Predicate<BinaryPredicate> IS_EQ_NULL_PREDICATE =
            arg -> arg.getOp() == BinaryType.EQ_FOR_NULL;

    private BinaryType op;

    // for restoring
    public BinaryPredicate() {
        super();
    }

    public BinaryPredicate(BinaryType op, Expr e1, Expr e2) {
        this(op, e1, e2, NodePosition.ZERO);
    }

    public BinaryPredicate(BinaryType op, Expr e1, Expr e2, NodePosition pos) {
        super(pos);
        this.op = op;
        Preconditions.checkNotNull(e1);
        children.add(e1);
        Preconditions.checkNotNull(e2);
        children.add(e2);
    }

    protected BinaryPredicate(BinaryPredicate other) {
        super(other);
        op = other.op;
    }

    @Override
    public Expr clone() {
        return new BinaryPredicate(this);
    }

    public BinaryType getOp() {
        return op;
    }

    @Override
    public boolean equalsWithoutChild(Object obj) {
        if (!super.equalsWithoutChild(obj)) {
            return false;
        }
        return ((BinaryPredicate) obj).op == this.op;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hashCode(op);
    }

    public boolean isNullable() {
        return !BinaryType.EQ_FOR_NULL.equals(op) && hasNullableChild();
    }

    /**
     * Below function is added by new analyzer
     */
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitBinaryPredicate(this, context);
    }

    @Override
    public boolean isSelfMonotonic() {
        return op.isMonotonic();
    }
}
