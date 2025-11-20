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

public class CompoundPredicate extends Predicate {
    private final Operator op;

    public CompoundPredicate(Operator op, Expr e1, Expr e2) {
        this(op, e1, e2, NodePosition.ZERO);
    }

    public CompoundPredicate(Operator op, Expr e1, Expr e2, NodePosition pos) {
        super(pos);
        this.op = op;
        Preconditions.checkNotNull(e1);
        children.add(e1);
        Preconditions.checkArgument(
                op == Operator.NOT && e2 == null || op != Operator.NOT && e2 != null);
        if (e2 != null) {
            children.add(e2);
        }
        incrDepth();
    }

    protected CompoundPredicate(CompoundPredicate other) {
        super(other);
        op = other.op;
    }

    @Override
    public Expr clone() {
        return new CompoundPredicate(this);
    }

    public Operator getOp() {
        return op;
    }

    @Override
    public boolean equalsWithoutChild(Object obj) {
        return super.equalsWithoutChild(obj) && ((CompoundPredicate) obj).op == op;
    }


    public enum Operator {
        AND("AND"),
        OR("OR"),
        NOT("NOT");

        private final String description;

        Operator(String description) {
            this.description = description;
        }

        @Override
        public String toString() {
            return description;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), op);
    }

    /**
     * Below function ia added by new analyzer
     */
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)  {
        return visitor.visitCompoundPredicate(this, context);
    }
}
