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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

/**
 * Represents an IN subquery with multiple columns.
 */
public class MultiInPredicate extends Predicate {
    private final boolean isNotIn;
    // Expressions involved on the left side of the IN predicate (outside the subquery).
    private List<Expr> outerExprs;
    private Expr subquery;

    // Number of columns involved in the IN subquery.
    private int numberOfColumns;

    public MultiInPredicate(List<Expr> outerExprs, Expr subquery, boolean isNotIn) {
        this(outerExprs, subquery, isNotIn, NodePosition.ZERO);
    }

    public MultiInPredicate(List<Expr> outerExprs, Expr subquery, boolean isNotIn, NodePosition pos) {
        super(pos);
        Preconditions.checkNotNull(subquery);
        children.addAll(outerExprs);
        children.add(subquery);
        this.outerExprs = outerExprs;
        this.subquery = subquery;
        this.isNotIn = isNotIn;
        this.numberOfColumns = outerExprs.size();
    }

    protected MultiInPredicate(MultiInPredicate other) {
        super(other);
        isNotIn = other.isNotIn();
        numberOfColumns = other.numberOfColumns;
    }

    @Override
    public Expr negate() {
        return new MultiInPredicate(this.outerExprs, this.subquery,
                !isNotIn);
    }

    public boolean isNotIn() {
        return isNotIn;
    }

    public int getNumberOfColumns() {return numberOfColumns;}

    @Override
    public String toSqlImpl() {
        StringBuilder strBuilder = new StringBuilder();
        String notStr = (isNotIn) ? "NOT " : "";
        strBuilder.append("(");
        for (int i = 0; i < numberOfColumns; ++i) {
            strBuilder.append(getChild(i).toSql());
            if (i < numberOfColumns - 1) {
                strBuilder.append(", ");
            }
        }
        strBuilder.append(") ").append(notStr).append("IN (").append(getChild(numberOfColumns).toSql());
        strBuilder.append(")");
        return strBuilder.toString();
    }

    public boolean equalsWithoutChild(Object obj) {
        if (super.equalsWithoutChild(obj)) {
            MultiInPredicate expr = (MultiInPredicate) obj;
            return isNotIn == expr.isNotIn && numberOfColumns == expr.numberOfColumns;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), isNotIn);
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public Expr clone() {
        return new MultiInPredicate(this);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) throws SemanticException {
        return visitor.visitMultiInPredicate(this, context);
    }
}
