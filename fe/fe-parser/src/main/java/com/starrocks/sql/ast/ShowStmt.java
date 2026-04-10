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

package com.starrocks.sql.ast;

import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.LimitElement;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public abstract class ShowStmt extends StatementBase {
    protected Expr predicate;
    protected LimitElement limitElement;
    protected List<OrderByElement> orderByElements;
    protected List<OrderByPair> orderByPairs;

    // version flag to indicate which part is implemented by self
    // 1st bit: predicate
    // 2nd bit: order by
    // 3rd bit: limit
    private short predicateOrderLimitVersion = 0;

    protected ShowStmt(NodePosition pos) {
        super(pos);
    }

    public void setPredicate(Expr predicate) {
        this.predicate = predicate;
    }

    public Expr getPredicate() {
        return predicate;
    }

    public void setOrderByElements(List<OrderByElement> orderByElements) {
        this.orderByElements = orderByElements;
    }

    public List<OrderByElement> getOrderByElements() {
        return orderByElements;
    }

    public List<OrderByPair> getOrderByPairs() {
        return orderByPairs;
    }

    public void setOrderByPairs(List<OrderByPair> orderByPairs) {
        this.orderByPairs = orderByPairs;
    }

    public void setLimitElement(LimitElement limitElement) {
        this.limitElement = limitElement;
    }

    public LimitElement getLimitElement() {
        return limitElement;
    }

    // some show stmt was implemented `where`/`order by`/`limit` by self, so need clean predicate to avoid conflict,
    // and we can find these stmt by this function
    // Todo: unify the implementation show stmt
    public void markSelfPredicateOrderLimit(boolean isSelfPredicate, boolean isSelfOrderBy, boolean isSelfLimit) {
        if (isSelfPredicate) {
            predicateOrderLimitVersion |= 1;
        }
        if (isSelfOrderBy) {
            predicateOrderLimitVersion |= 2;
        }
        if (isSelfLimit) {
            predicateOrderLimitVersion |= 4;
        }
    }

    public void markSelfPredicate() {
        markSelfPredicateOrderLimit(true, false, false);
    }

    public boolean isSelfPredicate() {
        return (predicateOrderLimitVersion & 1) != 0;
    }

    public boolean isSelfOrderBy() {
        return (predicateOrderLimitVersion & 2) != 0;
    }

    public boolean isSelfLimit() {
        return (predicateOrderLimitVersion & 4) != 0;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowStatement(this, context);
    }
}
