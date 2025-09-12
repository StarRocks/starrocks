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

import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.OrderByPair;
import com.starrocks.sql.ast.expression.LimitElement;
import com.starrocks.sql.ast.expression.Predicate;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public abstract class ShowStmt extends StatementBase {
    protected Predicate predicate;
    protected LimitElement limitElement;
    protected List<OrderByElement> orderByElements;
    protected List<OrderByPair> orderByPairs;

    protected ShowStmt(NodePosition pos) {
        super(pos);
    }

    public void setPredicate(Predicate predicate) {
        this.predicate = predicate;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public QueryStatement toSelectStmt() throws AnalysisException {
        return null;
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

    public LimitElement getLimitElement() {
        return limitElement;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitShowStatement(this, context);
    }
}
