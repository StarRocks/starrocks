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

import com.starrocks.sql.analyzer.FieldId;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Map;

public class SubqueryRelation extends QueryRelation {
    private final QueryStatement queryStatement;

    private final boolean assertRows;

    public SubqueryRelation(QueryStatement queryStatement) {
        this(queryStatement, false, queryStatement.getPos());
    }

    public SubqueryRelation(QueryStatement queryStatement, boolean assertRows, NodePosition pos) {
        super(pos);
        this.queryStatement = queryStatement;
        this.assertRows = assertRows;
        QueryRelation queryRelation = this.queryStatement.getQueryRelation();
        // The order by is meaningless in subquery
        if (!queryRelation.hasLimit()) {
            queryRelation.clearOrder();
        }
    }

    public boolean isAssertRows() {
        return assertRows;
    }

    @Override
    public Map<Expr, FieldId> getColumnReferences() {
        return queryStatement.getQueryRelation().getColumnReferences();
    }

    public QueryStatement getQueryStatement() {
        return queryStatement;
    }

    @Override
    public String toString() {
        return alias == null ? "anonymous" : alias.toString();
    }

    public boolean isAnonymous() {
        return alias == null;
    }

    @Override
    public List<Expr> getOutputExpression() {
        return this.queryStatement.getQueryRelation().getOutputExpression();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitSubqueryRelation(this, context);
    }
}
