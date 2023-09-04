// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.sql.analyzer.FieldId;

import java.util.List;
import java.util.Map;

public class SubqueryRelation extends QueryRelation {
    private final QueryStatement queryStatement;

    public SubqueryRelation(QueryStatement queryStatement) {
        this.queryStatement = queryStatement;
        QueryRelation queryRelation = this.queryStatement.getQueryRelation();
        // The order by is meaningless in subquery
        if (!queryRelation.hasLimit()) {
            queryRelation.clearOrder();
        }
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

    @Override
    public List<Expr> getOutputExpression() {
        return this.queryStatement.getQueryRelation().getOutputExpression();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSubquery(this, context);
    }
}
