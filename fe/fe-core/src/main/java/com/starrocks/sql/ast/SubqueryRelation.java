// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.ast;

public class SubqueryRelation extends Relation {
    private final QueryStatement queryStatement;

    public SubqueryRelation(QueryStatement queryStatement) {
        this.queryStatement = queryStatement;
        // The order by is meaningless in subquery
        QueryRelation queryRelation = this.queryStatement.getQueryRelation();
        if (!queryRelation.hasLimit()) {
            queryRelation.clearOrder();
        }
    }

    public QueryStatement getQueryStatement() {
        return queryStatement;
    }

    @Override
    public String toString() {
        return alias == null ? "anonymous" : alias.toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSubquery(this, context);
    }
}
