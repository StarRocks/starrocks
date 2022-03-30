// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.ast;

import com.starrocks.analysis.TableName;

public class SubqueryRelation extends Relation {
    private final String name;
    private final QueryStatement queryStatement;

    public SubqueryRelation(String name, QueryStatement queryStatement) {
        this.name = name;
        if (name != null) {
            this.alias = new TableName(null, name);
        } else {
            this.alias = null;
        }
        this.queryStatement = queryStatement;
        // The order by is meaningless in subquery
        QueryRelation queryRelation = this.queryStatement.getQueryRelation();
        if (!queryRelation.hasLimit()) {
            queryRelation.clearOrder();
        }
    }

    public String getName() {
        return name;
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
