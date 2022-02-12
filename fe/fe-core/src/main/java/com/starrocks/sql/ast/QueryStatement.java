// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.ast;

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.StatementBase;

public class QueryStatement extends StatementBase {
    private final QueryRelation queryRelation;

    public QueryStatement(QueryRelation queryRelation) {
        this.queryRelation = queryRelation;
    }

    public QueryRelation getQueryRelation() {
        return queryRelation;
    }

    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitQueryStatement(this, context);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return null;
    }
}