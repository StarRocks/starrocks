// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.OutFileClause;
import com.starrocks.analysis.RedirectStatus;

public class QueryStatement extends StatementBase {
    private final QueryRelation queryRelation;

    // represent the "INTO OUTFILE" clause
    protected OutFileClause outFileClause;

    public QueryStatement(QueryRelation queryRelation) {
        this.queryRelation = queryRelation;
    }

    public QueryRelation getQueryRelation() {
        return queryRelation;
    }

    public void setOutFileClause(OutFileClause outFileClause) {
        this.outFileClause = outFileClause;
    }

    public OutFileClause getOutFileClause() {
        return outFileClause;
    }

    public boolean hasOutFileClause() {
        return outFileClause != null;
    }

    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitQueryStatement(this, context);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }
}