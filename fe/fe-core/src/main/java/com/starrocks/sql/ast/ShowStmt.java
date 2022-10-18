// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.Predicate;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ShowResultSetMetaData;

public abstract class ShowStmt extends StatementBase {
    protected Predicate predicate;

    public void setPredicate(Predicate predicate) {
        this.predicate = predicate;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public abstract ShowResultSetMetaData getMetaData();

    public QueryStatement toSelectStmt() throws AnalysisException {
        return null;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowStatement(this, context);
    }
}
