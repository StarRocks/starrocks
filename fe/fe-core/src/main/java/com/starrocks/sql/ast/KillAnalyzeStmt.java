// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.RedirectStatus;

public class KillAnalyzeStmt extends StatementBase {
    private final long analyzeId;

    public KillAnalyzeStmt(long analyzeId) {
        this.analyzeId = analyzeId;
    }

    public long getAnalyzeId() {
        return analyzeId;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitKillAnalyzeStatement(this, context);
    }
}
