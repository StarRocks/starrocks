// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.starrocks.sql.ast.AstVisitor;

import java.util.List;

// use for delete sql's blacklist by ids.
// indexs is the ids of regular expression's sql
public class DelSqlBlackListStmt extends StatementBase {

    private List<Long> indexs;

    public List<Long> getIndexs() {
        return indexs;
    }

    public DelSqlBlackListStmt(List<Long> indexs) {
        this.indexs = indexs;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDelSqlBlackListStatement(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }
}

