// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.DdlStmt;

public class DropAnalyzeJobStmt extends DdlStmt {
    private final long id;

    public DropAnalyzeJobStmt(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
