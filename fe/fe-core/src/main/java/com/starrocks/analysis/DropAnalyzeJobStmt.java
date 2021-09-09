// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.analysis;

public class DropAnalyzeJobStmt extends DdlStmt {
    private final long id;

    public DropAnalyzeJobStmt(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }
}
