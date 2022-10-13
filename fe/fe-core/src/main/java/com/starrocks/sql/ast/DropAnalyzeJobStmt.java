// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

public class DropAnalyzeJobStmt extends DdlStmt {
    private final long id;

    public DropAnalyzeJobStmt(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }
}
