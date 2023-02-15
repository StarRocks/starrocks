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

    @Override
    public String toSql() {
        return "drop analyze " + id;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropAnalyzeStatement(this, context);
    }
}
