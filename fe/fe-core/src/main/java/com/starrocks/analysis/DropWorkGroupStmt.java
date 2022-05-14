// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.analysis;

import com.starrocks.sql.ast.AstVisitor;

public class DropWorkGroupStmt extends DdlStmt {
    private final String name;

    public DropWorkGroupStmt(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropWorkGroupStatement(this, context);
    }
}
