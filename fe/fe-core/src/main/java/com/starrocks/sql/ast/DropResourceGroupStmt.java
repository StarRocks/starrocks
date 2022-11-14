// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.ast;

// Drop ResourceGroup specified by name
// DROP RESOURCE GROUP <name>
public class DropResourceGroupStmt extends DdlStmt {
    private final String name;

    public DropResourceGroupStmt(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropResourceGroupStatement(this, context);
    }
}
