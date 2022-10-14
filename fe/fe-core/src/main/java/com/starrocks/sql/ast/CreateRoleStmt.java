// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

public class CreateRoleStmt extends DdlStmt {

    private String role;

    public CreateRoleStmt(String role) {
        this.role = role;
    }

    public String getQualifiedRole() {
        return role;
    }

    public void setQualifiedRole(String role) {
        this.role = role;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateRoleStatement(this, context);
    }
}
