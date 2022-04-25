// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.sql.ast.AstVisitor;

// GRANT Role 'role' TO 'user'
public class GrantRoleStmt extends DdlStmt {
    protected String role;
    protected UserIdentity userIdent;
    protected String qualifiedRole;

    public GrantRoleStmt(String role, UserIdentity userIdent) {
        this.role = role;
        this.userIdent = userIdent;
    }

    public UserIdentity getUserIdent() {
        return userIdent;
    }

    public String getQualifiedRole() {
        return qualifiedRole;
    }

    public String getRole() {
        return role;
    }

    public void setQualifiedRole(String qualifiedRole) {
        this.qualifiedRole = qualifiedRole;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("GRANT ROLE '").append(qualifiedRole).append("' TO ").append(userIdent);
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGrantRoleStatement(this, context);
    }
}
