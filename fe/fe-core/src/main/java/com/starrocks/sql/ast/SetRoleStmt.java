// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.RedirectStatus;

import java.util.List;

// set role all -> roles = null, all = true
// set role all except role1, role2 -> roles = [role1, role2], all = true
// set role role1, role2 -> roles = [role1, role2], all = false;
public class SetRoleStmt extends StatementBase {
    private List<String> roles;
    private boolean all;

    public SetRoleStmt(List<String> roles, boolean all) {
        this.roles = roles;
        this.all = all;
    }

    public List<String> getRoles() {
        return roles;
    }

    public boolean isAll() {
        return all;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitSetRoleStatement(this, context);
    }
}