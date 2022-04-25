// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

// REVOKE Role 'role' FROM 'user'
// share the same parameter and check logic with GrantRoleStmt
public class RevokeRoleStmt extends GrantRoleStmt {

    public RevokeRoleStmt(String role, UserIdentity userIdent) {
        super(role, userIdent);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("REVOKE ROLE '").append(qualifiedRole).append("' FROM ").append(userIdent);
        return sb.toString();
    }
}
