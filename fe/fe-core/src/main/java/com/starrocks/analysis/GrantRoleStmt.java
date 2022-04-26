// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

// GRANT Role 'role' TO 'user'
// share the same parameter and check logic with RevokeRoleStmt
public class GrantRoleStmt extends BaseGrantRevokeRoleStmt {

    public GrantRoleStmt(String role, UserIdentity userIdent) {
        super(role, userIdent);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("GRANT '").append(qualifiedRole).append("' TO ").append(userIdent);
        return sb.toString();
    }
}
