// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.UserIdentity;

// REVOKE Role 'role' FROM 'user'
// share the same parameter and check logic with GrantRoleStmt
public class RevokeRoleStmt extends BaseGrantRevokeRoleStmt {

    public RevokeRoleStmt(String role, UserIdentity userIdent) {
        super(role, userIdent, "REVOKE", "FROM");
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
