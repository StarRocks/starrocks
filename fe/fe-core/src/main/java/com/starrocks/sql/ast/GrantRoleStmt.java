// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.UserIdentity;

// GRANT Role 'role' TO 'user'
// share the same parameter and check logic with RevokeRoleStmt
public class GrantRoleStmt extends BaseGrantRevokeRoleStmt {

    public GrantRoleStmt(String role, UserIdentity userIdent) {
        super(role, userIdent, "GRANT", "TO");
    }
}
