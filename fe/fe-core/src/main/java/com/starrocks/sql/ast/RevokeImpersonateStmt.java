// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.UserIdentity;

// REVOKE IMPERSONATE ON USER FROM USER
// share the same parameter and check logic with GrantImpersonateStmt
public class RevokeImpersonateStmt extends BaseGrantRevokeImpersonateStmt {

    public RevokeImpersonateStmt(UserIdentity authorizedUser, UserIdentity securedUser) {
        super(authorizedUser, securedUser, "REVOKE", "FROM");
    }

    public RevokeImpersonateStmt(String authorizedRoleName, UserIdentity securedUser) {
        super(authorizedRoleName, securedUser, "REVOKE", "FROM");
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
