// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.ast;

import com.starrocks.analysis.UserIdentity;

// GRANT IMPERSONATE ON USER TO USER
// share the same parameter and check logic with RevokeImpersonateStmt
public class GrantImpersonateStmt extends BaseGrantRevokeImpersonateStmt {

    public GrantImpersonateStmt(UserIdentity authorizedUser, UserIdentity securedUser) {
        super(authorizedUser, securedUser, "GRANT", "TO");
    }
<<<<<<< HEAD
=======

    public GrantImpersonateStmt(String authorizedRoleName, UserIdentity securedUser) {
        super(authorizedRoleName, securedUser, "GRANT", "TO");
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
>>>>>>> 4d7266f88 ([BugFix] persist grant/revoke role and support grant impersonate to role (#10596))
}