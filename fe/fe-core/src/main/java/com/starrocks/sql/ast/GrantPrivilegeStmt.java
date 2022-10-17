// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.UserIdentity;

import java.util.Arrays;
import java.util.List;

public class GrantPrivilegeStmt extends BaseGrantRevokePrivilegeStmt {
    public GrantPrivilegeStmt(
            List<String> privList,
            String privType,
            GrantRevokeClause grantRevokeClause,
            GrantRevokePrivilegeObjects objects) {
        super(privList, privType, grantRevokeClause, objects);
    }

    /**
     * The following 2 functions is used to generate sql when excuting `show grants` in old privilege framework
     */
    public GrantPrivilegeStmt(List<String> privList, String privType, UserIdentity userIdentity) {
        super(privList, privType, new GrantRevokeClause(userIdentity, null, false),
                new GrantRevokePrivilegeObjects());
    }

    public void setUserPrivilegeObject(UserIdentity userIdentity) {
        this.objects.setUserPrivilegeObjectList(Arrays.asList(userIdentity));
    }
}
