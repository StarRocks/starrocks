// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.UserIdentity;

/**
 * grantRevokeClause
 *     : (user | ROLE identifierOrString ) (WITH GRANT OPTION)?
 *     ;
 */
public class GrantRevokeClause implements ParseNode  {
    private UserIdentity userIdentity;
    private String roleName;
    private boolean withGrantOption;

    public GrantRevokeClause(UserIdentity userIdentifier, String roleName, boolean withGrantOption) {
        this.userIdentity = userIdentifier;
        this.roleName = roleName;
        this.withGrantOption = withGrantOption;
    }

    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

    public String getRoleName() {
        return roleName;
    }

    public boolean isWithGrantOption() {
        return withGrantOption;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return null;
    }
}
