// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.ast;

import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.UserIdentity;

/**
 * TODO this class is merely a wrapper of UserIdentity,
 * they should be merged after all statement migrate to the new framework
 */
public class UserIdentifier implements ParseNode {
    private final UserIdentity userIdentity;

    public UserIdentifier(String name, String host, boolean isDomain) {
        userIdentity = new UserIdentity(name, host, isDomain);
    }

    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return null;
    }
}
