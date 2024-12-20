// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.authorization;

import com.starrocks.authorization.AccessController;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.sql.ast.UserIdentity;

import java.util.Set;

public interface AccessControllerEPack extends AccessController {

    default void checkPolicyAction(UserIdentity currentUser, Set<Long> roleIds, PolicyType policyType, String catalogName,
                                   String db, String policy, PrivilegeType privilegeType) throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnPolicy(UserIdentity currentUser, Set<Long> roleIds, PolicyType policyType, String catalogName,
                                        String db, String policy) throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnAnyPolicy(UserIdentity currentUser, Set<Long> roleIds, PolicyType policyType, String catalogName,
                                           String db) throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkFailoverGroupAction(UserIdentity currentUser, Set<Long> roleIds, String name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnFailoverGroup(UserIdentity currentUser, Set<Long> roleIds, String name)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }
}
