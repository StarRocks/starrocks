// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.authorization;

import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.privilege.AccessController;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.PrivilegeType;
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

    default void checkWarehouseAction(UserIdentity currentUser, Set<Long> roleIds, String name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnWarehouse(UserIdentity currentUser, Set<Long> roleIds, String name)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }
}
