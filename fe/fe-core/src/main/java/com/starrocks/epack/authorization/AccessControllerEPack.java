// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.authorization;

import com.starrocks.authorization.AccessController;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.qe.ConnectContext;

public interface AccessControllerEPack extends AccessController {

    default void checkPolicyAction(ConnectContext context, PolicyType policyType, String catalogName,
                                   String db, String policy, PrivilegeType privilegeType) throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnPolicy(ConnectContext context, PolicyType policyType, String catalogName,
                                        String db, String policy) throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnAnyPolicy(ConnectContext context, PolicyType policyType, String catalogName,
                                           String db) throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkFailoverGroupAction(ConnectContext context, String name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }

    default void checkAnyActionOnFailoverGroup(ConnectContext context, String name)
            throws AccessDeniedException {
        throw new AccessDeniedException();
    }
}
