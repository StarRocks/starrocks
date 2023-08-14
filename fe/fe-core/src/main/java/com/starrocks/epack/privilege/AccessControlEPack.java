// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.privilege;

import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.privilege.AccessControl;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.sql.ast.UserIdentity;

import java.util.Set;

public interface AccessControlEPack extends AccessControl {

    default void checkPolicyAction(UserIdentity currentUser, Set<Long> roleIds, PolicyType policyType, String catalogName,
                                   String db, String policy, PrivilegeType privilegeType) {
    }

    default void checkAnyActionOnPolicy(UserIdentity currentUser, Set<Long> roleIds, PolicyType policyType, String catalogName,
                                        String db, String policy) {
    }

    default void checkAnyActionOnAnyPolicy(UserIdentity currentUser, Set<Long> roleIds, PolicyType policyType, String catalogName,
                                           String db) {
    }

    default void checkWarehouseAction(UserIdentity currentUser, Set<Long> roleIds, String name, PrivilegeType privilegeType) {
    }

    default void checkAnyActionOnWarehouse(UserIdentity currentUser, Set<Long> roleIds, String name) {
    }
}
