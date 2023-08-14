// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.privilege;

import com.starrocks.catalog.InternalCatalog;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.UserIdentity;

import java.util.Set;

public class AuthorizerEPack extends Authorizer {

    public static void checkPolicyAction(UserIdentity currentUser, Set<Long> roleIds, PolicyType policyType, String catalogName,
                                         String db, String policy, PrivilegeType privilegeType) {
        ((AccessControlEPack) getInstance().getAccessControlOrDefault(catalogName))
                .checkPolicyAction(currentUser, roleIds, policyType, catalogName, db, policy, privilegeType);
    }

    public static void checkAnyActionOnPolicy(UserIdentity currentUser, Set<Long> roleIds, PolicyType policyType,
                                              String catalogName, String db, String policy) {
        ((AccessControlEPack) getInstance().getAccessControlOrDefault(catalogName)).checkAnyActionOnPolicy(
                currentUser, roleIds, policyType, catalogName, db, policy);
    }

    public static void checkWarehouseAction(UserIdentity currentUser, Set<Long> roleIds, String name,
                                            PrivilegeType privilegeType) {
        ((AccessControlEPack) getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME))
                .checkWarehouseAction(currentUser, roleIds, name, privilegeType);
    }

    public static void checkAnyActionOnWarehouse(UserIdentity currentUser, Set<Long> roleIds, String name) {
        ((AccessControlEPack) getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME))
                .checkAnyActionOnWarehouse(currentUser, roleIds, name);
    }
}
