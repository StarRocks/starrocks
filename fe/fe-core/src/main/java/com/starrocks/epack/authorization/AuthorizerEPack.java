// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.authorization;

import com.starrocks.catalog.InternalCatalog;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.privilege.AccessControlProvider;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.UserIdentity;

import java.util.Set;

public class AuthorizerEPack extends Authorizer {

    public AuthorizerEPack(AccessControlProvider accessControlProvider) {
        super(accessControlProvider);
    }

    public static void checkPolicyAction(UserIdentity currentUser, Set<Long> roleIds, PolicyType policyType, String catalogName,
                                         String db, String policy, PrivilegeType privilegeType) throws AccessDeniedException {
        String catalog = catalogName == null ? InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : catalogName;
        ((AccessControllerEPack) getInstance().getAccessControlOrDefault(catalog))
                .checkPolicyAction(currentUser, roleIds, policyType, catalog, db, policy, privilegeType);
    }

    public static void checkAnyActionOnPolicy(UserIdentity currentUser, Set<Long> roleIds, PolicyType policyType,
                                              String catalogName, String db, String policy) throws AccessDeniedException {
        String catalog = catalogName == null ? InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : catalogName;
        ((AccessControllerEPack) getInstance().getAccessControlOrDefault(catalog)).checkAnyActionOnPolicy(
                currentUser, roleIds, policyType, catalog, db, policy);
    }

    public static void checkWarehouseAction(UserIdentity currentUser, Set<Long> roleIds, String name,
                                            PrivilegeType privilegeType) throws AccessDeniedException {
        ((AccessControllerEPack) getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME))
                .checkWarehouseAction(currentUser, roleIds, name, privilegeType);
    }

    public static void checkAnyActionOnWarehouse(UserIdentity currentUser, Set<Long> roleIds, String name)
            throws AccessDeniedException {
        // Any user has an implicit usage permission on the default_warehouse
        if (!WarehouseManager.isDefaultWarehouse(name)) {
            ((AccessControllerEPack) getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME))
                    .checkAnyActionOnWarehouse(currentUser, roleIds, name);
        }
    }
}
