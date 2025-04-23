// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.authorization;

import com.starrocks.authorization.AccessControlProvider;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Authorizer;

public class AuthorizerEPack extends Authorizer {

    public AuthorizerEPack(AccessControlProvider accessControlProvider) {
        super(accessControlProvider);
    }

    public static void checkPolicyAction(ConnectContext context, PolicyType policyType, String catalogName,
                                         String db, String policy, PrivilegeType privilegeType) throws AccessDeniedException {
        String catalog = catalogName == null ? InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : catalogName;
        ((AccessControllerEPack) getInstance().getAccessControlOrDefault(catalog))
                .checkPolicyAction(context, policyType, catalog, db, policy, privilegeType);
    }

    public static void checkAnyActionOnPolicy(ConnectContext context, PolicyType policyType,
                                              String catalogName, String db, String policy) throws AccessDeniedException {
        String catalog = catalogName == null ? InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : catalogName;
        ((AccessControllerEPack) getInstance().getAccessControlOrDefault(catalog)).checkAnyActionOnPolicy(
                context, policyType, catalog, db, policy);
    }

    public static void checkFailoverGroupAction(ConnectContext context, String name,
                                                PrivilegeType privilegeType) throws AccessDeniedException {
        ((AccessControllerEPack) getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME))
                .checkFailoverGroupAction(context, name, privilegeType);
    }

    public static void checkAnyActionOnFailoverGroup(ConnectContext context, String name)
            throws AccessDeniedException {
        ((AccessControllerEPack) getInstance().getAccessControlOrDefault(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME))
                .checkAnyActionOnFailoverGroup(context, name);
    }
}
