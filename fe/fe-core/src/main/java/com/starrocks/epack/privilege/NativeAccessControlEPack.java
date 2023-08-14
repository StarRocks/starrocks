// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.privilege;

import com.google.common.collect.Lists;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.NativeAccessControl;
import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.sql.ast.UserIdentity;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class NativeAccessControlEPack extends NativeAccessControl implements AccessControlEPack {
    @Override
    public void checkPolicyAction(UserIdentity currentUser, Set<Long> roleIds, PolicyType policyType, String catalogName,
                                  String db, String policy, PrivilegeType privilegeType) {
        List<String> objectTokens = Lists.newArrayList(catalogName, db, policy);
        ObjectType objectType = policyType.equals(PolicyType.MASKING) ? ObjectTypeEPack.MASKING_POLICY :
                ObjectTypeEPack.ROW_ACCESS_POLICY;
        if (!checkObjectTypeAction(currentUser, roleIds, privilegeType, objectType, objectTokens)) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), objectType, policy);
        }
    }

    @Override
    public void checkAnyActionOnPolicy(UserIdentity currentUser, Set<Long> roleIds, PolicyType policyType, String catalogName,
                                       String db, String policy) {
        List<String> objectTokens = Lists.newArrayList(catalogName, db, policy);
        ObjectType objectType = policyType.equals(PolicyType.MASKING) ? ObjectTypeEPack.MASKING_POLICY :
                ObjectTypeEPack.ROW_ACCESS_POLICY;
        if (!checkAnyActionOnObject(currentUser, roleIds, objectType, objectTokens)) {
            AccessDeniedException.reportAccessDenied("ANY", objectType, policy);
        }
    }

    @Override
    public void checkAnyActionOnAnyPolicy(UserIdentity currentUser, Set<Long> roleIds, PolicyType policyType, String catalogName,
                                          String db) {
        checkAnyActionOnPolicy(currentUser, roleIds, policyType, catalogName, db, "*");
    }


    @Override
    public void checkWarehouseAction(UserIdentity currentUser, Set<Long> roleIds, String name, PrivilegeType privilegeType) {
        if (!checkObjectTypeAction(currentUser, roleIds, privilegeType, ObjectTypeEPack.WAREHOUSE,
                Collections.singletonList(name))) {
            AccessDeniedException.reportAccessDenied(privilegeType.name(), ObjectTypeEPack.WAREHOUSE, name);
        }
    }

    @Override
    public void checkAnyActionOnWarehouse(UserIdentity currentUser, Set<Long> roleIds, String name) {
        if (!checkAnyActionOnObject(currentUser, roleIds, ObjectTypeEPack.WAREHOUSE, Collections.singletonList(name))) {
            AccessDeniedException.reportAccessDenied("ANY", ObjectTypeEPack.WAREHOUSE, name);
        }
    }
}
