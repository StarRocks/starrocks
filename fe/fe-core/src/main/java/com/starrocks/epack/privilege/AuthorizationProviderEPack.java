// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.privilege;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.privilege.DefaultAuthorizationProvider;
import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.PEntryObject;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.server.GlobalStateMgr;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AuthorizationProviderEPack extends DefaultAuthorizationProvider {

    private static final Map<ObjectType, List<PrivilegeType>> TYPE_TO_ACTION_LIST_EPACK =
            ImmutableMap.<ObjectType, List<PrivilegeType>>builder()
                    .putAll(TYPE_TO_ACTION_LIST)
                    .put(ObjectTypeEPack.MASKING_POLICY, ImmutableList.of(
                            PrivilegeTypeEPack.APPLY, PrivilegeType.DROP, PrivilegeType.ALTER))
                    .put(ObjectTypeEPack.ROW_ACCESS_POLICY, ImmutableList.of(
                            PrivilegeTypeEPack.APPLY, PrivilegeType.DROP, PrivilegeType.ALTER))
                    .build();

    @Override
    public Set<ObjectType> getAllPrivObjectTypes() {
        return TYPE_TO_ACTION_LIST_EPACK.keySet();
    }

    @Override
    public List<PrivilegeType> getAvailablePrivType(ObjectType objectType) {
        return new ArrayList<>(TYPE_TO_ACTION_LIST_EPACK.get(objectType));
    }

    @Override
    public boolean isAvailablePrivType(ObjectType objectType, PrivilegeType privilegeType) {
        if (!TYPE_TO_ACTION_LIST_EPACK.containsKey(objectType)) {
            return false;
        }
        return TYPE_TO_ACTION_LIST_EPACK.get(objectType).contains(privilegeType);
    }

    @Override
    public PEntryObject generateObject(ObjectType objectType, List<String> objectTokens, GlobalStateMgr mgr)
            throws PrivilegeException {
        if (ObjectTypeEPack.ROW_ACCESS_POLICY.equals(objectType)) {
            return PolicyPEntryObject.generate(mgr, PolicyType.ROW_ACCESS, objectTokens);
        } else if (ObjectTypeEPack.MASKING_POLICY.equals(objectType)) {
            return PolicyPEntryObject.generate(mgr, PolicyType.MASKING, objectTokens);
        } else {
            return super.generateObject(objectType, objectTokens, mgr);
        }
    }
}
