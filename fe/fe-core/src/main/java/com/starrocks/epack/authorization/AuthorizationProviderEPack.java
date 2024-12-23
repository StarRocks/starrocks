// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.authorization;

import com.starrocks.authorization.DefaultAuthorizationProvider;
import com.starrocks.authorization.ObjectType;
import com.starrocks.authorization.PEntryObject;
import com.starrocks.authorization.PrivilegeException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.authorization.WarehousePEntryObject;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import org.apache.hadoop.util.Lists;

import java.util.List;

public class AuthorizationProviderEPack extends DefaultAuthorizationProvider {

    public AuthorizationProviderEPack() {
        super();

        typeToActionList.get(ObjectType.DATABASE).addAll(Lists.newArrayList(
                PrivilegeTypeEPack.CREATE_MASKING_POLICY,
                PrivilegeTypeEPack.CREATE_ROW_ACCESS_POLICY));

        typeToActionList.get(ObjectType.SYSTEM).addAll(Lists.newArrayList(
                PrivilegeTypeEPack.CREATE_WAREHOUSE,
                PrivilegeTypeEPack.SECURITY,
                PrivilegeTypeEPack.CREATE_FAILOVER_GROUP));

        typeToActionList.put(ObjectTypeEPack.MASKING_POLICY, Lists.newArrayList(
                PrivilegeTypeEPack.APPLY,
                PrivilegeType.DROP,
                PrivilegeType.ALTER));

        typeToActionList.put(ObjectTypeEPack.ROW_ACCESS_POLICY, Lists.newArrayList(
                PrivilegeTypeEPack.APPLY,
                PrivilegeType.DROP,
                PrivilegeType.ALTER));

        typeToActionList.put(ObjectTypeEPack.FAILOVER_GROUP, Lists.newArrayList(
                PrivilegeType.USAGE,
                PrivilegeType.ALTER,
                PrivilegeType.DROP));
    }


    @Override
    public PrivilegeType getPrivilegeType(String privTypeString) {
        return PrivilegeTypeEPack.NAME_TO_PRIVILEGE.get(privTypeString);
    }

    @Override
    public ObjectType getObjectType(String objectTypeUnResolved) {
        if (ObjectTypeEPack.NAME_TO_OBJECT.containsKey(objectTypeUnResolved)) {
            return ObjectTypeEPack.NAME_TO_OBJECT.get(objectTypeUnResolved);
        }

        if (ObjectTypeEPack.PLURAL_TO_OBJECT.containsKey(objectTypeUnResolved)) {
            return ObjectTypeEPack.PLURAL_TO_OBJECT.get(objectTypeUnResolved);
        }

        throw new SemanticException("cannot find privilege object type " + objectTypeUnResolved);
    }

    @Override
    public PEntryObject generateObject(ObjectType objectType, List<String> objectTokens, GlobalStateMgr mgr)
            throws PrivilegeException {
        if (ObjectTypeEPack.ROW_ACCESS_POLICY.equals(objectType)) {
            return PolicyPEntryObject.generate(mgr, PolicyType.ROW_ACCESS, objectTokens);
        } else if (ObjectTypeEPack.MASKING_POLICY.equals(objectType)) {
            return PolicyPEntryObject.generate(mgr, PolicyType.MASKING, objectTokens);
        } else if (ObjectType.WAREHOUSE.equals(objectType)) {
            return WarehousePEntryObject.generate(mgr, objectTokens);
        } else if (ObjectTypeEPack.FAILOVER_GROUP.equals(objectType)) {
            return FailoverGroupPEntryObject.generate(mgr, objectTokens);
        } else {
            return super.generateObject(objectType, objectTokens, mgr);
        }
    }
}
