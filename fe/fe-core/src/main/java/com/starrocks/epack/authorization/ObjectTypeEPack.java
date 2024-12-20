// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.authorization;

import com.google.common.collect.ImmutableMap;
import com.starrocks.authorization.ObjectType;

import java.util.Map;

public class ObjectTypeEPack extends ObjectType {
    private ObjectTypeEPack(int id) {
        super(id);
    }

    @Override
    public String name() {
        if (OBJECT_TO_NAME.get(id) != null) {
            return OBJECT_TO_NAME.get(id).first;
        } else if (id == MASKING_POLICY.id) {
            return "MASKING POLICY";
        } else if (id == ROW_ACCESS_POLICY.id) {
            return "ROW ACCESS POLICY";
        } else if (id == FAILOVER_GROUP.id) {
            return "FAILOVER GROUP";
        } else {
            return "UNKNOWN";
        }
    }

    @Override
    public String plural() {
        if (OBJECT_TO_NAME.get(id) != null) {
            return OBJECT_TO_NAME.get(id).second;
        } else if (id == MASKING_POLICY.id) {
            return "MASKING POLICIES";
        } else if (id == ROW_ACCESS_POLICY.id) {
            return "ROW ACCESS POLICIES";
        } else if (id == FAILOVER_GROUP.id) {
            return "FAILOVER GROUPS";
        } else {
            return "UNKNOWN";
        }
    }

    public static final ObjectTypeEPack MASKING_POLICY = new ObjectTypeEPack(20001);
    public static final ObjectTypeEPack ROW_ACCESS_POLICY = new ObjectTypeEPack(20002);
    public static final ObjectTypeEPack FAILOVER_GROUP = new ObjectTypeEPack(20004);

    public static final Map<String, ObjectType> NAME_TO_OBJECT = new ImmutableMap.Builder<String, ObjectType>()
            .putAll(ObjectType.NAME_TO_OBJECT)
            .put(MASKING_POLICY.name(), MASKING_POLICY)
            .put(ROW_ACCESS_POLICY.name(), ROW_ACCESS_POLICY)
            .put(FAILOVER_GROUP.name(), FAILOVER_GROUP)
            .build();

    public static final Map<String, ObjectType> PLURAL_TO_OBJECT = new ImmutableMap.Builder<String, ObjectType>()
            .putAll(ObjectType.PLURAL_TO_OBJECT)
            .put(MASKING_POLICY.plural(), MASKING_POLICY)
            .put(ROW_ACCESS_POLICY.plural(), ROW_ACCESS_POLICY)
            .put(FAILOVER_GROUP.plural(), FAILOVER_GROUP)
            .build();
}
