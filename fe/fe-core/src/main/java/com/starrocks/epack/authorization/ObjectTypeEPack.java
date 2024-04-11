// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.authorization;

import com.google.common.collect.ImmutableMap;
import com.starrocks.privilege.ObjectType;

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
        } else if (id == WAREHOUSE.id) {
            return "WAREHOUSE";
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
        } else if (id == WAREHOUSE.id) {
            return "WAREHOUSES";
        } else {
            return "UNKNOWN";
        }
    }

    public static final ObjectTypeEPack MASKING_POLICY = new ObjectTypeEPack(20001);
    public static final ObjectTypeEPack ROW_ACCESS_POLICY = new ObjectTypeEPack(20002);
    public static final ObjectTypeEPack WAREHOUSE = new ObjectTypeEPack(20003);

    public static final Map<String, ObjectType> NAME_TO_OBJECT = new ImmutableMap.Builder<String, ObjectType>()
            .putAll(ObjectType.NAME_TO_OBJECT)
            .put(MASKING_POLICY.name(), MASKING_POLICY)
            .put(ROW_ACCESS_POLICY.name(), ROW_ACCESS_POLICY)
            .put(WAREHOUSE.name(), WAREHOUSE)
            .build();

    public static final Map<String, ObjectType> PLURAL_TO_OBJECT = new ImmutableMap.Builder<String, ObjectType>()
            .putAll(ObjectType.PLURAL_TO_OBJECT)
            .put(MASKING_POLICY.plural(), MASKING_POLICY)
            .put(ROW_ACCESS_POLICY.plural(), ROW_ACCESS_POLICY)
            .put(WAREHOUSE.plural(), WAREHOUSE)
            .build();
}
