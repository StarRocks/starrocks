// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.privilege;

import com.google.common.collect.ImmutableMap;
import com.starrocks.privilege.PrivilegeType;

import java.util.Map;

public class PrivilegeTypeEPack extends PrivilegeType {
    private PrivilegeTypeEPack(int id, String name) {
        super(id, name);
    }

    public static final PrivilegeTypeEPack CREATE_MASKING_POLICY = new PrivilegeTypeEPack(20001,
            "CREATE MASKING POLICY");
    public static final PrivilegeTypeEPack CREATE_ROW_ACCESS_POLICY = new PrivilegeTypeEPack(20002,
            "CREATE ROW ACCESS POLICY");
    public static final PrivilegeTypeEPack APPLY = new PrivilegeTypeEPack(20003, "APPLY");
    public static final PrivilegeTypeEPack CREATE_WAREHOUSE =
            new PrivilegeTypeEPack(20004, "CREATE WAREHOUSE");
    public static final PrivilegeTypeEPack SECURITY = new PrivilegeTypeEPack(20005, "SECURITY");

    public static final Map<String, PrivilegeType> NAME_TO_PRIVILEGE =
            new ImmutableMap.Builder<String, PrivilegeType>()
                    .putAll(PrivilegeType.NAME_TO_PRIVILEGE)
                    .put(CREATE_MASKING_POLICY.name(), CREATE_MASKING_POLICY)
                    .put(CREATE_ROW_ACCESS_POLICY.name(), CREATE_ROW_ACCESS_POLICY)
                    .put(APPLY.name(), APPLY)
                    .put(CREATE_WAREHOUSE.name(), CREATE_WAREHOUSE)
                    .put(SECURITY.name(), SECURITY)
                    .build();

    @Override
    public String name() {
        if (VALID_PRIVILEGE_TYPE.contains(this)) {
            return name;
        } else if (id == CREATE_MASKING_POLICY.id) {
            return CREATE_MASKING_POLICY.name;
        } else if (id == CREATE_ROW_ACCESS_POLICY.id) {
            return CREATE_ROW_ACCESS_POLICY.name;
        } else if (id == APPLY.id) {
            return APPLY.name;
        } else if (id == CREATE_WAREHOUSE.id) {
            return CREATE_WAREHOUSE.name;
        } else if (id == SECURITY.id) {
            return SECURITY.name;
        } else {
            return "UNKNOWN";
        }
    }
}
