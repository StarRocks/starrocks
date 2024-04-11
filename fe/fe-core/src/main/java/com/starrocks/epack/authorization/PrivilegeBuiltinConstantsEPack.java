// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.authorization;

import com.google.common.collect.ImmutableSet;
import com.starrocks.privilege.PrivilegeBuiltinConstants;

import java.util.Set;

public class PrivilegeBuiltinConstantsEPack extends PrivilegeBuiltinConstants {
    public static final long ALL_POLICY_ID = -5;
    public static final long ALL_WAREHOUSES_ID = -1; // -1 represent all warehouses

    public static final String SECURITY_ADMIN_ROLE_NAME = "security_admin";
    public static final long SECURITY_ADMIN_ROLE_ID = -106;

    public static final Set<String> BUILT_IN_ROLE_NAMES = new ImmutableSet.Builder<String>()
            .addAll(PrivilegeBuiltinConstants.BUILT_IN_ROLE_NAMES)
            .add(SECURITY_ADMIN_ROLE_NAME)
            .build();

    public static final Set<Long> IMMUTABLE_BUILT_IN_ROLE_IDS = new ImmutableSet.Builder<Long>()
            .addAll(PrivilegeBuiltinConstants.IMMUTABLE_BUILT_IN_ROLE_IDS)
            .add(SECURITY_ADMIN_ROLE_ID)
            .build();

    public static final Set<String> IMMUTABLE_BUILT_IN_ROLE_NAMES = new ImmutableSet.Builder<String>()
            .addAll(PrivilegeBuiltinConstants.IMMUTABLE_BUILT_IN_ROLE_NAMES)
            .add(SECURITY_ADMIN_ROLE_NAME)
            .build();
}
