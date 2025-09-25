// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.authorization;

import com.starrocks.authorization.PrivilegeBuiltinConstants;

public class PrivilegeBuiltinConstantsEPack extends PrivilegeBuiltinConstants {
    public static final long ALL_POLICY_ID = -5;
    public static final long ALL_FAILOVER_GROUPS_ID = -1; // -1 represent all failover groups
}
