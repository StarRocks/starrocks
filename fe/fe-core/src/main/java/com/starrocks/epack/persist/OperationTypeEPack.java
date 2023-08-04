// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.persist;

import com.starrocks.persist.OperationType;

public class OperationTypeEPack extends OperationType {
    public static final short OP_CREATE_MASKING_POLICY = 20001;
    public static final short OP_CREATE_ROW_ACCESS_POLICY = 20002;
    public static final short OP_DROP_POLICY = 20003;
    public static final short OP_ALTER_POLICY_SET_BODY = 20004;
    public static final short OP_ALTER_POLICY_SET_COMMENT = 20005;
    public static final short OP_ALTER_POLICY_RENAME = 20006;
    public static final short OP_APPLY_MASKING_POLICY = 20007;
    public static final short OP_APPLY_ROW_ACCESS_POLICY = 20008;
    public static final short OP_REVOKE_MASKING_POLICY = 20009;
    public static final short OP_REVOKE_ROW_ACCESS_POLICY = 20010;

    // warehouse
    public static final short OP_CREATE_WAREHOUSE = 20101;
    public static final short OP_DROP_WAREHOUSE = 20102;
    public static final short OP_ALTER_WAREHOUSE = 20103;

    // security integration and role mapping
    public static final short OP_CREATE_SECURITY_INTEGRATION = 20269;
    public static final short OP_CREATE_ROLE_MAPPING = 20270;
    public static final short OP_DROP_SECURITY_INTEGRATION = 20271;
    public static final short OP_ALTER_SECURITY_INTEGRATION = 20272;
    public static final short OP_DROP_ROLE_MAPPING = 20273;
    public static final short OP_ALTER_ROLE_MAPPING = 20274;
}