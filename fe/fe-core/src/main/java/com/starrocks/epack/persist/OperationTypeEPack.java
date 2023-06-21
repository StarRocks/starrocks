// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.persist;

import com.starrocks.persist.OperationType;

public class OperationTypeEPack extends OperationType {
    public static final short OP_CREATE_MASKING_POLICY = 20001;
    public static final short OP_CREATE_ROW_ACCESS_POLICY = 20002;
}