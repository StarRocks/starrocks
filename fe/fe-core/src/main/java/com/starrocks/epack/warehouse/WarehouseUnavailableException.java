// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.warehouse;

import com.starrocks.common.UserException;

public class WarehouseUnavailableException extends UserException {
    public WarehouseUnavailableException(String msg) {
        super(msg);
    }
}
