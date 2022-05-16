// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.ast;

import com.starrocks.catalog.RefreshType;

public class SyncRefreshSchemeDesc extends RefreshSchemeDesc {

    public SyncRefreshSchemeDesc() {
        super(RefreshType.SYNC);
    }

}

