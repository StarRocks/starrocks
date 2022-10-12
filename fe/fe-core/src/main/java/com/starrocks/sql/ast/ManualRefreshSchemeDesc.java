// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;


import com.starrocks.catalog.MaterializedView;

public class ManualRefreshSchemeDesc extends RefreshSchemeDesc {

    public ManualRefreshSchemeDesc() {
        super(MaterializedView.RefreshType.MANUAL);
    }

}

