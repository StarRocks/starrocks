// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.catalog.RefreshType;
import com.starrocks.common.UserException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.RefreshSchemeDesc;

public class ManualRefreshSchemeDesc extends RefreshSchemeDesc {

    public ManualRefreshSchemeDesc() {
        this.type = RefreshType.MANUAL;
    }

}

