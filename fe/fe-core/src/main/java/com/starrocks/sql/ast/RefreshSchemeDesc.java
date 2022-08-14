// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.ParseNode;
import com.starrocks.catalog.RefreshType;

public class RefreshSchemeDesc implements ParseNode {

    protected RefreshType type;

    public RefreshSchemeDesc(RefreshType type) {
        this.type = type;
    }

    public RefreshType getType() {
        return type;
    }

}
