// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.ParseNode;
import com.starrocks.catalog.MaterializedView;

public class RefreshSchemeDesc implements ParseNode {

    protected MaterializedView.RefreshType type;

    public RefreshSchemeDesc(MaterializedView.RefreshType type) {
        this.type = type;
    }

    public MaterializedView.RefreshType getType() {
        return type;
    }

}
