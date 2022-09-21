// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.alter.AlterOpType;

// alter table clause
public abstract class AlterTableClause extends AlterClause {

    public AlterTableClause(AlterOpType opType) {
        super(opType);
    }

    // if set to true, the corresponding table should be stable before processing this operation on it.
    protected boolean needTableStable = true;

    public boolean isNeedTableStable() {
        return needTableStable;
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
