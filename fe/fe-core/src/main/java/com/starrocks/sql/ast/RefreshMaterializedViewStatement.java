// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.ast;

import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.TableName;

public class RefreshMaterializedViewStatement extends DdlStmt {
    private final TableName mvName;
    private final RangePartitionWithoutIntervalDesc rangePartitionWithoutIntervalDesc;
    private final boolean force;

    public RefreshMaterializedViewStatement(TableName mvName,
                                            RangePartitionWithoutIntervalDesc rangePartitionWithoutIntervalDesc,
                                            boolean force) {
        this.mvName = mvName;
        this.rangePartitionWithoutIntervalDesc = rangePartitionWithoutIntervalDesc;
        this.force = force;
    }

    public TableName getMvName() {
        return mvName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRefreshMaterializedViewStatement(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }

    public RangePartitionWithoutIntervalDesc getRangePartitionWithoutIntervalDesc() {
        return rangePartitionWithoutIntervalDesc;
    }

    public boolean isForce() {
        return force;
    }
}
