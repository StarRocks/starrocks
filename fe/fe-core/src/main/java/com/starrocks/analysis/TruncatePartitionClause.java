// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.alter.AlterOpType;
import com.starrocks.sql.ast.AstVisitor;

public class TruncatePartitionClause extends AlterTableClause {

    private PartitionNames partitionNames;
    public TruncatePartitionClause(AlterOpType opType) {
        super(opType);
    }

    public TruncatePartitionClause(PartitionNames partitionNames) {
        super(AlterOpType.TRUNCATE_PARTITION);
        this.partitionNames = partitionNames;
    }

    public PartitionNames getPartitionNames() {
        return partitionNames;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitTruncatePartitionClause(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
