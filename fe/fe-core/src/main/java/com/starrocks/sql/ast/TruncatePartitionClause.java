// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.alter.AlterOpType;

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


}
