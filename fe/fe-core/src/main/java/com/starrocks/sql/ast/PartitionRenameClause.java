// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.alter.AlterOpType;

import java.util.Map;

// rename table
public class PartitionRenameClause extends AlterTableClause {
    private final String partitionName;
    private final String newPartitionName;

    public PartitionRenameClause(String partitionName, String newPartitionName) {
        super(AlterOpType.RENAME);
        this.partitionName = partitionName;
        this.newPartitionName = newPartitionName;
        this.needTableStable = false;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public String getNewPartitionName() {
        return newPartitionName;
    }

    @Override
    public Map<String, String> getProperties() {
        return null;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitPartitionRenameClause(this, context);
    }
}
