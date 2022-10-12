// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.alter.AlterOpType;

import java.util.Map;

// clause which is used to add partition
public class AddPartitionClause extends AlterTableClause {

    private final PartitionDesc partitionDesc;
    private final DistributionDesc distributionDesc;
    private final Map<String, String> properties;
    // true if this is to add a temporary partition
    private final boolean isTempPartition;

    public PartitionDesc getPartitionDesc() {
        return partitionDesc;
    }

    public DistributionDesc getDistributionDesc() {
        return distributionDesc;
    }

    public boolean isTempPartition() {
        return isTempPartition;
    }

    public AddPartitionClause(PartitionDesc partitionDesc,
                              DistributionDesc distributionDesc,
                              Map<String, String> properties,
                              boolean isTempPartition) {
        super(AlterOpType.ADD_PARTITION);
        this.partitionDesc = partitionDesc;
        this.distributionDesc = distributionDesc;
        this.properties = properties;
        this.isTempPartition = isTempPartition;

        this.needTableStable = false;
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAddPartitionClause(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
