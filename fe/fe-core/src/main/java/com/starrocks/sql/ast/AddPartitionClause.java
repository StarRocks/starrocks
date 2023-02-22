// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.sql.ast;

import com.starrocks.alter.AlterOpType;
import com.starrocks.sql.parser.NodePosition;

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
        this(partitionDesc, distributionDesc, properties, isTempPartition, NodePosition.ZERO);
    }

    public AddPartitionClause(PartitionDesc partitionDesc,
                              DistributionDesc distributionDesc,
                              Map<String, String> properties,
                              boolean isTempPartition, NodePosition pos) {
        super(AlterOpType.ADD_PARTITION, pos);
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
}
