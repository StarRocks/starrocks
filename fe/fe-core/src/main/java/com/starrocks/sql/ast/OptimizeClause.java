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

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.alter.AlterOpType;
import com.starrocks.analysis.KeysDesc;
import com.starrocks.sql.parser.NodePosition;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;

public class OptimizeClause extends AlterTableClause {
    private KeysDesc keysDesc;
    private PartitionDesc partitionDesc;
    private DistributionDesc distributionDesc;
    private PartitionNames partitionNames;

    @SerializedName(value = "sourcePartitionIds")
    List<Long> sourcePartitionIds = Lists.newArrayList();

    private List<String> sortKeys = Lists.newArrayList();

    public OptimizeClause(KeysDesc keysDesc,
                           PartitionDesc partitionDesc,
                           DistributionDesc distributionDesc,
                           List<String> sortKeys,
                           PartitionNames partitionNames) {
        this(keysDesc, partitionDesc, distributionDesc, sortKeys, partitionNames, NodePosition.ZERO);
    }

    public OptimizeClause(KeysDesc keysDesc,
                           PartitionDesc partitionDesc,
                           DistributionDesc distributionDesc,
                           List<String> sortKeys,
                           PartitionNames partitionNames,
                           NodePosition pos) {
        super(AlterOpType.OPTIMIZE, pos);
        this.keysDesc = keysDesc;
        this.partitionDesc = partitionDesc;
        this.distributionDesc = distributionDesc;
        this.sortKeys = sortKeys;
        this.partitionNames = partitionNames;
    }

    public KeysDesc getKeysDesc() {
        return this.keysDesc;
    }

    public PartitionDesc getPartitionDesc() {
        return this.partitionDesc;
    }

    public DistributionDesc getDistributionDesc() {
        return this.distributionDesc;
    }

    public List<String> getSortKeys() {
        return sortKeys;
    }

    public void setDistributionDesc(DistributionDesc distributionDesc) {
        this.distributionDesc = distributionDesc;
    }

    public void setPartitionDesc(PartitionDesc partitionDesc) {
        this.partitionDesc = partitionDesc;
    }

    public PartitionNames getPartitionNames() {
        return partitionNames;
    }

    public void setSourcePartitionIds(List<Long> sourcePartitionIds) {
        this.sourcePartitionIds = sourcePartitionIds;
    }

    public List<Long> getSourcePartitionIds() {
        return sourcePartitionIds;
    }

    public static OptimizeClause read(DataInput in) throws IOException {
        throw new RuntimeException("OptimizeClause serialization is not supported anymore.");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER ");
        if (partitionDesc != null) {
            sb.append(partitionDesc.toString());
        }
        if (distributionDesc != null) {
            sb.append(distributionDesc.toString());
        }
        if (keysDesc != null) {
            sb.append(keysDesc.toSql());
        }
        if (sortKeys != null && !sortKeys.isEmpty()) {
            sb.append(String.join(",", sortKeys));
        }
        return sb.toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitOptimizeClause(this, context);
    }
}
