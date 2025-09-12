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
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class OptimizeClause extends AlterTableClause {
    private KeysDesc keysDesc;
    private PartitionDesc partitionDesc;
    private DistributionDesc distributionDesc;
    private PartitionNames partitionNames;
    private OptimizeRange range;

    @SerializedName(value = "sourcePartitionIds")
    private List<Long> sourcePartitionIds = Lists.newArrayList();

    @SerializedName(value = "isTableOptimize")
    private boolean isTableOptimize = false;

    // It saves the original sort order elements parsing from the order by clause.
    // Because other sort properties, such as sort-direction and null-orders, are not supported in optimize clause now.
    // We extract its sort columns to `sortKeys` in analyze phase and use the `sortKeys` instead of it in most places.
    private List<OrderByElement> orderByElements;
    // It will be set based on `orderByElements` in analyze
    private List<String> sortKeys;

    public OptimizeClause(KeysDesc keysDesc,
                          PartitionDesc partitionDesc,
                          DistributionDesc distributionDesc,
                          List<OrderByElement> orderByElements,
                          PartitionNames partitionNames,
                          OptimizeRange range) {
        this(keysDesc, partitionDesc, distributionDesc, orderByElements, partitionNames, range, NodePosition.ZERO);
    }

    public OptimizeClause(KeysDesc keysDesc,
                          PartitionDesc partitionDesc,
                          DistributionDesc distributionDesc,
                          List<OrderByElement> orderByElements,
                          PartitionNames partitionNames,
                          OptimizeRange range,
                          NodePosition pos) {
        super(pos);
        this.keysDesc = keysDesc;
        this.partitionDesc = partitionDesc;
        this.distributionDesc = distributionDesc;
        this.orderByElements = orderByElements;
        this.partitionNames = partitionNames;
        this.range = range;
    }

    // Add getter and setter for OptimizeRange
    public OptimizeRange getRange() {
        return range;
    }

    public void setRange(OptimizeRange range) {
        this.range = range;
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

    public List<OrderByElement> getOrderByElements() {
        return orderByElements;
    }

    public void setSortKeys(List<String> sortKeys) {
        this.sortKeys = sortKeys;
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

    public boolean isTableOptimize() {
        return isTableOptimize;
    }

    public void setTableOptimize(boolean tableOptimize) {
        isTableOptimize = tableOptimize;
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
        if (range != null) {
            sb.append(range.toString());
        }
        return sb.toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitOptimizeClause(this, context);
    }
}
