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
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.type.Type;

import java.util.List;

// to describe the key range partition's information in create table stmt
public class RangePartitionDesc extends PartitionDesc {

    private final List<String> partitionColNames;
    private final List<SingleRangePartitionDesc> singleRangePartitionDescs;
    private final List<MultiRangePartitionDesc> multiRangePartitionDescs;
    private List<String> partitionNames = Lists.newArrayList();
    // for automatic partition table is ture. otherwise is false
    protected boolean isAutoPartitionTable = false;
    // For automatically created partitioned tables, the partition column type and expression type may be inconsistent.
    protected Type partitionType;

    public RangePartitionDesc(List<String> partitionColNames, List<PartitionDesc> partitionDescs) {
        this(partitionColNames, partitionDescs, NodePosition.ZERO);
    }

    public RangePartitionDesc(List<String> partitionColNames, List<PartitionDesc> partitionDescs, NodePosition pos) {
        super(pos);
        this.partitionColNames = partitionColNames;

        singleRangePartitionDescs = Lists.newArrayList();
        multiRangePartitionDescs = Lists.newArrayList();
        if (partitionDescs != null) {
            for (PartitionDesc partitionDesc : partitionDescs) {
                if (partitionDesc instanceof SingleRangePartitionDesc) {
                    singleRangePartitionDescs.add((SingleRangePartitionDesc) partitionDesc);
                } else if (partitionDesc instanceof MultiRangePartitionDesc) {
                    multiRangePartitionDescs.add((MultiRangePartitionDesc) partitionDesc);
                }
            }
        }
    }

    public List<SingleRangePartitionDesc> getSingleRangePartitionDescs() {
        return this.singleRangePartitionDescs;
    }

    public List<MultiRangePartitionDesc> getMultiRangePartitionDescs() {
        return multiRangePartitionDescs;
    }

    public List<String> getPartitionColNames() {
        return partitionColNames;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public void setPartitionNames(List<String> partitionNames) {
        this.partitionNames = partitionNames;
    }

    public void setAutoPartitionTable(boolean autoPartitionTable) {
        this.isAutoPartitionTable = autoPartitionTable;
    }

    public boolean isAutoPartitionTable() {
        return isAutoPartitionTable;
    }

    public Type getPartitionType() {
        return partitionType;
    }

    public void setPartitionType(Type partitionType) {
        this.partitionType = partitionType;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PARTITION BY RANGE(");
        int idx = 0;
        for (String column : partitionColNames) {
            if (idx != 0) {
                sb.append(", ");
            }
            sb.append("`").append(column).append("`");
            idx++;
        }
        sb.append(")\n(\n");

        for (int i = 0; i < singleRangePartitionDescs.size(); i++) {
            if (i != 0) {
                sb.append(",\n");
            }
            sb.append(singleRangePartitionDescs.get(i).toString());
        }
        sb.append("\n)");
        return sb.toString();
    }
}
