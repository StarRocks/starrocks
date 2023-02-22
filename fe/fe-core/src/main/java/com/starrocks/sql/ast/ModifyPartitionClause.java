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
import com.starrocks.alter.AlterOpType;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Map;

// clause which is used to modify partition properties
// 1. Modify Partition p1 set ("replication_num" = "3")
// 2. Modify Partition (p1, p3, p4) set ("replication_num" = "3")
// 3. Modify Partition (*) set ("replication_num" = "3")
public class ModifyPartitionClause extends AlterTableClause {

    private List<String> partitionNames;
    private Map<String, String> properties;
    private boolean needExpand = false;

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    // c'tor for non-star clause
    public ModifyPartitionClause(List<String> partitionNames, Map<String, String> propertie) {
        this(partitionNames, propertie, NodePosition.ZERO);
    }

    public ModifyPartitionClause(List<String> partitionNames, Map<String, String> properties, NodePosition pos) {
        super(AlterOpType.MODIFY_PARTITION, pos);
        this.partitionNames = partitionNames;
        this.properties = properties;
        this.needExpand = false;
        // ATTN: currently, modify partition only allow 3 kinds of operations:
        // 1. modify replication num
        // 2. modify data property
        // 3. modify in memory
        // And these 3 operations does not require table to be stable.
        // If other kinds of operations be added later, "needTableStable" may be changed.
        this.needTableStable = false;
    }

    // c'tor for 'Modify Partition(*)' clause
    private ModifyPartitionClause(Map<String, String> properties, NodePosition pos) {
        super(AlterOpType.MODIFY_PARTITION, pos);
        this.partitionNames = Lists.newArrayList();
        this.properties = properties;
        this.needExpand = true;
        this.needTableStable = false;
    }

    public static ModifyPartitionClause createStarClause(Map<String, String> properties) {
        return new ModifyPartitionClause(properties, NodePosition.ZERO);
    }


    public static ModifyPartitionClause createStarClause(Map<String, String> properties, NodePosition pos) {
        return new ModifyPartitionClause(properties, pos);
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    public boolean isNeedExpand() {
        return this.needExpand;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitModifyPartitionClause(this, context);
    }
}
