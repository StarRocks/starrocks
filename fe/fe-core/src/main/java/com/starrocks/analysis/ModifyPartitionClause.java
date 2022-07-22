// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.starrocks.alter.AlterOpType;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.sql.ast.AstVisitor;

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
    public ModifyPartitionClause(List<String> partitionNames, Map<String, String> properties) {
        super(AlterOpType.MODIFY_PARTITION);
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
    private ModifyPartitionClause(Map<String, String> properties) {
        super(AlterOpType.MODIFY_PARTITION);
        this.partitionNames = Lists.newArrayList();
        this.properties = properties;
        this.needExpand = true;
        this.needTableStable = false;
    }

    public static ModifyPartitionClause createStarClause(Map<String, String> properties) {
        return new ModifyPartitionClause(properties);
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    public boolean isNeedExpand() {
        return this.needExpand;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("MODIFY PARTITION ");
        sb.append("(");
        if (needExpand) {
            sb.append("*");
        } else {
            sb.append(Joiner.on(", ").join(partitionNames));
        }
        sb.append(")");
        sb.append(" SET (");
        sb.append(new PrintableMap<String, String>(properties, "=", true, false));
        sb.append(")");

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitModifyPartitionClause(this, context);
    }
}
