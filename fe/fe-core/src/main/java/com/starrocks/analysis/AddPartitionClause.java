// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/AddPartitionClause.java

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

import com.starrocks.alter.AlterOpType;
import com.starrocks.sql.ast.AstVisitor;

import java.util.Map;

// clause which is used to add partition
public class AddPartitionClause extends AlterTableClause {

    private PartitionDesc partitionDesc;
    private DistributionDesc distributionDesc;
    private Map<String, String> properties;
    // true if this is to add a temporary partition
    private boolean isTempPartition;
    private boolean isMultiAlter;

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
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ADD ");
        sb.append(partitionDesc.toSql() + "\n");
        if (distributionDesc != null) {
            sb.append(distributionDesc.toSql());
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAddPartitionClause(this, context);
    }
}
