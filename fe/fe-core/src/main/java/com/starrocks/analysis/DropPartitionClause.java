// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/DropPartitionClause.java

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

// clause which is used to add one column to
public class DropPartitionClause extends AlterTableClause {
    private final boolean ifExists;
    private final String partitionName;
    // true if this is to drop a temp partition
    private final boolean isTempPartition;
    private final boolean forceDrop;

    public DropPartitionClause(boolean ifExists, String partitionName, boolean isTempPartition, boolean forceDrop) {
        super(AlterOpType.DROP_PARTITION);
        this.ifExists = ifExists;
        this.partitionName = partitionName;
        this.isTempPartition = isTempPartition;
        this.needTableStable = false;
        this.forceDrop = forceDrop;
    }

    public boolean isSetIfExists() {
        return ifExists;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public boolean isTempPartition() {
        return isTempPartition;
    }

    public boolean isForceDrop() {
        return forceDrop;
    }

    @Override
    public Map<String, String> getProperties() {
        return null;
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropPartitionClause(this, context);
    }
}
