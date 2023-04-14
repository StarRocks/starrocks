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

public class CompactionClause extends AlterTableClause {

    private List<String> partitionNames;
    private boolean baseCompaction = true;

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public CompactionClause(List<String> partitionNames, boolean baseCompaction, NodePosition pos) {
        super(AlterOpType.COMPACT, pos);
        this.partitionNames = partitionNames;
        this.baseCompaction = baseCompaction;
        this.needTableStable = false;
    }

    public CompactionClause(boolean baseCompaction, NodePosition pos) {
        super(AlterOpType.COMPACT, pos);
        this.partitionNames = Lists.newArrayList();
        this.baseCompaction = baseCompaction;
        this.needTableStable = false;
    }

    public boolean isBaseCompaction() {
        return this.baseCompaction;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCompactionClause(this, context);
    }
}
