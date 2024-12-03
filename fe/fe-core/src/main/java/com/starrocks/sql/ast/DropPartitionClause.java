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
import com.starrocks.analysis.Expr;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

// clause which is used to add one column to
public class DropPartitionClause extends AlterTableClause {
    private final boolean ifExists;
    private final String partitionName;
    // true if this is to drop a temp partition
    private final boolean isTempPartition;
    private final boolean forceDrop;
    private final MultiRangePartitionDesc multiRangePartitionDesc;
    private final List<String> partitionNames;
    private final Expr dropWhereExpr;

    //Object Resolved by Analyzer
    private List<String> resolvedPartitionNames;

    public DropPartitionClause(boolean ifExists, String partitionName, boolean isTempPartition, boolean forceDrop) {
        this(ifExists, partitionName, isTempPartition, forceDrop, NodePosition.ZERO);
    }

    public DropPartitionClause(boolean ifExists, String partitionName, boolean isTempPartition,
                               boolean forceDrop, NodePosition pos) {
        super(AlterOpType.DROP_PARTITION, pos);
        this.ifExists = ifExists;
        this.partitionName = partitionName;
        this.isTempPartition = isTempPartition;
        this.forceDrop = forceDrop;
        this.multiRangePartitionDesc = null;
        this.partitionNames = null;
        this.dropWhereExpr = null;
    }

    public DropPartitionClause(boolean ifExists, List<String> partitionNames, boolean isTempPartition,
                               boolean forceDrop, NodePosition pos) {
        super(AlterOpType.DROP_PARTITION, pos);
        this.ifExists = ifExists;
        this.partitionName = null;
        this.isTempPartition = isTempPartition;
        this.forceDrop = forceDrop;
        this.multiRangePartitionDesc = null;
        this.partitionNames = partitionNames;
        this.dropWhereExpr = null;
    }

    public DropPartitionClause(boolean ifExists, MultiRangePartitionDesc multiRangePartitionDesc, boolean isTempPartition,
                               boolean forceDrop, NodePosition pos) {
        super(AlterOpType.DROP_PARTITION, pos);
        this.ifExists = ifExists;
        this.partitionName = null;
        this.isTempPartition = isTempPartition;
        this.forceDrop = forceDrop;
        this.multiRangePartitionDesc = multiRangePartitionDesc;
        this.partitionNames = null;
        this.dropWhereExpr = null;
    }

    public DropPartitionClause(boolean ifExists, Expr whereExpr, boolean isTempPartition,
                               boolean forceDrop, NodePosition pos) {
        super(AlterOpType.DROP_PARTITION, pos);
        this.ifExists = ifExists;
        this.partitionName = null;
        this.isTempPartition = isTempPartition;
        this.forceDrop = forceDrop;
        this.multiRangePartitionDesc = null;
        this.partitionNames = null;
        this.dropWhereExpr = whereExpr;
    }

    public Expr getDropWhereExpr() {
        return dropWhereExpr;
    }

    public List<String> getResolvedPartitionNames() {
        return resolvedPartitionNames;
    }

    public void setResolvedPartitionNames(List<String> resolvedPartitionNames) {
        this.resolvedPartitionNames = resolvedPartitionNames;
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

    public MultiRangePartitionDesc getMultiRangePartitionDesc() {
        return multiRangePartitionDesc;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropPartitionClause(this, context);
    }
}
