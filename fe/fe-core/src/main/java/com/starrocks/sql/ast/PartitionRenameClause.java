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

<<<<<<< HEAD
import java.util.Map;

=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
// rename table
public class PartitionRenameClause extends AlterTableClause {
    private final String partitionName;
    private final String newPartitionName;

    public PartitionRenameClause(String partitionName, String newPartitionName) {
        this(partitionName, newPartitionName, NodePosition.ZERO);
    }

    public PartitionRenameClause(String partitionName, String newPartitionName, NodePosition pos) {
        super(AlterOpType.RENAME, pos);
        this.partitionName = partitionName;
        this.newPartitionName = newPartitionName;
<<<<<<< HEAD
        this.needTableStable = false;
=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    public String getPartitionName() {
        return partitionName;
    }

    public String getNewPartitionName() {
        return newPartitionName;
    }

    @Override
<<<<<<< HEAD
    public Map<String, String> getProperties() {
        return null;
    }

    @Override
=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitPartitionRenameClause(this, context);
    }
}
