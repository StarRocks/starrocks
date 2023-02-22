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

import com.starrocks.analysis.TableName;
import com.starrocks.sql.parser.NodePosition;

public class RecoverPartitionStmt extends DdlStmt {
    private final TableName dbTblName;
    private final String partitionName;

    public RecoverPartitionStmt(TableName dbTblName, String partitionName) {
        this(dbTblName, partitionName, NodePosition.ZERO);
    }

    public RecoverPartitionStmt(TableName dbTblName, String partitionName, NodePosition pos) {
        super(pos);
        this.dbTblName = dbTblName;
        this.partitionName = partitionName;
    }

    public String getDbName() {
        return dbTblName.getDb();
    }

    public String getTableName() {
        return dbTblName.getTbl();
    }

    public TableName getDbTblName() {
        return dbTblName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRecoverPartitionStatement(this, context);
    }
}
