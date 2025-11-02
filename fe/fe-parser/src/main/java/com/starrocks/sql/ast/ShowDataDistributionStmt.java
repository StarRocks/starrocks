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

import com.starrocks.sql.parser.NodePosition;

public class ShowDataDistributionStmt extends ShowStmt {
    private final TableRef tableRef;

    public ShowDataDistributionStmt(TableRef tblRef) {
        this(tblRef, NodePosition.ZERO);
    }

    public ShowDataDistributionStmt(TableRef tblRef, NodePosition pos) {
        super(pos);
        this.tableRef = tblRef;
    }

    public String getDbName() {
        return tableRef.getDbName();
    }

    public String getTblName() {
        return tableRef.getTableName();
    }

    public PartitionRef getPartitionDef() {
        return tableRef.getPartitionDef();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowDataDistributionStatement(this, context);
    }
}

