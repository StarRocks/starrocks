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

import com.starrocks.catalog.TableName;
import com.starrocks.sql.parser.NodePosition;

public class RecoverPartitionStmt extends DdlStmt {
    private TableRef tableRef;
    private final String partitionName;

    public RecoverPartitionStmt(TableRef tableRef, String partitionName) {
        this(tableRef, partitionName, NodePosition.ZERO);
    }

    public RecoverPartitionStmt(TableRef tableRef, String partitionName, NodePosition pos) {
        super(pos);
        this.tableRef = tableRef;
        this.partitionName = partitionName;
    }

    public TableRef getTableRef() {
        return tableRef;
    }

    public void setTableRef(TableRef tableRef) {
        this.tableRef = tableRef;
    }

    public String getCatalogName() {
        return tableRef == null ? null : tableRef.getCatalogName();
    }

    public String getDbName() {
        return tableRef == null ? null : tableRef.getDbName();
    }

    public String getTableName() {
        return tableRef == null ? null : tableRef.getTableName();
    }

    public TableName toTableName() {
        if (tableRef == null) {
            return null;
        }
        return new TableName(getCatalogName(), getDbName(), getTableName(), tableRef.getPos());
    }

    public String getPartitionName() {
        return partitionName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitRecoverPartitionStatement(this, context);
    }
}
