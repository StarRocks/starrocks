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

public class DropStatsStmt extends StatementBase {
    private TableRef tableRef;
    private boolean isExternal = false;
    private boolean isMultiColumn = false;

    public DropStatsStmt(TableRef tableRef) {
        this(tableRef, false, NodePosition.ZERO);
    }

    public DropStatsStmt(TableRef tableRef, boolean isMultiColumn, NodePosition pos) {
        super(pos);
        this.tableRef = tableRef;
        this.isMultiColumn = isMultiColumn;
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

    public boolean isExternal() {
        return isExternal;
    }

    public void setExternal(boolean isExternal) {
        this.isExternal = isExternal;
    }

    public boolean isMultiColumn() {
        return isMultiColumn;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitDropStatsStatement(this, context);
    }
}
