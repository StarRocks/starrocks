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

import com.starrocks.sql.ast.ShowAlterStmt.AlterType;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

/*
 * CANCEL ALTER COLUMN|ROLLUP FROM db_name.table_name
 */
public class CancelAlterTableStmt extends CancelStmt {

    private final AlterType alterType;

    private TableRef tableRef;

    public AlterType getAlterType() {
        return alterType;
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

    private final List<Long> alterJobIdList;

    public CancelAlterTableStmt(AlterType alterType, TableRef tableRef) {
        this(alterType, tableRef, null);
    }

    public CancelAlterTableStmt(AlterType alterType, TableRef tableRef, List<Long> alterJobIdList) {
        this(alterType, tableRef, alterJobIdList, NodePosition.ZERO);
    }

    public CancelAlterTableStmt(AlterType alterType, TableRef tableRef, List<Long> alterJobIdList, NodePosition pos) {
        super(pos);
        this.alterType = alterType;
        this.tableRef = tableRef;
        this.alterJobIdList = alterJobIdList;
    }

    public List<Long> getAlterJobIdList() {
        return alterJobIdList;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitCancelAlterTableStatement(this, context);
    }

    @Override
    public String toString() {
        return toSql();
    }

}
