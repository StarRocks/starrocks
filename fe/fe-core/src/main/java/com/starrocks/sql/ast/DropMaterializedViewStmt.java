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

/**
 * DROP MATERIALIZED VIEW [ IF EXISTS ] <mv_name> IN|FROM [db_name].<table_name>
 * <p>
 * Parameters
 * IF EXISTS: Do not throw an error if the materialized view does not exist. A notice is issued in this case.
 * mv_name: The name of the materialized view to remove.
 * db_name: The name of db to which materialized view belongs.
 * table_name: The name of table to which materialized view belongs.
 */
public class DropMaterializedViewStmt extends DdlStmt {

    private final boolean ifExists;
    private TableRef tableRef;

    public DropMaterializedViewStmt(boolean ifExists, TableRef tableRef) {
        this(ifExists, tableRef, NodePosition.ZERO);
    }

    public DropMaterializedViewStmt(boolean ifExists, TableRef tableRef, NodePosition pos) {
        super(pos);
        this.ifExists = ifExists;
        this.tableRef = tableRef;
    }

    public boolean isSetIfExists() {
        return ifExists;
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

    public String getMvName() {
        return tableRef == null ? null : tableRef.getTableName();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitDropMaterializedViewStatement(this, context);
    }
}
