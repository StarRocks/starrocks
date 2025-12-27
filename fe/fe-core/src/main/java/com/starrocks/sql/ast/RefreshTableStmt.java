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

import java.util.List;

/**
 * This command used to refresh connector table of external catalog.
 * For example:
 * 'REFRESH EXTERNAL TABLE catalog1.db1.table1'
 * This sql will refresh table1 of db1 in catalog1.
 */
public class RefreshTableStmt extends DdlStmt {
    private TableRef tableRef;
    private final List<String> partitionNames;

    public RefreshTableStmt(TableRef tableRef, List<String> partitionNames) {
        this(tableRef, partitionNames, NodePosition.ZERO);
    }

    public RefreshTableStmt(TableRef tableRef, List<String> partitionNames, NodePosition pos) {
        super(pos);
        this.tableRef = tableRef;
        this.partitionNames = partitionNames;
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

    public List<String> getPartitions() {
        return partitionNames;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitRefreshTableStatement(this, context);
    }
}
