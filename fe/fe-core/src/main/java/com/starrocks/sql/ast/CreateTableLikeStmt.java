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

import java.util.Map;

public class CreateTableLikeStmt extends DdlStmt {
    private final boolean ifNotExists;
    private TableRef tableRef;
    private TableRef existedTableRef;

    private final PartitionDesc partitionDesc;
    private final DistributionDesc distributionDesc;
    private final Map<String, String> properties;

    private CreateTableStmt createTableStmt;

    public CreateTableLikeStmt(boolean ifNotExists, TableRef tableRef,
                               TableRef existedTableRef,
                               PartitionDesc partitionDesc,
                               DistributionDesc distributionDesc,
                               Map<String, String> properties,
                               NodePosition pos) {
        super(pos);
        this.ifNotExists = ifNotExists;
        this.tableRef = tableRef;
        this.existedTableRef = existedTableRef;
        this.partitionDesc = partitionDesc;
        this.distributionDesc = distributionDesc;
        this.properties = properties;
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    public TableRef getTableRef() {
        return tableRef;
    }

    public void setTableRef(TableRef tableRef) {
        this.tableRef = tableRef;
    }

    public TableRef getExistedTableRef() {
        return existedTableRef;
    }

    public void setExistedTableRef(TableRef existedTableRef) {
        this.existedTableRef = existedTableRef;
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

    public String getExistedCatalogName() {
        return existedTableRef == null ? null : existedTableRef.getCatalogName();
    }

    public String getExistedDbName() {
        return existedTableRef == null ? null : existedTableRef.getDbName();
    }

    public String getExistedTableName() {
        return existedTableRef == null ? null : existedTableRef.getTableName();
    }

    public CreateTableStmt getCreateTableStmt() {
        return createTableStmt;
    }

    public void setCreateTableStmt(CreateTableStmt createTableStmt) {
        this.createTableStmt = createTableStmt;
    }

    public PartitionDesc getPartitionDesc() {
        return partitionDesc;
    }

    public DistributionDesc getDistributionDesc() {
        return distributionDesc;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitCreateTableLikeStatement(this, context);
    }
}
