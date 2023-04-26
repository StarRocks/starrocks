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

public class CreateTableLikeStmt extends DdlStmt {
    private final boolean ifNotExists;
    private final TableName tableName;
    private final TableName existedTableName;

    private CreateTableStmt createTableStmt;

    public CreateTableLikeStmt(boolean ifNotExists, TableName tableName, TableName existedTableName) {
        this(ifNotExists, tableName, existedTableName, NodePosition.ZERO);
    }

    public CreateTableLikeStmt(boolean ifNotExists, TableName tableName,
                               TableName existedTableName, NodePosition pos) {
        super(pos);
        this.ifNotExists = ifNotExists;
        this.tableName = tableName;
        this.existedTableName = existedTableName;
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    public String getDbName() {
        return tableName.getDb();
    }

    public String getTableName() {
        return tableName.getTbl();
    }

    public String getExistedDbName() {
        return existedTableName.getDb();
    }

    public String getExistedTableName() {
        return existedTableName.getTbl();
    }

    public TableName getDbTbl() {
        return tableName;
    }

    public TableName getExistedDbTbl() {
        return existedTableName;
    }

    public CreateTableStmt getCreateTableStmt() {
        return createTableStmt;
    }

    public void setCreateTableStmt(CreateTableStmt createTableStmt) {
        this.createTableStmt = createTableStmt;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateTableLikeStatement(this, context);
    }
}
