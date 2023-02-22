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

public class AlterDatabaseRenameStatement extends DdlStmt {
    private String catalog;
    private String dbName;
    private final String newDbName;

    public AlterDatabaseRenameStatement(String dbName, String newDbName) {
        this(dbName, newDbName, NodePosition.ZERO);
    }

    public AlterDatabaseRenameStatement(String dbName, String newDbName, NodePosition pos) {
        super(pos);
        this.dbName = dbName;
        this.newDbName = newDbName;
    }

    public String getCatalogName() {
        return this.catalog;
    }

    public void setCatalogName(String catalogName) {
        this.catalog = catalogName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getNewDbName() {
        return newDbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterDatabaseRenameStatement(this, context);
    }

}
