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

// DROP DB Statement
public class DropDbStmt extends DdlStmt {
    private final boolean ifExists;
    private String catalog;
    private String dbName;
    private final boolean forceDrop;

    public DropDbStmt(boolean ifExists, String dbName, boolean forceDrop) {
        this(ifExists, dbName, forceDrop, NodePosition.ZERO);
    }

    public DropDbStmt(boolean ifExists, String dbName, boolean forceDrop, NodePosition pos) {
        super(pos);
        this.ifExists = ifExists;
        this.dbName = dbName;
        this.forceDrop = forceDrop;
    }

    public boolean isSetIfExists() {
        return ifExists;
    }

    public String getCatalogName() {
        return this.catalog;
    }

    public void setCatalogName(String catalogName) {
        this.catalog = catalogName;
    }

    public String getDbName() {
        return this.dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public boolean isForceDrop() {
        return this.forceDrop;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropDbStatement(this, context);
    }

}
