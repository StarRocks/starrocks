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

import static com.starrocks.common.util.Util.normalizeName;

// Show create database statement
//  Syntax:
//      SHOW CREATE DATABASE db
public class ShowCreateDbStmt extends ShowStmt {

    private String catalog;
    private String db;

    public ShowCreateDbStmt(String db) {
        this(db, NodePosition.ZERO);
    }

    public ShowCreateDbStmt(String db, NodePosition pos) {
        super(pos);
        this.db = normalizeName(db);
    }

    public String getCatalogName() {
        return this.catalog;
    }

    public void setCatalogName(String catalogName) {
        this.catalog = normalizeName(catalogName);
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = normalizeName(db);
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitShowCreateDbStatement(this, context);
    }
}
