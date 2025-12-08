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

import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.NodePosition;

// SHOW TABLES
public class ShowTableStmt extends ShowStmt {
    private String db;
    private final boolean isVerbose;
    private final String pattern;
    private Expr where;
    private String catalogName;

    public ShowTableStmt(String db, boolean isVerbose, String pattern) {
        this(db, isVerbose, pattern, null, null, NodePosition.ZERO);
    }

    public ShowTableStmt(String db, boolean isVerbose, String pattern, String catalogName) {
        this(db, isVerbose, pattern, null, catalogName, NodePosition.ZERO);
    }

    public ShowTableStmt(String db, boolean isVerbose, String pattern, Expr where, String catalogName) {
        this(db, isVerbose, pattern, where, catalogName, NodePosition.ZERO);
    }

    public ShowTableStmt(String db, boolean isVerbose, String pattern, Expr where,
                         String catalogName, NodePosition pos) {
        super(pos);
        this.db = db;
        this.isVerbose = isVerbose;
        this.pattern = pattern;
        this.where = where;
        this.catalogName = catalogName;
    }

    public String getDb() {
        return db;
    }

    public boolean isVerbose() {
        return isVerbose;
    }

    public String getPattern() {
        return pattern;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public Expr getWhereClause() {
        return where;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowTableStatement(this, context);
    }
}
