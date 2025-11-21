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

import static com.starrocks.common.util.Util.normalizeName;

// Show rollup statement, used to show rollup information of one table.
//
// Syntax:
//      SHOW MATERIALIZED VIEWS { FROM | IN } db
public class ShowMaterializedViewsStmt extends ShowStmt {
    private String db;

    private String catalogName;

    private final String pattern;

    private Expr where;

    public ShowMaterializedViewsStmt(String catalogName, String db) {
        this(catalogName, db, null, null, NodePosition.ZERO);
    }

    public ShowMaterializedViewsStmt(String catalogName, String db, String pattern) {
        this(catalogName, db, pattern, null, NodePosition.ZERO);
    }

    public ShowMaterializedViewsStmt(String catalogName, String db, Expr where) {
        this(catalogName, db, null, where, NodePosition.ZERO);
    }

    public ShowMaterializedViewsStmt(String catalogName, String db, String pattern, Expr where, NodePosition pos) {
        super(pos);
        this.catalogName = normalizeName(catalogName);
        this.db = normalizeName(db);
        this.pattern = pattern;
        this.where = where;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = normalizeName(db);
    }

    public String getPattern() {
        return pattern;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public Expr getWhereClause() {
        return where;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitShowMaterializedViewStatement(this, context);
    }
}
