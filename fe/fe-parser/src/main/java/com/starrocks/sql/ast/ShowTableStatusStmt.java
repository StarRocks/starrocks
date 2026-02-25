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

// SHOW TABLE STATUS
public class ShowTableStatusStmt extends ShowStmt {
    private String db;
    private final String wild;
    private Expr where;

    public ShowTableStatusStmt(String db, String wild, Expr where) {
        this(db, wild, where, NodePosition.ZERO);
    }

    public ShowTableStatusStmt(String db, String wild, Expr where, NodePosition pos) {
        super(pos);
        this.db = db;
        this.wild = wild;
        this.where = where;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getDb() {
        return db;
    }

    public String getPattern() {
        return wild;
    }

    public Expr getWhereClause() {
        return where;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowTableStatusStatement(this, context);
    }
}
