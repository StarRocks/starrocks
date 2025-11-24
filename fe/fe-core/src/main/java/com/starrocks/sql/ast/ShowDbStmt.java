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

// Show database statement.
public class ShowDbStmt extends ShowStmt {
    private final String pattern;
    private Expr where;

    private String catalogName;

    public ShowDbStmt(String pattern) {
        this(pattern, null, null, NodePosition.ZERO);
    }

    public ShowDbStmt(String pattern, Expr where) {
        this(pattern, where, null, NodePosition.ZERO);
    }

    public ShowDbStmt(String pattern, String catalogName) {
        this(pattern, null, catalogName, NodePosition.ZERO);
    }

    public ShowDbStmt(String pattern, Expr where, String catalogName) {
        this(pattern, where, catalogName, NodePosition.ZERO);
    }

    public ShowDbStmt(String pattern, Expr where, String catalogName, NodePosition pos) {
        super(pos);
        this.pattern = pattern;
        this.where = where;
        this.catalogName = normalizeName(catalogName);
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
        return ((AstVisitorExtendInterface<R, C>) visitor).visitShowDatabasesStatement(this, context);
    }
}
