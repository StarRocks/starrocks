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

// SHOW COLUMNS
public class ShowColumnStmt extends ShowStmt {
    private TableRef tableRef;
    private final String pattern;
    private final boolean isVerbose;
    private Expr where;

    public ShowColumnStmt(TableRef tableRef, String pattern, boolean isVerbose) {
        this(tableRef, pattern, isVerbose, null, NodePosition.ZERO);
    }

    public ShowColumnStmt(TableRef tableRef, String pattern, boolean isVerbose, Expr where) {
        this(tableRef, pattern, isVerbose, where, NodePosition.ZERO);
    }

    public ShowColumnStmt(TableRef tableRef, String pattern, boolean isVerbose,
                          Expr where, NodePosition pos) {
        super(pos);
        this.tableRef = tableRef;
        this.pattern = pattern;
        this.isVerbose = isVerbose;
        this.where = where;
    }

    public String getCatalog() {
        return tableRef == null ? null : tableRef.getCatalogName();
    }

    public String getDb() {
        return tableRef == null ? null : tableRef.getDbName();
    }

    public String getTable() {
        return tableRef == null ? null : tableRef.getTableName();
    }

    public boolean isVerbose() {
        return isVerbose;
    }

    public String getPattern() {
        return pattern;
    }

    public TableRef getTableRef() {
        return tableRef;
    }

    public void setTableRef(TableRef tableRef) {
        this.tableRef = tableRef;
    }

    public Expr getWhereClause() {
        return where;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitShowColumnStatement(this, context);
    }
}
