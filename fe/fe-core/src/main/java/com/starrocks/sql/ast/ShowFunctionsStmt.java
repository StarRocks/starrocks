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

import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

public class ShowFunctionsStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Signature", ScalarType.createVarchar(256)))
                    .addColumn(new Column("Return Type", ScalarType.createVarchar(32)))
                    .addColumn(new Column("Function Type", ScalarType.createVarchar(16)))
                    .addColumn(new Column("Intermediate Type", ScalarType.createVarchar(16)))
                    .addColumn(new Column("Properties", ScalarType.createVarchar(16)))
                    .build();

    private String dbName;
    private final boolean isBuiltin;
    private final boolean isGlobal;
    private final boolean isVerbose;
    private final String wild;
    private final Expr expr;

    public ShowFunctionsStmt(String dbName, boolean isBuiltin, boolean isGlobal, boolean isVerbose, String wild,
                             Expr expr) {
        this(dbName, isBuiltin, isGlobal, isVerbose, wild, expr, NodePosition.ZERO);
    }

    public ShowFunctionsStmt(String dbName, boolean isBuiltin, boolean isGlobal, boolean isVerbose, String wild,
                             Expr expr, NodePosition pos) {
        super(pos);
        this.dbName = dbName;
        this.isBuiltin = isBuiltin;
        this.isGlobal = isGlobal;
        this.isVerbose = isVerbose;
        this.wild = wild;
        this.expr = expr;
    }

    public String getDbName() {
        return dbName;
    }

    public boolean getIsBuiltin() {
        return isBuiltin;
    }

    public boolean getIsGlobal() {
        return isGlobal;
    }

    public boolean getIsVerbose() {
        return isVerbose;
    }

    public String getWild() {
        return wild;
    }

    public Expr getExpr() {
        return expr;
    }

    public boolean like(String str) {
        str = str.toLowerCase();
        return str.matches(wild.replace(".", "\\.").replace("?", ".").replace("%", ".*").toLowerCase());
    }

    public void setDbName(String db) {
        this.dbName = db;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowFunctionsStatement(this, context);
    }
}
