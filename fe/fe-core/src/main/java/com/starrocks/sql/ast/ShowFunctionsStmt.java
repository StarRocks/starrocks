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

public class ShowFunctionsStmt extends ShowStmt {

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
        this.dbName = normalizeName(dbName);
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
        this.dbName = normalizeName(db);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitShowFunctionsStatement(this, context);
    }
}
