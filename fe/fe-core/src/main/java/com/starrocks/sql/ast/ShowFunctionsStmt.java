// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;

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
    private final boolean isVerbose;
    private final String wild;
    private final Expr expr;

    public ShowFunctionsStmt(String dbName, boolean isBuiltin, boolean isVerbose, String wild, Expr expr) {
        this.dbName = dbName;
        this.isBuiltin = isBuiltin;
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
