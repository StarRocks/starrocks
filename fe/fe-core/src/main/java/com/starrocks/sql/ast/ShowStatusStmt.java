// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;

/**
 * Show Status statement
 * Acceptable syntax:
 * SHOW [GLOBAL | LOCAL | SESSION] STATUS [LIKE 'pattern' | WHERE expr]
 */
public class ShowStatusStmt extends ShowStmt {

    private static final String NAME_COL = "Variable_name";
    private static final String VALUE_COL = "Value";
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column(NAME_COL, ScalarType.createVarchar(20)))
                    .addColumn(new Column(VALUE_COL, ScalarType.createVarchar(20)))
                    .build();

    private final SetType type;
    private String pattern;
    private Expr where;

    public ShowStatusStmt() {
        this.type = SetType.DEFAULT;
    }

    public ShowStatusStmt(SetType type, String pattern, Expr where) {
        this.type = type;
        this.pattern = pattern;
        this.where = where;
    }

    public SetType getType() {
        return type;
    }

    public String getPattern() {
        return pattern;
    }

    public Expr getWhere() {
        return where;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}
