// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;

/**
 * Show charset statement
 * Acceptable syntax:
 * SHOW {CHAR SET | CHARSET}
 * [LIKE 'pattern' | WHERE expr]
 */
public class ShowCharsetStmt extends ShowStmt {
    private static final String CHAR_SET_COL = "Charset";
    private static final String DESC_COL = "Description";
    private static final String DEFAULT_COLLATION_COL = "Default collation";
    private static final String MAX_LEN_COL = "Maxlen";

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column(CHAR_SET_COL, ScalarType.createVarchar(20)))
                    .addColumn(new Column(DESC_COL, ScalarType.createVarchar(20)))
                    .addColumn(new Column(DEFAULT_COLLATION_COL, ScalarType.createVarchar(20)))
                    .addColumn(new Column(MAX_LEN_COL, ScalarType.createVarchar(20)))
                    .build();

    private String pattern;
    private Expr where;

    public ShowCharsetStmt() {
    }

    public ShowCharsetStmt(String pattern) {
        this.pattern = pattern;
    }

    public ShowCharsetStmt(String pattern, Expr where) {
        this.pattern = pattern;
        this.where = where;
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
