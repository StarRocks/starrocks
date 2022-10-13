// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;

/**
 * Show collation statement:
 * Acceptable syntax:
 * SHOW COLLATION
 * [LIKE 'pattern' | WHERE expr]
 */
public class ShowCollationStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Collation", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Charset", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Id", ScalarType.createType(PrimitiveType.BIGINT)))
                    .addColumn(new Column("Default", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Compiled", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Sortlen", ScalarType.createType(PrimitiveType.BIGINT)))
                    .build();

    private String pattern;
    private Expr where;

    public ShowCollationStmt() {

    }

    public ShowCollationStmt(String pattern) {
        this.pattern = pattern;
    }

    public ShowCollationStmt(String pattern, Expr where) {
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
