// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;

public class ShowProcedureStmt extends ShowStmt {

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Db", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Name", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Type", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Definer", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Modified", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Created", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Security_type", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Comment", ScalarType.createVarchar(80)))
                    .addColumn(new Column("character_set_client", ScalarType.createVarchar(80)))
                    .addColumn(new Column("collation_connection", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Database Collation", ScalarType.createVarchar(80)))
                    .build();

    private String pattern;
    private Expr where;

    public ShowProcedureStmt(String pattern) {
        this.pattern = pattern;
    }

    public ShowProcedureStmt(Expr where) {
        this.where = where;
    }

    public ShowProcedureStmt() {
    }


    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    public String getPattern() {
        return pattern;
    }
}
