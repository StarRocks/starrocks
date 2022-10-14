// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;

// SHOW OPEN TABLES
public class ShowOpenTableStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Database", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Table", ScalarType.createVarchar(10)))
                    .addColumn(new Column("In_use", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Name_locked", ScalarType.createVarchar(64)))
                    .build();

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowOpenTableStatement(this, context);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}
