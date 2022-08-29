// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.ShowStmt;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;

// Show Events statement
public class ShowEventsStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Db", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Name", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Definer", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Time", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Execute at", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Interval value", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Interval field", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Status", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Ends", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Status", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Originator", ScalarType.createVarchar(30)))
                    .addColumn(new Column("character_set_client", ScalarType.createVarchar(30)))
                    .addColumn(new Column("collation_connection", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Database Collation", ScalarType.createVarchar(30)))
                    .build();

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}
