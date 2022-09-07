// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.ShowStmt;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;

public class ShowTriggersStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Trigger", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Event", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Table", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Statement", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Timing", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Created", ScalarType.createVarchar(80)))
                    .addColumn(new Column("sql_mode", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Definer", ScalarType.createVarchar(80)))
                    .addColumn(new Column("character_set_client", ScalarType.createVarchar(80)))
                    .addColumn(new Column("collation_connection", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Database Collation", ScalarType.createVarchar(80)))
                    .build();
    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
