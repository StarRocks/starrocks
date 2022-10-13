// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;

public class ShowPluginsStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Name", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Type", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Description", ScalarType.createVarchar(200)))
                    .addColumn(new Column("Version", ScalarType.createVarchar(20)))
                    .addColumn(new Column("JavaVersion", ScalarType.createVarchar(20)))
                    .addColumn(new Column("ClassName", ScalarType.createVarchar(64)))
                    .addColumn(new Column("SoName", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Sources", ScalarType.createVarchar(200)))
                    .addColumn(new Column("Status", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Properties", ScalarType.createVarchar(250)))
                    .build();

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}
