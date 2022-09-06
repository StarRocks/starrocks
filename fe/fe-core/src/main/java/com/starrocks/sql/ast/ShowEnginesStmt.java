// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.ShowStmt;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;

public class ShowEnginesStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Engine", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Support", ScalarType.createVarchar(8)))
                    .addColumn(new Column("Comment", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Transactions", ScalarType.createVarchar(3)))
                    .addColumn(new Column("XA", ScalarType.createVarchar(3)))
                    .addColumn(new Column("Savepoints", ScalarType.createVarchar(3)))
                    .build();

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}
