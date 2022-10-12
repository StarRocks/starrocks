// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.ShowStmt;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;

// Used to test show executor.
public class ShowAuthorStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Name", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Location", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Comment", ScalarType.createVarchar(30)))
                    .build();

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}
