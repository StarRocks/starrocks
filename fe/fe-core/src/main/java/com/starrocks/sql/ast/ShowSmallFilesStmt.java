// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;

public class ShowSmallFilesStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Id", ScalarType.createVarchar(32)))
                    .addColumn(new Column("DbName", ScalarType.createVarchar(256)))
                    .addColumn(new Column("GlobalStateMgr", ScalarType.createVarchar(32)))
                    .addColumn(new Column("FileName", ScalarType.createVarchar(16)))
                    .addColumn(new Column("FileSize", ScalarType.createVarchar(16)))
                    .addColumn(new Column("IsContent", ScalarType.createVarchar(16)))
                    .addColumn(new Column("MD5", ScalarType.createVarchar(16)))
                    .build();

    private String dbName;

    public ShowSmallFilesStmt(String dbName) {
        this.dbName = dbName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowSmallFilesStatement(this, context);
    }
}
