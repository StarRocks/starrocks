// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;

// SHOW PROCESSLIST statement.
// Used to show connection belong to this user.
public class ShowProcesslistStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("FeHost", ScalarType.createVarchar(16)))
                    .addColumn(new Column("Id", ScalarType.createType(PrimitiveType.BIGINT)))
                    .addColumn(new Column("User", ScalarType.createVarchar(16)))
                    .addColumn(new Column("ClientHost", ScalarType.createVarchar(16)))
                    .addColumn(new Column("Db", ScalarType.createVarchar(16)))
                    .addColumn(new Column("Command", ScalarType.createVarchar(16)))
                    .addColumn(new Column("ConnectionStartTime", ScalarType.createVarchar(16)))
                    .addColumn(new Column("Time", ScalarType.createType(PrimitiveType.INT)))
                    .addColumn(new Column("State", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Info", ScalarType.createVarchar(32 * 1024)))
                    .addColumn(new Column("IsPending", ScalarType.createVarchar(16)))
                    .build();
    private final boolean isShowFull;
    private final boolean isShowAll;

    private final String feHost;

    public ShowProcesslistStmt(boolean isShowFull, boolean isShowAll, String feHost) {
        this.isShowFull = isShowFull;
        this.isShowAll = isShowAll;
        this.feHost = feHost;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowProcesslistStatement(this, context);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    public boolean isShowFull() {
        return isShowFull;
    }

    public boolean isShowAll() {
        return isShowAll;
    }

    public String getFeHost() {
        return feHost;
    }
}
