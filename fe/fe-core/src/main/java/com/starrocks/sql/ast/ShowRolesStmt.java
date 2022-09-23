// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;

public class ShowRolesStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA;

    static {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        builder.addColumn(new Column("Name", ScalarType.createVarchar(100)));
        builder.addColumn(new Column("Users", ScalarType.createVarchar(100)));
        builder.addColumn(new Column("GlobalPrivs", ScalarType.createVarchar(300)));
        builder.addColumn(new Column("DatabasePrivs", ScalarType.createVarchar(300)));
        builder.addColumn(new Column("TablePrivs", ScalarType.createVarchar(300)));
        builder.addColumn(new Column("ResourcePrivs", ScalarType.createVarchar(300)));

        META_DATA = builder.build();
    }

    public ShowRolesStmt() {

    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowRolesStatement(this, context);
    }

}
