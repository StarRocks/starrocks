// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;

//SHOW WHITELIST;
// this statement is unmaintained since 2.0.
@Deprecated
public class ShowWhiteListStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("user_name", ScalarType.createVarchar(20)))
                    .addColumn(new Column("white_list", ScalarType.createVarchar(1000)))
                    .build();

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowWhiteListStatement(this, context);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}
