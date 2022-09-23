// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;


// used to show sql's blacklist
// format is 
// Id | Forbidden SQL
// 
// for example:
// +-------+--------------------------------------+
// | Id | Forbidden SQL                        |
// +-------+--------------------------------------+
// | 1     | select count *\(\*\) from .+         |
// | 2     | select count 2342423 *\(\*\) from .+ |
// +-------+--------------------------------------+
public class ShowSqlBlackListStmt extends ShowStmt {

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Id", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Forbidden SQL", ScalarType.createVarchar(100)))
                    .build();

    public boolean isSupportNewPlanner() {
        return true;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowSqlBlackListStatement(this, context);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}
