// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.LimitElement;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;

// Show Warning stmt
public class ShowWarningStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Level", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Code", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Message", ScalarType.createVarchar(20)))
                    .build();


    private LimitElement limitElement;

    public ShowWarningStmt() {
    }

    public ShowWarningStmt(LimitElement limitElement) {
        this.limitElement = limitElement;
    }

    public long getLimitNum() {
        if (limitElement != null && limitElement.hasLimit()) {
            return limitElement.getLimit();
        }
        return -1L;
    }


    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowWarningStatement(this, context);
    }

    public boolean isSupportNewPlanner() {
        return true;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}