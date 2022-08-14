// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.TableName;

import java.util.List;

public class DropHistogramStmt extends StatementBase {
    private final TableName tbl;
    private final List<String> columnNames;

    public DropHistogramStmt(TableName tbl, List<String> columnNames) {
        this.tbl = tbl;
        this.columnNames = columnNames;
    }

    public TableName getTableName() {
        return tbl;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropHistogramStatement(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }
}
