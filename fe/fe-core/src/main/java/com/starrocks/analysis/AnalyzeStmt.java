// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.sql.ast.AstVisitor;

import java.util.List;
import java.util.Map;

public class AnalyzeStmt extends StatementBase {
    private final TableName tbl;
    private List<String> columnNames;
    private final boolean isSample;
    private Map<String, String> properties;

    public AnalyzeStmt(TableName tbl, List<String> columns, Map<String, String> properties, boolean isSample) {
        this.tbl = tbl;
        this.columnNames = columns;
        this.isSample = isSample;
        this.properties = properties;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    public TableName getTableName() {
        return tbl;
    }

    public boolean isSample() {
        return isSample;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAnalyzeStatement(this, context);
    }
}
