// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.ast;

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.TableName;

import java.util.List;
import java.util.Map;

public class AnalyzeStmt extends StatementBase {
    private final TableName tbl;
    private List<String> columnNames;
    private final boolean isSample;
    private final boolean isAsync;
    private Map<String, String> properties;
    private final AnalyzeTypeDesc analyzeTypeDesc;

    public AnalyzeStmt(TableName tbl, List<String> columns, Map<String, String> properties,
                       boolean isSample, boolean isAsync,
                       AnalyzeTypeDesc analyzeTypeDesc) {
        this.tbl = tbl;
        this.columnNames = columns;
        this.isSample = isSample;
        this.isAsync = isAsync;
        this.properties = properties;
        this.analyzeTypeDesc = analyzeTypeDesc;
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

    public boolean isAsync() {
        return isAsync;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public AnalyzeTypeDesc getAnalyzeTypeDesc() {
        return analyzeTypeDesc;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAnalyzeStatement(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
