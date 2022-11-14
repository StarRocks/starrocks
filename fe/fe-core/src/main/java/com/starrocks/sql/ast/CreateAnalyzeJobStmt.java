// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.collect.Lists;
import com.starrocks.analysis.TableName;
import com.starrocks.statistic.StatsConstants;

import java.util.List;
import java.util.Map;

public class CreateAnalyzeJobStmt extends DdlStmt {
    private long dbId;
    private long tableId;
    private final TableName tbl;
    private final List<String> columnNames;
    private final boolean isSample;
    private Map<String, String> properties;

    public CreateAnalyzeJobStmt(boolean isSample, Map<String, String> properties) {
        this(null, Lists.newArrayList(), isSample, properties);
    }

    public CreateAnalyzeJobStmt(String db, boolean isSample, Map<String, String> properties) {
        this(new TableName(db, null), Lists.newArrayList(), isSample, properties);
    }

    public CreateAnalyzeJobStmt(TableName tbl, List<String> columnNames, boolean isSample,
                                Map<String, String> properties) {
        this.tbl = tbl;
        this.dbId = StatsConstants.DEFAULT_ALL_ID;
        this.tableId = StatsConstants.DEFAULT_ALL_ID;
        this.columnNames = columnNames;
        this.isSample = isSample;
        this.properties = properties;
    }

    public void setDbId(long dbId) {
        this.dbId = dbId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public TableName getTableName() {
        return tbl;
    }

    public List<String> getColumnNames() {
        return columnNames;
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
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateAnalyzeJobStatement(this, context);
    }
}
