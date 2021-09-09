// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.analysis;

import com.google.common.collect.Lists;
import com.starrocks.statistic.AnalyzeJob;
import com.starrocks.statistic.Constants;

import java.util.List;
import java.util.Map;

public class CreateAnalyzeJobStmt extends DdlStmt {
    private long dbId;
    private long tableId;
    private TableName tbl;
    private List<String> columnNames;
    private boolean isSample;
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
        this.dbId = AnalyzeJob.DEFAULT_ALL_ID;
        this.tableId = AnalyzeJob.DEFAULT_ALL_ID;
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

    public AnalyzeJob toAnalyzeJob() {
        AnalyzeJob job = new AnalyzeJob();
        job.setDbId(dbId);
        job.setTableId(tableId);
        job.setColumns(columnNames);
        job.setType(isSample ? Constants.AnalyzeType.SAMPLE : Constants.AnalyzeType.FULL);
        job.setScheduleType(Constants.ScheduleType.SCHEDULE);
        job.setProperties(properties);
        job.setStatus(Constants.ScheduleStatus.PENDING);

        return job;
    }
}
