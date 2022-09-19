// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;

public class ShowRestoreStmt extends ShowStmt {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("JobId").add("Label").add("Timestamp").add("DbName").add("State")
            .add("AllowLoad").add("ReplicationNum")
            .add("RestoreObjs").add("CreateTime").add("MetaPreparedTime").add("SnapshotFinishedTime")
            .add("DownloadFinishedTime").add("FinishedTime").add("UnfinishedTasks").add("Progress")
            .add("TaskErrMsg").add("Status").add("Timeout")
            .build();

    private String dbName;
    private Expr where;
    private String label;

    public ShowRestoreStmt(String dbName, Expr where) {
        this.dbName = dbName;
        this.where = where;
    }

    public String getDbName() {
        return dbName;
    }

    public String getLabel() {
        return label;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowRestoreStmt(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}

