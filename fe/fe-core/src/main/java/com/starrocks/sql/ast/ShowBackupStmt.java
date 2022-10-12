// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;

public class ShowBackupStmt extends ShowStmt {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("JobId").add("SnapshotName").add("DbName").add("State").add("BackupObjs").add("CreateTime")
            .add("SnapshotFinishedTime").add("UploadFinishedTime").add("FinishedTime").add("UnfinishedTasks")
            .add("Progress").add("TaskErrMsg").add("Status").add("Timeout")
            .build();

    private String dbName;

    public ShowBackupStmt(String dbName) {
        this.dbName = dbName;
    }

    public String getDbName() {
        return dbName;
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
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowBackupStmt(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
