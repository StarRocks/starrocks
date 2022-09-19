// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;

public class ShowSnapshotStmt extends ShowStmt {
    public static final ImmutableList<String> SNAPSHOT_ALL = new ImmutableList.Builder<String>()
            .add("Snapshot").add("Timestamp").add("Status")
            .build();
    public static final ImmutableList<String> SNAPSHOT_DETAIL = new ImmutableList.Builder<String>()
            .add("Snapshot").add("Timestamp").add("Database").add("Details").add("Status")
            .build();

    private final String repoName;
    private final Expr where;
    private String snapshotName;
    private String timestamp;

    public ShowSnapshotStmt(String repoName, Expr where) {
        this.repoName = repoName;
        this.where = where;
    }

    public String getRepoName() {
        return repoName;
    }

    public String getSnapshotName() {
        return snapshotName;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public Expr getWhere() {
        return where;
    }

    public void setSnapshotName(String snapshotName) {
        this.snapshotName = snapshotName;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        if (!Strings.isNullOrEmpty(snapshotName) && !Strings.isNullOrEmpty(timestamp)) {
            for (String title : SNAPSHOT_DETAIL) {
                builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
            }
        } else {
            for (String title : SNAPSHOT_ALL) {
                builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
            }
        }
        return builder.build();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowSnapshotStmt(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}

