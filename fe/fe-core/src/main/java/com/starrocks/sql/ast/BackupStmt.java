// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.LabelName;
import com.starrocks.analysis.TableRef;

import java.util.List;
import java.util.Map;

public class BackupStmt extends AbstractBackupStmt {
    public enum BackupType {
        INCREMENTAL, FULL
    }

    private BackupType type = BackupType.FULL;

    public BackupStmt(LabelName labelName, String repoName, List<TableRef> tblRefs, Map<String, String> properties) {
        super(labelName, repoName, tblRefs, properties);
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public BackupType getType() {
        return type;
    }

    public void setTimeoutMs(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public void setType(BackupType type) {
        this.type = type;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitBackupStmt(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }

}