// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.LabelName;
import com.starrocks.analysis.TableRef;
import com.starrocks.common.FeConstants;

import java.util.List;
import java.util.Map;

public class RestoreStmt extends AbstractBackupStmt {
    private boolean allowLoad = false;
    private int replicationNum = FeConstants.default_replication_num;
    private String backupTimestamp = null;
    private int metaVersion = -1;
    private int starrocksMetaVersion = -1;

    public RestoreStmt(LabelName labelName, String repoName, List<TableRef> tblRefs, Map<String, String> properties) {
        super(labelName, repoName, tblRefs, properties);
    }

    public boolean allowLoad() {
        return allowLoad;
    }

    public int getReplicationNum() {
        return replicationNum;
    }

    public String getBackupTimestamp() {
        return backupTimestamp;
    }

    public int getMetaVersion() {
        return metaVersion;
    }

    public int getStarRocksMetaVersion() {
        return starrocksMetaVersion;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRestoreStmt(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }

    public void setTimeoutMs(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public void setAllowLoad(boolean allowLoad) {
        this.allowLoad = allowLoad;
    }

    public void setReplicationNum(int replicationNum) {
        this.replicationNum = replicationNum;
    }

    public void setBackupTimestamp(String backupTimestamp) {
        this.backupTimestamp = backupTimestamp;
    }

    public void setMetaVersion(int metaVersion) {
        this.metaVersion = metaVersion;
    }

    public void setStarrocksMetaVersion(int starrocksMetaVersion) {
        this.starrocksMetaVersion = starrocksMetaVersion;
    }
}
