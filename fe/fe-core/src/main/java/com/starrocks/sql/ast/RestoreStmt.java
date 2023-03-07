// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.sql.ast;

import com.starrocks.analysis.LabelName;
import com.starrocks.analysis.TableRef;
import com.starrocks.server.RunMode;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Map;

public class RestoreStmt extends AbstractBackupStmt {
    private boolean allowLoad = false;
    private int replicationNum = RunMode.defaultReplicationNum();
    private String backupTimestamp = null;
    private int metaVersion = -1;
    private int starrocksMetaVersion = -1;

    public RestoreStmt(LabelName labelName, String repoName, List<TableRef> tblRefs,
                       Map<String, String> properties) {
        this(labelName, repoName, tblRefs, properties, NodePosition.ZERO);
    }

    public RestoreStmt(LabelName labelName, String repoName, List<TableRef> tblRefs,
                       Map<String, String> properties, NodePosition pos) {
        super(labelName, repoName, tblRefs, properties, pos);
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
        return visitor.visitRestoreStatement(this, context);
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
