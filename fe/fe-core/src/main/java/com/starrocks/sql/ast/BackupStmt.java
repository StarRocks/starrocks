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
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Map;

public class BackupStmt extends AbstractBackupStmt {
    public enum BackupType {
        INCREMENTAL, FULL
    }

    private BackupType type = BackupType.FULL;

    public BackupStmt(LabelName labelName, String repoName, List<TableRef> tblRefs, Map<String, String> properties) {
        super(labelName, repoName, tblRefs, properties, NodePosition.ZERO);
    }

    public BackupStmt(LabelName labelName, String repoName, List<TableRef> tblRefs,
                      Map<String, String> properties, NodePosition pos) {
        super(labelName, repoName, tblRefs, properties, pos);
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
        return visitor.visitBackupStatement(this, context);
    }

}