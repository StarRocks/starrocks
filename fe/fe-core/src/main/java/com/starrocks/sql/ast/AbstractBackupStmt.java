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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.LabelName;
import com.starrocks.analysis.TableRef;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Map;

public class AbstractBackupStmt extends DdlStmt {
    protected LabelName labelName;
    protected String repoName;
    protected List<TableRef> tblRefs;
    protected Map<String, String> properties;

    protected long timeoutMs;

    public AbstractBackupStmt(LabelName labelName, String repoName, List<TableRef> tableRefs,
                              Map<String, String> properties, NodePosition pos) {
        super(pos);
        this.labelName = labelName;
        this.repoName = repoName;
        this.tblRefs = tableRefs;
        if (this.tblRefs == null) {
            this.tblRefs = Lists.newArrayList();
        }

        this.properties = properties == null ? Maps.newHashMap() : properties;
    }

    public String getDbName() {
        return labelName.getDbName();
    }

    public String getLabel() {
        return labelName.getLabelName();
    }

    public LabelName getLabelName() {
        return labelName;
    }

    public String getRepoName() {
        return repoName;
    }

    public List<TableRef> getTableRefs() {
        return tblRefs;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }
}

