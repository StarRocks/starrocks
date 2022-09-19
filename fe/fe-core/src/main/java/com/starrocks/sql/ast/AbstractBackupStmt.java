// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.LabelName;
import com.starrocks.analysis.TableRef;

import java.util.List;
import java.util.Map;

public class AbstractBackupStmt extends DdlStmt {
    protected LabelName labelName;
    protected String repoName;
    protected List<TableRef> tblRefs;
    protected Map<String, String> properties;

    protected long timeoutMs;

    public AbstractBackupStmt(LabelName labelName, String repoName, List<TableRef> tableRefs,
                              Map<String, String> properties) {
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

