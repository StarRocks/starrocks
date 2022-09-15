// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/RestoreStmt.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.starrocks.sql.ast.AstVisitor;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.UserException;
import com.starrocks.common.util.PrintableMap;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class RestoreStmt extends AbstractBackupStmt {
    private static final String PROP_ALLOW_LOAD = "allow_load";
    private static final String PROP_REPLICATION_NUM = "replication_num";
    private static final String PROP_BACKUP_TIMESTAMP = "backup_timestamp";
    private static final String PROP_META_VERSION = "meta_version";
    private static final String PROP_STARROCKS_META_VERSION = "starrocks_meta_version";

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
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        // check if alias is duplicated
        Set<String> aliasSet = Sets.newHashSet();
        for (TableRef tblRef : tblRefs) {
            aliasSet.add(tblRef.getName().getTbl());
        }

        for (TableRef tblRef : tblRefs) {
            if (tblRef.hasExplicitAlias() && !aliasSet.add(tblRef.getExplicitAlias())) {
                throw new AnalysisException("Duplicated alias name: " + tblRef.getExplicitAlias());
            }
        }
    }

    @Override
    public void analyzeProperties() throws AnalysisException {
        super.analyzeProperties();

        Map<String, String> copiedProperties = Maps.newHashMap(properties);
        // allow load
        if (copiedProperties.containsKey(PROP_ALLOW_LOAD)) {
            if (copiedProperties.get(PROP_ALLOW_LOAD).equalsIgnoreCase("true")) {
                allowLoad = true;
            } else if (copiedProperties.get(PROP_ALLOW_LOAD).equalsIgnoreCase("false")) {
                allowLoad = false;
            } else {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_COMMON_ERROR,
                        "Invalid allow load value: "
                                + copiedProperties.get(PROP_ALLOW_LOAD));
            }
            copiedProperties.remove(PROP_ALLOW_LOAD);
        }

        // replication num
        if (copiedProperties.containsKey(PROP_REPLICATION_NUM)) {
            try {
                replicationNum = Integer.parseInt(copiedProperties.get(PROP_REPLICATION_NUM));
            } catch (NumberFormatException e) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_COMMON_ERROR,
                        "Invalid replication num format: "
                                + copiedProperties.get(PROP_REPLICATION_NUM));
            }
            copiedProperties.remove(PROP_REPLICATION_NUM);
        }

        // backup timestamp
        if (copiedProperties.containsKey(PROP_BACKUP_TIMESTAMP)) {
            backupTimestamp = copiedProperties.get(PROP_BACKUP_TIMESTAMP);
            copiedProperties.remove(PROP_BACKUP_TIMESTAMP);
        } else {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_COMMON_ERROR,
                    "Missing " + PROP_BACKUP_TIMESTAMP + " property");
        }

        // meta version
        if (copiedProperties.containsKey(PROP_META_VERSION)) {
            try {
                metaVersion = Integer.parseInt(copiedProperties.get(PROP_META_VERSION));
            } catch (NumberFormatException e) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_COMMON_ERROR,
                        "Invalid meta version format: "
                                + copiedProperties.get(PROP_META_VERSION));
            }
            copiedProperties.remove(PROP_META_VERSION);
        }
        if (copiedProperties.containsKey(PROP_STARROCKS_META_VERSION)) {
            try {
                starrocksMetaVersion = Integer.parseInt(copiedProperties.get(PROP_STARROCKS_META_VERSION));
            } catch (NumberFormatException e) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_COMMON_ERROR,
                        "Invalid meta version format: "
                                + copiedProperties.get(PROP_STARROCKS_META_VERSION));
            }
            copiedProperties.remove(PROP_STARROCKS_META_VERSION);
        }

        if (!copiedProperties.isEmpty()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_COMMON_ERROR,
                    "Unknown restore job properties: " + copiedProperties.keySet());
        }
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("RESTORE SNAPSHOT ").append(labelName.toSql());
        sb.append("\n").append("FROM ").append(repoName).append("\nON\n(");

        sb.append(Joiner.on(",\n").join(tblRefs));

        sb.append("\n)\nPROPERTIES\n(");
        sb.append(new PrintableMap<String, String>(properties, " = ", true, true));
        sb.append("\n)");
        return sb.toString();
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
