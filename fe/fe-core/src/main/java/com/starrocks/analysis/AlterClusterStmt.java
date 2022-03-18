// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/AlterClusterStmt.java

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

import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;

import java.util.Map;

@Deprecated
public class AlterClusterStmt extends DdlStmt {
    private Map<String, String> properties;
    private String alterClusterName;
    private String clusterName;
    private int instanceNum;

    public AlterClusterStmt(String clusterName, Map<String, String> properties) {
        this.alterClusterName = clusterName;
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        ErrorReport.reportAnalysisException(ErrorCode.ERR_INVALID_OPERATION, "ALTER CLUSTER");
    }

    @Override
    public String toSql() {
        return "ALTER CLUSTER " + alterClusterName + " PROPERTIES(\"instance_num\"=" + "\"" + instanceNum + "\")";
    }

    public String getAlterClusterName() {
        return alterClusterName;
    }

    public void setAlterClusterName(String alterClusterName) {
        this.alterClusterName = alterClusterName;
    }

    public String getClusterName() {
        return this.clusterName;
    }
}
