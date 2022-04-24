// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/DropClusterStmt.java

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

import com.google.common.base.Strings;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.SystemInfoService;

@Deprecated
public class DropClusterStmt extends DdlStmt {
    private boolean ifExists;
    private String name;

    public DropClusterStmt(boolean ifExists, String name) {
        this.ifExists = ifExists;
        this.name = name;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        if (Config.disable_cluster_feature) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_INVALID_OPERATION, "DROP CLUSTER");
        }

        if (Strings.isNullOrEmpty(name)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CLUSTER_NAME_NULL);
        }

        if (name.equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
            throw new AnalysisException("Can not drop " + SystemInfoService.DEFAULT_CLUSTER);
        }

        if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.OPERATOR)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CLUSTER_NO_PERMISSIONS);
        }
    }

    @Override
    public String toSql() {
        return "DROP CLUSTER " + name;
    }

    public String getClusterName() {
        return name;
    }

    public void setClusterName(String clusterName) {
        this.name = clusterName;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
