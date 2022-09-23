// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/AdminSetReplicaStatusStmt.java

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

import com.starrocks.catalog.Replica.ReplicaStatus;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;

import java.util.Map;

/*
 *  admin set replicas status properties ("key" = "val", ..);
 *  Required:
 *      "tablet_id" = "10010",
 *      "backend_id" = "10001"
 *      "status" = "bad"/"ok"
 */
public class AdminSetReplicaStatusStmt extends DdlStmt {

    public static final String TABLET_ID = "tablet_id";
    public static final String BACKEND_ID = "backend_id";
    public static final String STATUS = "status";

    private Map<String, String> properties;
    private long tabletId = -1;
    private long backendId = -1;
    private ReplicaStatus status;

    public AdminSetReplicaStatusStmt(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);

        // check auth
        if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        checkProperties();
    }

    private void checkProperties() throws AnalysisException {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            String val = entry.getValue();

            if (key.equalsIgnoreCase(TABLET_ID)) {
                try {
                    tabletId = Long.valueOf(val);
                } catch (NumberFormatException e) {
                    throw new AnalysisException("Invalid tablet id format: " + val);
                }
            } else if (key.equalsIgnoreCase(BACKEND_ID)) {
                try {
                    backendId = Long.valueOf(val);
                } catch (NumberFormatException e) {
                    throw new AnalysisException("Invalid backend id format: " + val);
                }
            } else if (key.equalsIgnoreCase(STATUS)) {
                status = ReplicaStatus.valueOf(val.toUpperCase());
                if (status != ReplicaStatus.BAD && status != ReplicaStatus.OK) {
                    throw new AnalysisException("Do not support setting replica status as " + val);
                }
            } else {
                throw new AnalysisException("Unknown property: " + key);
            }
        }

        if (tabletId == -1 || backendId == -1 || status == null) {
            throw new AnalysisException("Should add following properties: TABLET_ID, BACKEND_ID and STATUS");
        }
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public long getTabletId() {
        return tabletId;
    }

    public long getBackendId() {
        return backendId;
    }

    public ReplicaStatus getStatus() {
        return status;
    }

    public void setTabletId(long tabletId) {
        this.tabletId = tabletId;
    }

    public void setBackendId(long backendId) {
        this.backendId = backendId;
    }

    public void setStatus(ReplicaStatus status) {
        this.status = status;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAdminSetReplicaStatusStatement(this, context);
    }
}
