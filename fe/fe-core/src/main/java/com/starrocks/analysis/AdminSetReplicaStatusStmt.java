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
