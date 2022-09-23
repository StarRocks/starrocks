// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.catalog.Replica.ReplicaStatus;

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

    private final Map<String, String> properties;
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
