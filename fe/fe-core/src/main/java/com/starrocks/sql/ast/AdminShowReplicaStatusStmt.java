// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryPredicate.Operator;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.analysis.TableRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Replica.ReplicaStatus;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSetMetaData;

import java.util.List;

public class AdminShowReplicaStatusStmt extends ShowStmt {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("TabletId").add("ReplicaId").add("BackendId").add("Version").add("LastFailedVersion")
            .add("LastSuccessVersion").add("CommittedVersion").add("SchemaHash").add("VersionNum")
            .add("IsBad").add("IsSetBadForce").add("State").add("Status")
            .build();

    private final TableRef tblRef;
    private final Expr where;
    private List<String> partitions = Lists.newArrayList();

    private Operator op;
    private ReplicaStatus statusFilter;

    public AdminShowReplicaStatusStmt(TableRef tblRef, Expr where) {
        this.tblRef = tblRef;
        this.where = where;
    }

    public TableRef getTblRef() {
        return tblRef;
    }

    public String getDbName() {
        return tblRef.getName().getDb();
    }

    public void setDbName(String dbName) {
        this.tblRef.getName().setDb(dbName);
    }

    public String getTblName() {
        return tblRef.getName().getTbl();
    }

    public List<String> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<String> partitions) {
        this.partitions = partitions;
    }

    public Operator getOp() {
        return op;
    }

    public void setOp(Operator op) {
        this.op = op;
    }

    public ReplicaStatus getStatusFilter() {
        return statusFilter;
    }

    public void setStatusFilter(ReplicaStatus statusFilter) {
        this.statusFilter = statusFilter;
    }

    public Expr getWhere() {
        return where;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        if (ConnectContext.get().getSessionVariable().getForwardToLeader()) {
            return RedirectStatus.FORWARD_NO_SYNC;
        } else {
            return RedirectStatus.NO_FORWARD;
        }
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAdminShowReplicaStatusStatement(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
