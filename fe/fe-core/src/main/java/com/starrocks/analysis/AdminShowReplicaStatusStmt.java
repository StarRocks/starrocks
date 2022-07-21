// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/AdminShowReplicaStatusStmt.java

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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryPredicate.Operator;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Replica.ReplicaStatus;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;

import java.util.List;

public class AdminShowReplicaStatusStmt extends ShowStmt {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("TabletId").add("ReplicaId").add("BackendId").add("Version").add("LastFailedVersion")
            .add("LastSuccessVersion").add("CommittedVersion").add("SchemaHash").add("VersionNum")
            .add("IsBad").add("IsSetBadForce").add("State").add("Status")
            .build();

    private TableRef tblRef;
    private Expr where;
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
        if (ConnectContext.get().getSessionVariable().getForwardToMaster()) {
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
