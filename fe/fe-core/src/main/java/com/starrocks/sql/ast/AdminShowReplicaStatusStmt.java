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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryPredicate.Operator;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.TableRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Replica.ReplicaStatus;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

// ADMIN SHOW REPLICA STATUS FROM example_db.example_table;
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
        this(tblRef, where, NodePosition.ZERO);
    }

    public AdminShowReplicaStatusStmt(TableRef tblRef, Expr where, NodePosition pos) {
        super(pos);
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
}
