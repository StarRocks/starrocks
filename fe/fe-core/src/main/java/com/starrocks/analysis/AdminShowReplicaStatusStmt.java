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
<<<<<<< HEAD
import com.starrocks.server.GlobalStateMgr;
=======
import com.starrocks.sql.ast.AstVisitor;
>>>>>>> cf0fb08a7 ([Refactor] Remove some unused code in old parser for admin stmt (#8963))

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

<<<<<<< HEAD
    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);

        // check auth
        if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        String dbName = null;
        if (Strings.isNullOrEmpty(tblRef.getName().getDb())) {
            dbName = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        } else {
            dbName = ClusterNamespace.getFullName(getClusterName(), tblRef.getName().getDb());
        }

        tblRef.getName().setDb(dbName);

        CatalogUtils.checkOlapTableHasStarOSPartition(dbName, tblRef.getName().getTbl());

        PartitionNames partitionNames = tblRef.getPartitionNames();
        if (partitionNames != null) {
            if (partitionNames.isTemp()) {
                throw new AnalysisException("Do not support showing replica status of temporary partitions");
            }
            partitions.addAll(partitionNames.getPartitionNames());
        }

        if (!analyzeWhere()) {
            throw new AnalysisException(
                    "Where clause should looks like: status =/!= 'OK/DEAD/VERSION_ERROR/SCHEMA_ERROR/MISSING'");
        }
    }

    private boolean analyzeWhere() throws AnalysisException {
        // analyze where clause if not null
        if (where == null) {
            return true;
        }

        if (!(where instanceof BinaryPredicate)) {
            return false;
        }

        BinaryPredicate binaryPredicate = (BinaryPredicate) where;
        op = binaryPredicate.getOp();
        if (op != Operator.EQ && op != Operator.NE) {
            return false;
        }

        Expr leftChild = binaryPredicate.getChild(0);
        if (!(leftChild instanceof SlotRef)) {
            return false;
        }

        String leftKey = ((SlotRef) leftChild).getColumnName();
        if (!leftKey.equalsIgnoreCase("status")) {
            return false;
        }

        Expr rightChild = binaryPredicate.getChild(1);
        if (!(rightChild instanceof StringLiteral)) {
            return false;
        }

        try {
            statusFilter = ReplicaStatus.valueOf(((StringLiteral) rightChild).getStringValue().toUpperCase());
        } catch (Exception e) {
            return false;
        }

        if (statusFilter == null) {
            return false;
        }

        return true;
=======
    public TableRef getTblRef() {
        return tblRef;
>>>>>>> cf0fb08a7 ([Refactor] Remove some unused code in old parser for admin stmt (#8963))
    }

    public String getDbName() {
        return tblRef.getName().getDb();
    }

    public String getTblName() {
        return tblRef.getName().getTbl();
    }

    public List<String> getPartitions() {
        return partitions;
    }

    public Operator getOp() {
        return op;
    }

    public ReplicaStatus getStatusFilter() {
        return statusFilter;
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
}
