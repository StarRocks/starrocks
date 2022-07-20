// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/RecoverPartitionStmt.java

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
import com.starrocks.analysis.CompoundPredicate.Operator;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.PrivBitSet;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.mysql.privilege.Privilege;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;

public class RecoverPartitionStmt extends DdlStmt {
    private TableName dbTblName;
    private String partitionName;

    public RecoverPartitionStmt(TableName dbTblName, String partitionName) {
        this.dbTblName = dbTblName;
        this.partitionName = partitionName;
    }

    public String getDbName() {
        return dbTblName.getDb();
    }

    public String getTableName() {
        return dbTblName.getTbl();
    }

    public TableName getDbTblName() {
        return dbTblName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        dbTblName.analyze(analyzer);
        if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(ConnectContext.get(), dbTblName.getDb(),
                dbTblName.getTbl(),
                PrivPredicate.of(PrivBitSet.of(Privilege.ALTER_PRIV,
                                Privilege.CREATE_PRIV,
                                Privilege.ADMIN_PRIV),
                        Operator.OR))) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "RECOVERY",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    dbTblName.getTbl());
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("RECOVER PARTITION ").append(partitionName).append(" FROM ");
        if (!Strings.isNullOrEmpty(getDbName())) {
            sb.append(getDbName()).append(".");
        }
        sb.append(getTableName());
        return sb.toString();
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRecoverPartitionStmt(this, context);
    }
}
