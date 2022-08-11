// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/AlterSystemStmt.java

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

import com.google.common.base.Preconditions;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;

public class AlterSystemStmt extends DdlStmt {
    private final AlterClause alterClause;

    public AlterSystemStmt(AlterClause alterClause) {
        this.alterClause = alterClause;
    }

    public AlterClause getAlterClause() {
        return alterClause;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {

        if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.OPERATOR)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    "NODE");
        }

        Preconditions.checkState( (alterClause instanceof DropBackendClause)
                || (alterClause instanceof AddComputeNodeClause)
                || (alterClause instanceof AddBackendClause)
                || (alterClause instanceof DropBackendClause)
                || (alterClause instanceof ModifyBackendAddressClause)
                || (alterClause instanceof DecommissionBackendClause)
                || (alterClause instanceof AddObserverClause)
                || (alterClause instanceof DropObserverClause)
                || (alterClause instanceof AddFollowerClause)
                || (alterClause instanceof DropFollowerClause)
                || (alterClause instanceof ModifyFrontendAddressClause)
                || (alterClause instanceof AlterLoadErrorUrlClause));

        alterClause.analyze(analyzer);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER SYSTEM ").append(alterClause.toSql());
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterSystemStmt(this, context);
    }

    public static boolean isSupportNewPlanner(StatementBase statement) {
        return statement instanceof AlterSystemStmt &&
                isNewAlterSystemClause(((AlterSystemStmt) statement).getAlterClause());
    }

    private static boolean isNewAlterSystemClause(AlterClause clause) {
        return clause instanceof DropComputeNodeClause
                || clause instanceof AddComputeNodeClause
                || clause instanceof AddBackendClause
                || clause instanceof DropBackendClause
                || clause instanceof ModifyBackendAddressClause
                || clause instanceof ModifyFrontendAddressClause
                || clause instanceof ModifyBrokerClause;

    }
}
