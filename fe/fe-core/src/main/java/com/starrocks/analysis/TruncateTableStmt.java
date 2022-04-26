// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/TruncateTableStmt.java

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

import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;

// TRUNCATE TABLE tbl [PARTITION(p1, p2, ...)]
public class TruncateTableStmt extends DdlStmt {

    private TableRef tblRef;

    public TruncateTableStmt(TableRef tblRef) {
        this.tblRef = tblRef;
    }

    public TableRef getTblRef() {
        return tblRef;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        tblRef.getName().analyze(analyzer);

        if (tblRef.hasExplicitAlias()) {
            throw new AnalysisException("Not support truncate table with alias");
        }

        // check access
        // it requires LOAD privilege, because we consider this operation as 'delete data', which is also a
        // 'load' operation.
        if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(ConnectContext.get(), tblRef.getName().getDb(),
                tblRef.getName().getTbl(), PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "LOAD");
        }

        // check partition if specified. do not support truncate temp partitions
        PartitionNames partitionNames = tblRef.getPartitionNames();
        if (partitionNames != null) {
            partitionNames.analyze(analyzer);
            if (partitionNames.isTemp()) {
                throw new AnalysisException("Not support truncate temp partitions");
            }
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("TRUNCATE TABLE ");
        sb.append(tblRef.getName().toSql());
        if (tblRef.getPartitionNames() != null) {
            sb.append(tblRef.getPartitionNames().toSql());
        }
        return sb.toString();
    }

}
