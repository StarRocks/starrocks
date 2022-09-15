// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/ShowGrantsStmt.java

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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;

/*
 *  SHOW ALL GRANTS;
 *      show all grants.
 *
 *  SHOW GRANTS:
 *      show grants of current user
 *
 *  SHOW GRANTS FOR user@'xxx';
 *      show grants for specified user identity
 */
//
// SHOW GRANTS;
// SHOW GRANTS FOR user@'xxx'
public class ShowGrantsStmt extends ShowStmt {

    private static final ShowResultSetMetaData META_DATA;

    static {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(new Column("UserIdentity", ScalarType.createVarchar(100)));
        builder.addColumn(new Column("Grants", ScalarType.createVarchar(400)));
        META_DATA = builder.build();
    }

    private boolean isAll;
    private UserIdentity userIdent;

    public ShowGrantsStmt(UserIdentity userIdent, boolean isAll) {
        this.userIdent = userIdent;
        this.isAll = isAll;
    }

    public UserIdentity getUserIdent() {
        return userIdent;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (userIdent != null) {
            if (isAll) {
                throw new AnalysisException("Can not specified keyword ALL when specified user");
            }
            userIdent.analyze();
            if (!GlobalStateMgr.getCurrentState().getAuth().doesUserExist(userIdent)) {
                throw new AnalysisException("user " + userIdent + " not exist!");
            }
        } else {
            if (!isAll) {
                // self
                userIdent = ConnectContext.get().getCurrentUserIdentity();
            }
        }
        Preconditions.checkState(isAll || userIdent != null);
        UserIdentity self = ConnectContext.get().getCurrentUserIdentity();

        // if show all grants, or show other user's grants, need global GRANT priv.
        if (isAll || !self.equals(userIdent)) {
            if (!GlobalStateMgr.getCurrentState().getAuth()
                    .checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
        }
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

}
