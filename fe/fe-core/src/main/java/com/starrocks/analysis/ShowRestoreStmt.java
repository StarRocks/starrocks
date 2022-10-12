// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/ShowRestoreStmt.java

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

import com.starrocks.sql.ast.AstVisitor;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;

public class ShowRestoreStmt extends ShowStmt {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("JobId").add("Label").add("Timestamp").add("DbName").add("State")
            .add("AllowLoad").add("ReplicationNum")
            .add("RestoreObjs").add("CreateTime").add("MetaPreparedTime").add("SnapshotFinishedTime")
            .add("DownloadFinishedTime").add("FinishedTime").add("UnfinishedTasks").add("Progress")
            .add("TaskErrMsg").add("Status").add("Timeout")
            .build();

    private String dbName;
    private Expr where;
    private String label;

    public ShowRestoreStmt(String dbName, Expr where) {
        this.dbName = dbName;
        this.where = where;
    }

    public String getDbName() {
        return dbName;
    }

    public String getLabel() {
        return label;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }

        // check auth
        if (!GlobalStateMgr.getCurrentState().getAuth().checkDbPriv(ConnectContext.get(), dbName, PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DB_ACCESS_DENIED,
                    ConnectContext.get().getQualifiedUser(), dbName);
        }
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
    public String toSql() {
        StringBuilder builder = new StringBuilder();
        builder.append("SHOW RESTORE");
        if (dbName != null) {
            builder.append(" FROM `").append(dbName).append("` ");
        }

        if (where != null) {
            builder.append(where.toSql());
        }
        return builder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowRestoreStmt(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}

