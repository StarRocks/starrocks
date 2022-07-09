// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/ShowFunctionsStmt.java

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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;

public class ShowFunctionsStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Signature", ScalarType.createVarchar(256)))
                    .addColumn(new Column("Return Type", ScalarType.createVarchar(32)))
                    .addColumn(new Column("Function Type", ScalarType.createVarchar(16)))
                    .addColumn(new Column("Intermediate Type", ScalarType.createVarchar(16)))
                    .addColumn(new Column("Properties", ScalarType.createVarchar(16)))
                    .build();

    private String dbName;

    private boolean isBuiltin;

    private boolean isVerbose;

    private String wild;

    private Expr expr;

    public ShowFunctionsStmt(String dbName, boolean isBuiltin, boolean isVerbose, String wild, Expr expr) {
        this.dbName = dbName;
        this.isBuiltin = isBuiltin;
        this.isVerbose = isVerbose;
        this.wild = wild;
        this.expr = expr;
    }

    public String getDbName() {
        return dbName;
    }

    public boolean getIsBuiltin() {
        return isBuiltin;
    }

    public boolean getIsVerbose() {
        return isVerbose;
    }

    public String getWild() {
        return wild;
    }

    public Expr getExpr() {
        return expr;
    }

    public boolean like(String str) {
        str = str.toLowerCase();
        return str.matches(wild.replace(".", "\\.").replace("?", ".").replace("%", ".*").toLowerCase());
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        if (Strings.isNullOrEmpty(dbName)) {
            dbName = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        } else {
            dbName = ClusterNamespace.getFullName(dbName);
        }

        if (!GlobalStateMgr.getCurrentState().getAuth().checkDbPriv(ConnectContext.get(), dbName, PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(
                    ErrorCode.ERR_DB_ACCESS_DENIED, ConnectContext.get().getQualifiedUser(), dbName);
        }

        if (expr != null) {
            throw new AnalysisException("Only support like 'function_pattern' syntax.");
        }
    }

    public void setDbName(String db) {
        this.dbName = db;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowFunctions(this, context);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW ");
        if (isVerbose) {
            sb.append("FULL ");
        }
        if (isBuiltin) {
            sb.append("BUILTIN ");
        }
        sb.append("FUNCTIONS FROM ");
        if (!Strings.isNullOrEmpty(dbName)) {
            sb.append("`").append(dbName).append("` ");
        }
        if (wild != null) {
            sb.append("LIKE ").append("`").append(wild).append("`");
        }
        return sb.toString();
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }

    @Override
    public String toString() {
        return toSql();
    }

}
