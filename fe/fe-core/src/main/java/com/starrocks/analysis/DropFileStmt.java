// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/DropFileStmt.java

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
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;

import java.util.Map;
import java.util.Optional;

public class DropFileStmt extends DdlStmt {
    public static final String PROP_CATALOG = "globalStateMgr";

    private String fileName;
    private String dbName;
    private Map<String, String> properties;

    private String catalogName;

    public DropFileStmt(String fileName, String dbName, Map<String, String> properties) {
        this.fileName = fileName;
        this.dbName = dbName;
        this.properties = properties;
    }

    public String getFileName() {
        return fileName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);

        // check operation privilege
        if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        if (dbName == null) {
            dbName = analyzer.getDefaultDb();
        }

        if (Strings.isNullOrEmpty(dbName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
        }

        if (Strings.isNullOrEmpty(fileName)) {
            throw new AnalysisException("File name is not specified");
        }

        Optional<String> optional = properties.keySet().stream().filter(
                entity -> !PROP_CATALOG.equals(entity)).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid property");
        }

        catalogName = properties.get(PROP_CATALOG);
        if (Strings.isNullOrEmpty(catalogName)) {
            throw new AnalysisException("globalStateMgr name is missing");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP FILE \"").append(fileName).append("\"");
        if (dbName != null) {
            sb.append(" FROM ").append(dbName);
        }

        sb.append(" PROPERTIES(");
        PrintableMap<String, String> map = new PrintableMap<>(properties, ",", true, false);
        sb.append(map.toString());
        return sb.toString();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropFileStatement(this, context);
    }
}
