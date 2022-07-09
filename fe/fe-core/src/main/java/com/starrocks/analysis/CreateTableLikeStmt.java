// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/CreateTableLikeStmt.java

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

import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeNameFormat;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CreateTableLikeStmt extends DdlStmt {
    private static final Logger LOG = LogManager.getLogger(CreateTableLikeStmt.class);

    private final boolean ifNotExists;
    private final TableName tableName;
    private final TableName existedTableName;

    public CreateTableLikeStmt(boolean ifNotExists, TableName tableName, TableName existedTableName) {
        this.ifNotExists = ifNotExists;
        this.tableName = tableName;
        this.existedTableName = existedTableName;
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    public String getDbName() {
        return tableName.getDb();
    }

    public String getTableName() {
        return tableName.getTbl();
    }

    public String getExistedDbName() {
        return existedTableName.getDb();
    }

    public String getExistedTableName() {
        return existedTableName.getTbl();
    }

    public TableName getDbTbl() {
        return tableName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        existedTableName.analyze(analyzer);
        ConnectContext ctx = ConnectContext.get();
        if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(ctx, existedTableName.getDb(),
                existedTableName.getTbl(), PrivPredicate.SELECT)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "SELECT");
        }

        tableName.analyze(analyzer);
        FeNameFormat.checkTableName(getTableName());
        if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(ctx, tableName.getDb(),
                tableName.getTbl(), PrivPredicate.CREATE)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "CREATE");
        }
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateTableLikeStatement(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }

    @Override
    public String toSql() {
        return String.format("CREATE TABLE %s LIKE %s", tableName.toSql(), existedTableName.toSql());
    }

    @Override
    public String toString() {
        return toSql();
    }
}
