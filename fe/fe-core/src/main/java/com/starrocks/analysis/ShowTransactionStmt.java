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
import com.starrocks.common.proc.TransProcDir;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.AstVisitor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// syntax:
//      SHOW TRANSACTION  WHERE id=123
public class ShowTransactionStmt extends ShowStmt {
    private static final Logger LOG = LogManager.getLogger(ShowTransactionStmt.class);

    private String dbName;
    private Expr whereClause;
    private long txnId;

    public ShowTransactionStmt(String dbName, Expr whereClause) {
        this.dbName = dbName;
        this.whereClause = whereClause;
    }

    public Expr getWhereClause() {
        return whereClause;
    }

    public String getDbName() {
        return dbName;
    }

    public long getTxnId() {
        return txnId;
    }
    
    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public void setTxnId(long txnId) {
        this.txnId = txnId;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW TRANSACTION ");
        if (!Strings.isNullOrEmpty(dbName)) {
            sb.append("FROM `").append(dbName).append("` ");
        }

        sb.append("WHERE ").append(whereClause.toSql());
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TransProcDir.TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowTransactionStmt(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
