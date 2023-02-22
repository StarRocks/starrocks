// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.proc.TransProcDir;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

// syntax:
//      SHOW TRANSACTION  WHERE id=123
public class ShowTransactionStmt extends ShowStmt {

    private String dbName;
    private Expr whereClause;
    private long txnId;

    public ShowTransactionStmt(String dbName, Expr whereClause) {
        this(dbName, whereClause, NodePosition.ZERO);
    }

    public ShowTransactionStmt(String dbName, Expr whereClause, NodePosition pos) {
        super(pos);
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
        return visitor.visitShowTransactionStatement(this, context);
    }
}
