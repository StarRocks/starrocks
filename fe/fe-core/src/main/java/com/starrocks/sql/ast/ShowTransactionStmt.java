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

import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.NodePosition;

import static com.starrocks.common.util.Util.normalizeName;

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
        this.dbName = normalizeName(dbName);
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
        this.dbName = normalizeName(dbName);
    }

    public void setTxnId(long txnId) {
        this.txnId = txnId;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitShowTransactionStatement(this, context);
    }
}
